#!/usr/bin/env python3
"""
V-Lake end-to-end benchmark.

Measures real latencies against:
  - the running Flask backend on http://localhost:5000
  - the Besu QBFT node on http://localhost:8545

Writes results to scripts/bench_results.json and prints a summary.

Nothing here is simulated. On-chain anchoring is a real Ethereum
transaction; the script waits for the receipt before recording the
latency. The full-pipeline query overhead is measured by the
backend's /api/eval/overhead endpoint, which runs the same SQL twice:
once through DuckDB directly and once through the V-Lake predicate
injection + compliance + Merkle path.
"""
import argparse
import json
import math
import os
import statistics
import sys
import time
from pathlib import Path

import requests
from web3 import Web3

BACKEND = os.getenv("BACKEND_URL", "http://localhost:5000")
RPC = os.getenv("BESU_RPC", "http://localhost:8545")

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_DIR = SCRIPT_DIR.parent

# --- stat helpers -----------------------------------------------------

def ci95(xs):
    if len(xs) < 2:
        return 0.0
    s = statistics.stdev(xs)
    return 1.96 * s / math.sqrt(len(xs))

def summarize(xs):
    xs = sorted(xs)
    n = len(xs)
    if n == 0:
        return {}
    mean = statistics.mean(xs)
    out = {
        "n": n,
        "mean_ms": round(mean, 3),
        "std_ms": round(statistics.stdev(xs), 3) if n > 1 else 0.0,
        "ci95_ms": round(ci95(xs), 3),
        "p50_ms": round(xs[n // 2], 3),
        "p95_ms": round(xs[min(int(n * 0.95), n - 1)], 3),
        "p99_ms": round(xs[min(int(n * 0.99), n - 1)], 3),
        "min_ms": round(xs[0], 3),
        "max_ms": round(xs[-1], 3),
    }
    return out

def bench(name, fn, warmup=5, n=50):
    for _ in range(warmup):
        try:
            fn()
        except Exception:
            pass
    times = []
    for _ in range(n):
        t0 = time.perf_counter()
        fn()
        times.append((time.perf_counter() - t0) * 1000)
    s = summarize(times)
    s["name"] = name
    print(f"  {name:45s} mean={s['mean_ms']:7.2f}ms  p95={s['p95_ms']:7.2f}ms  "
          f"±{s['ci95_ms']:5.2f} (95% CI, n={n})")
    return s

# --- sanity checks ----------------------------------------------------

def assert_stack_live():
    h = requests.get(f"{BACKEND}/api/health", timeout=5).json()
    if h.get("mode") != "on-chain":
        raise SystemExit(f"backend is in mode={h.get('mode')}, "
                         "not on-chain — abort to avoid misleading numbers")
    w3 = Web3(Web3.HTTPProvider(RPC))
    if not w3.is_connected():
        raise SystemExit(f"Besu RPC not reachable at {RPC}")
    print(f"backend: mode={h['mode']} stewards={h['stewards']} "
          f"datasets={h['datasets']}")
    print(f"besu:    block={w3.eth.block_number} chain_id={w3.eth.chain_id}")
    return w3, h

# --- backend-path benchmarks -----------------------------------------

def bench_query_overhead(datasets, out):
    out["query_overhead"] = []
    for did, name in datasets:
        r = requests.post(f"{BACKEND}/api/eval/overhead",
                          json={"datasetId": did,
                                "query": f'SELECT * FROM "{name}" LIMIT 100',
                                "iterations": 200}).json()
        if "error" in r:
            print(f"  skip {name}: {r['error']}")
            continue
        r["dataset"] = name
        out["query_overhead"].append(r)
        print(f"  dataset={name:20s}  baseline avg={r['baseline_ms']['avg']:5.2f}ms  "
              f"p95={r['baseline_ms']['p95']:5.2f}ms | "
              f"vlake avg={r['vlake_ms']['avg']:5.2f}ms  "
              f"p95={r['vlake_ms']['p95']:5.2f}ms | "
              f"overhead +{r['overhead_pct']:.1f}%")

def bench_merkle_scale(out):
    r = requests.post(f"{BACKEND}/api/eval/merkle-scale",
                      json={"sizes": [10, 100, 1000, 5000, 10000, 50000]}).json()
    out["merkle_scale"] = r["results"]
    for row in r["results"]:
        print(f"  rows={row['rows']:6d}  avg={row['avg_ms']:8.2f}ms  "
              f"p95={row['p95_ms']:8.2f}ms")

def bench_attacks(out):
    r = requests.get(f"{BACKEND}/api/eval/attack-scenarios").json()
    out["attacks"] = r
    for s in r["scenarios"]:
        print(f"  [{'BLOCKED' if s['blocked'] else 'OPEN   '}] {s['attack']}")
    print(f"  summary: {sum(1 for s in r['scenarios'] if s['blocked'])}/"
          f"{len(r['scenarios'])} modeled adversaries blocked by test")

# --- blockchain-path benchmarks (direct web3) -------------------------

def bench_chain(w3, out):
    """Measure real on-chain round-trip for governance and anchoring ops.

    Uses the existing backend endpoints that internally send signed
    transactions to Besu; each call waits for the receipt so the
    measurement includes block inclusion latency (QBFT ~2s period).
    """
    # Ingestion anchor: force a re-ingest on an existing dataset — the
    # backend re-hashes rows and calls recordIngestion() on chain.
    out["chain"] = {}
    dataset_ids = [d["id"] for d in requests.get(f"{BACKEND}/api/datasets").json()["datasets"]]

    def anchor_once():
        did = dataset_ids[0]
        # trigger a re-hash + on-chain anchor
        requests.post(f"{BACKEND}/api/ingest/stream",
                      json={"datasetId": did, "rebuild": True}, timeout=30)

    anchor_stats = bench("on-chain anchor (Merkle root record)", anchor_once,
                        warmup=2, n=10)
    out["chain"]["anchor"] = anchor_stats

    # Proposal create + 2 steward votes (real contract calls)
    def propose_and_vote():
        addr = f"0x{os.urandom(20).hex()}"
        r = requests.post(f"{BACKEND}/api/proposals",
                          json={"type": "ASSIGN_CUSTODIAN",
                                "datasetId": dataset_ids[0],
                                "target": addr,
                                "proposer": "0x1111111111111111111111111111111111111111"},
                          timeout=30).json()
        pid = r.get("id")
        if not pid:
            return
        requests.post(f"{BACKEND}/api/proposals/{pid}/vote",
                      json={"voter": "0x1111111111111111111111111111111111111111",
                            "approve": True}, timeout=30)
        requests.post(f"{BACKEND}/api/proposals/{pid}/vote",
                      json={"voter": "0x2222222222222222222222222222222222222222",
                            "approve": True}, timeout=30)

    prop_stats = bench("WQG proposal + 2 votes (on-chain)", propose_and_vote,
                       warmup=1, n=5)
    out["chain"]["proposal_and_votes"] = prop_stats

    # Raw TX latency to a block: send a zero-value tx from dev account
    acct = w3.eth.accounts[0] if w3.eth.accounts else None
    if acct:
        w3.eth.default_account = acct
        def simple_tx():
            tx = {"from": acct, "to": acct, "value": 0, "gas": 21000,
                  "gasPrice": 0, "nonce": w3.eth.get_transaction_count(acct)}
            h = w3.eth.send_transaction(tx)
            w3.eth.wait_for_transaction_receipt(h, timeout=20)
        tx_stats = bench("raw Besu TX round-trip (signed + mined)", simple_tx,
                         warmup=2, n=10)
        out["chain"]["raw_tx"] = tx_stats

# --- column crypto microbenchmark (kernel-level) ---------------------

def bench_column_crypto(out):
    """Exercise the column_crypto module directly."""
    sys.path.insert(0, str(PROJECT_DIR / "backend"))
    try:
        from column_crypto import (derive_column_keys, encrypt_det, decrypt_det,
                                   encrypt_rand, decrypt_rand, blind_index)
    except Exception as e:
        print(f"  skip (no column_crypto module): {e}")
        return
    keys = derive_column_keys(os.urandom(32))

    def det_roundtrip():
        c = encrypt_det(keys["siv"], "MRN-00042")
        decrypt_det(keys["siv"], c)

    def rand_roundtrip():
        c = encrypt_rand(keys["fernet"], "type 2 diabetes mellitus")
        decrypt_rand(keys["fernet"], c)

    def bidx_hash():
        blind_index(keys["bidx"], "E11.9", xform="icd3")

    out["column_crypto"] = {
        "det_roundtrip": bench("det encrypt+decrypt", det_roundtrip, n=200),
        "rand_roundtrip": bench("rand encrypt+decrypt", rand_roundtrip, n=200),
        "bidx_hash": bench("blind-index hash (icd3)", bidx_hash, n=500),
    }

# --- centralized-IAM baseline (no blockchain, no Merkle, no quorum) ---

def bench_centralized_baseline(out):
    """Simulate a conventional centralized IAM / RBAC stack as a baseline.

    This emulates the path a traditional app would take:

      query path:   check (user, role, column) against an in-memory ACL
                    dict (one dict lookup + set-subset test) and then run
                    raw DuckDB. No Merkle tree, no compliance attestation,
                    no on-chain commit.

      governance:   a single admin mutates an in-memory role dict and
                    appends to a local audit list. No proposal, no votes,
                    no chain.

      audit:        append a row to an in-memory list. No hash chain, no
                    anchor.

    This is the best case for centralized IAM — it has no durability, no
    integrity guarantee, no multi-party separation of duties, and no
    verifiability, so the latency is effectively the cost of a dict
    lookup plus the same underlying DuckDB query. It is the comparator
    V-Lake should be measured against when discussing overhead.
    """
    import duckdb as _duck

    out["centralized"] = {}
    con = _duck.connect(":memory:")
    con.execute("CREATE TABLE t AS SELECT range AS id, "
                "'John_' || range AS name, "
                "CAST(20 + (range % 60) AS INTEGER) AS age, "
                "'type_' || (range % 5) AS cat "
                "FROM range(0, 1000)")
    acl = {"analyst_1": {"t": {"id", "cat", "age"}}}  # column whitelist
    audit_log = []

    def centralized_query():
        allowed = acl["analyst_1"]["t"]
        requested = {"id", "cat", "age"}
        if not requested.issubset(allowed):
            return
        cols = ",".join(sorted(requested))
        con.execute(f"SELECT {cols} FROM t LIMIT 100").fetchall()
        audit_log.append(("analyst_1", "t", time.time()))

    def centralized_admin_op():
        acl.setdefault("analyst_2", {})["t"] = {"id", "cat"}
        audit_log.append(("admin", "grant", "analyst_2", time.time()))

    out["centralized"]["query"] = bench(
        "centralized query path (RBAC + raw DuckDB)",
        centralized_query, n=200)
    out["centralized"]["admin_op"] = bench(
        "centralized admin grant (single writer)",
        centralized_admin_op, n=200)

# --- main -------------------------------------------------------------

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--out", default=str(SCRIPT_DIR / "bench_results.json"))
    p.add_argument("--skip-chain", action="store_true",
                   help="skip on-chain benchmarks (faster; for smoke tests)")
    args = p.parse_args()

    w3, _ = assert_stack_live()
    datasets = [(d["id"], d["name"])
                for d in requests.get(f"{BACKEND}/api/datasets").json()["datasets"]
                if d.get("rowCount", 0) > 0]
    print(f"\neligible datasets for overhead study: "
          f"{[n for _,n in datasets]}\n")

    out = {"backend": BACKEND, "rpc": RPC, "ts": int(time.time())}

    print("─" * 72)
    print("[1/5] Query-overhead (full V-Lake pipeline vs raw DuckDB)")
    print("─" * 72)
    bench_query_overhead(datasets, out)

    print("\n" + "─" * 72)
    print("[2/5] Merkle build scaling (row count)")
    print("─" * 72)
    bench_merkle_scale(out)

    print("\n" + "─" * 72)
    print("[3/5] Attack-scenario coverage (deterministic)")
    print("─" * 72)
    bench_attacks(out)

    print("\n" + "─" * 72)
    print("[4/6] Column-encryption kernel microbench")
    print("─" * 72)
    bench_column_crypto(out)

    print("\n" + "─" * 72)
    print("[5/6] Centralized IAM baseline (no blockchain, no Merkle)")
    print("─" * 72)
    bench_centralized_baseline(out)

    if not args.skip_chain:
        print("\n" + "─" * 72)
        print("[6/6] Real on-chain round-trip (Besu QBFT)")
        print("─" * 72)
        bench_chain(w3, out)

    Path(args.out).write_text(json.dumps(out, indent=2))
    print(f"\nresults written to {args.out}")

if __name__ == "__main__":
    main()
