#!/usr/bin/env python3
"""
V-Lake Comparative Evaluation
═════════════════════════════
Generates all metrics needed for the paper's evaluation section.

Measures:
  1. Predicate injection overhead vs raw DuckDB
  2. Merkle tree scalability (10 to 100K rows)
  3. WQC consensus latency
  4. Attack scenario validation
  5. On-chain gas costs (when Besu is available)

Usage: python scripts/evaluate.py [--api http://localhost:5000/api]
"""
import requests, json, time, sys, os

API = sys.argv[1] if len(sys.argv) > 1 else os.getenv("VLAKE_API", "http://localhost:5000/api")

def get(path): return requests.get(f"{API}{path}").json()
def post(path, data): return requests.post(f"{API}{path}", json=data).json()

def banner(t):
    print(f"\n{'═'*70}\n  {t}\n{'═'*70}")

def main():
    banner("V-LAKE EVALUATION SUITE")
    print(f"  API: {API}")

    # Check health
    h = get("/health")
    print(f"  Mode: {h.get('mode')}")
    print(f"  Blockchain: {h.get('mode') == 'on-chain'}")

    # Ensure demo data exists
    if h.get("datasets", 0) == 0:
        print("  Running demo setup...")
        post("/demo/reset", {})
        for i in range(20):
            r = post("/demo/next", {})
            if r.get("error") and not r.get("details"):
                print(f"  Demo step {i} failed: {r.get('error')}")
                break

    # ─── 1. PREDICATE INJECTION OVERHEAD ───
    banner("1. PREDICATE INJECTION OVERHEAD")
    r = post("/eval/overhead", {"datasetId": "1", "query": 'SELECT * FROM "trial_enrollment"', "iterations": 100})
    if "error" not in r:
        print(f"  Baseline (raw DuckDB):    {r['baseline_ms']['avg']:.3f} ms avg, {r['baseline_ms']['p95']:.3f} ms p95")
        print(f"  V-Lake (inject+comply+mk): {r['vlake_ms']['avg']:.3f} ms avg, {r['vlake_ms']['p95']:.3f} ms p95")
        print(f"  Overhead: {r['overhead_pct']:.1f}%")
    else:
        print(f"  Error: {r['error']}")

    # ─── 2. MERKLE SCALABILITY ───
    banner("2. MERKLE TREE SCALABILITY")
    r = post("/eval/merkle-scale", {"sizes": [10, 100, 500, 1000, 5000, 10000, 50000]})
    if "results" in r:
        print(f"  {'Rows':>8s}  {'Avg (ms)':>10s}  {'P95 (ms)':>10s}")
        print(f"  {'─'*8}  {'─'*10}  {'─'*10}")
        for m in r["results"]:
            print(f"  {m['rows']:>8d}  {m['avg_ms']:>10.3f}  {m['p95_ms']:>10.3f}")

    # ─── 3. ATTACK SCENARIOS ───
    banner("3. ATTACK SCENARIO VALIDATION")
    r = get("/eval/attack-scenarios")
    if "scenarios" in r:
        for s in r["scenarios"]:
            status = "✓ BLOCKED" if s["blocked"] else "✕ NOT BLOCKED"
            print(f"  {status}  {s['attack']}")
            print(f"           {s['detail'][:80]}")
        print(f"\n  Result: {'ALL ATTACKS BLOCKED' if r['all_blocked'] else 'SOME ATTACKS NOT BLOCKED'} ({r['count']} scenarios)")

    # ─── 4. BLOCKCHAIN STATUS ───
    banner("4. BLOCKCHAIN STATUS")
    r = get("/blockchain/status")
    print(f"  Mode: {r.get('mode')}")
    print(f"  Connected: {r.get('connected')}")
    if r.get("connected"):
        print(f"  Block number: {r.get('block_number')}")
        print(f"  Chain ID: {r.get('chain_id')}")
    if r.get("warning"):
        print(f"  ⚠ {r['warning']}")

    # ─── 5. SYSTEM SUMMARY ───
    banner("5. SYSTEM SUMMARY")
    ds = get("/datasets")
    props = get("/proposals")
    logs = get("/audit/queries")
    chain = get("/ssi/verify-chain")
    forest = get("/merkle/forest")
    cache = get("/cache/stats")
    wqc_cfg = get("/consensus/config")

    print(f"  Datasets: {len(ds.get('datasets',[]))}")
    print(f"  Proposals: {len(props.get('proposals',[]))}")
    print(f"  Query logs: {len(logs.get('logs',[]))}")
    print(f"  Consent chain: valid={chain.get('valid')}, length={chain.get('length')}")
    print(f"  Forest root: {forest.get('forestRoot','?')[:32]}...")
    print(f"  Grant cache: hits={cache.get('stats',{}).get('hits',0)}, misses={cache.get('stats',{}).get('misses',0)}")
    print(f"  Weight model: {wqc_cfg.get('justification',{}).get('method','?')}")
    print(f"  Reference game: {wqc_cfg.get('justification',{}).get('reference_game','?')}")

    banner("EVALUATION COMPLETE")

if __name__ == "__main__":
    main()
