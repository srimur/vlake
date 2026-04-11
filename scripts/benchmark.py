#!/usr/bin/env python3
"""V-Lake Performance Benchmarks — generates numbers for the paper."""
import time, json, sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "backend"))
from app import (build_merkle_tree, hash_leaf, get_merkle_proof, verify_merkle_proof,
                 inject_predicates, compute_wqc, check_compliance, S, COMPLIANCE_RULES,
                 sha256, _dataset_policies)

def bench(name, fn, n=100):
    times = []
    for _ in range(n):
        t0 = time.perf_counter()
        fn()
        times.append((time.perf_counter() - t0) * 1000)
    avg = sum(times) / len(times)
    p95 = sorted(times)[int(n * 0.95)]
    print(f"  {name:40s} avg={avg:8.3f}ms  p95={p95:8.3f}ms  (n={n})")
    return avg

print("═" * 70)
print("  V-LAKE PERFORMANCE BENCHMARKS")
print("═" * 70)

# C1: Merkle Tree
print("\nC1: Merkle Tree Build Time")
for sz in [10, 100, 1000, 5000]:
    rows = [(f"patient_{i}", f"data_{i}", i, f"dept_{i%5}") for i in range(sz)]
    bench(f"  build_merkle_tree({sz} rows)", lambda: build_merkle_tree(rows), n=20)

print("\nC1: Merkle Proof Generation + Verification")
rows = [(f"row_{i}",) for i in range(1000)]
root, tree, lc = build_merkle_tree(rows)
bench("  get_merkle_proof(1000 rows)", lambda: get_merkle_proof(tree, 500))
proof = get_merkle_proof(tree, 500)
leaf = tree[0][500]
bench("  verify_merkle_proof", lambda: verify_merkle_proof(leaf, proof, root))

# Predicate Injection
print("\nPredicate Injection Overhead")
grant_none = {"allowedColumns": "", "rowFilter": "", "expiresAt": 0, "active": True}
grant_cols = {"allowedColumns": "age,gender,blood_type,site,arm", "rowFilter": "", "expiresAt": 0, "active": True}
grant_row = {"allowedColumns": "", "rowFilter": "patient_id='P0001'", "expiresAt": 0, "active": True}
grant_both = {"allowedColumns": "age,gender,blood_type", "rowFilter": "site='City Hospital'", "expiresAt": 0, "active": True}
q = 'SELECT * FROM "trial_enrollment"'
bench("  no restriction", lambda: inject_predicates(q, grant_none, "trial_enrollment"))
bench("  column restriction (5 cols)", lambda: inject_predicates(q, grant_cols, "trial_enrollment"))
bench("  row filter only", lambda: inject_predicates(q, grant_row, "trial_enrollment"))
bench("  column + row filter", lambda: inject_predicates(q, grant_both, "trial_enrollment"))

# C2: WQC
print("\nC2: WQC Compute Time")
S["custodians"]["1"] = ["0x4444444444444444444444444444444444444444"]
p = {"id": "bench", "type": "ASSIGN_CUSTODIAN", "datasetId": "1"}
votes = {"0x1111111111111111111111111111111111111111": True, "0x2222222222222222222222222222222222222222": True}
bench("  compute_wqc (standard)", lambda: compute_wqc(p, votes))
p2 = {"id": "bench", "type": "ATTACH_POLICY", "datasetId": "1"}
bench("  compute_wqc (critical)", lambda: compute_wqc(p2, votes))

# Compliance
print("\nCompliance Check Time")
_dataset_policies["bench"] = ["HIPAA", "GDPR"]
schema = [{"name": c, "type": "VARCHAR"} for c in ["patient_id", "name", "age", "email", "phone", "diagnosis"]]
bench("  check_compliance (HIPAA+GDPR)", lambda: check_compliance('SELECT age, diagnosis FROM "tbl"', "bench", "0xtest", grant_cols, schema))

print("\n" + "═" * 70)
print("  Benchmark complete")
print("═" * 70)
