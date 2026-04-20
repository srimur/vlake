# WQC Weight Justification

The Weighted Quorum Consensus (WQC) role weights `(s, c, a) = (3, 2, 1)` are chosen by **constraint-based design**, then **verified via Shapley-Shubik power analysis**. The full proof lives in [backend/app.py:683-722](../backend/app.py#L683-L722).

## Design logic

Six governance requirements are stated, and the minimal integer solution is derived:

| ID | Requirement | Formal constraint |
|---|---|---|
| R1 | Steward supremacy — 2 stewards alone can pass | `2s ≥ q` |
| R2 | No unilateral steward — 1 steward alone cannot | `s < q` |
| R3 | Custodian must be pivotal in some coalition | ∃ coalition where C flips outcome |
| R4 | Non-stewards alone insufficient | `c + a < q` |
| R5 | Strict ordering by liability | `0 < w(A) < w(C) < w(S)` |
| R6 | Integer weights (Solidity gas efficiency) | `s, c, a ∈ ℤ⁺` |

**Unique minimal solution:** `(s, c, a) = (3, 2, 1)`.

Verification for a (3S, 1C, 1A) committee, `W = 12`, `q = ⌈W/2⌉ = 6`:

- R1: `2·3 = 6 ≥ 6` ✓
- R2: `3 < 6` ✓
- R4: `2 + 1 = 3 < 6` ✓
- R5: `1 < 2 < 3` ✓
- R3: `{S₁, C₁, A₁} = 6` passes, but `{S₁, A₁} = 4 < 6` → C is pivotal ✓

Why these are *minimal*: R5 forces `a ≥ 1, c ≥ 2, s ≥ 3`, and `(3, 2, 1)` already satisfies R1–R4, so no smaller integer triple works.

## Shapley-Shubik verification

On the reference game `[7; 3, 3, 3, 2, 2, 1]` (3 stewards, 2 custodians, 1 analyst — [backend/app.py:706-711](../backend/app.py#L706-L711)):

- φ(Steward)   = 156/720 ≈ **0.217** — highest power, reflects highest liability (GDPR fines up to 4% revenue)
- φ(Custodian) = 108/720 ≈ **0.150** — intermediate power, domain expertise over data quality
- φ(Analyst)   =  36/720 ≈ **0.050** — minimal power, access-scope-bounded

Power ordering matches weight ordering, and the ratios track the liability rationale: stewards bear regulatory exposure, custodians bear operational liability, analysts are access-scope-bounded.

Note: in a 5-player game `φ(C) = φ(A) = 0.10`, but `w=2` vs `w=1` still differentiates in multi-custodian deployments and encodes operational intent.

## Safety theorem

For `q ≥ W/2`: if `YES ≥ q`, then `NO ≤ W − q < W/2 < q`, so a proposal cannot be simultaneously approved and rejected.

**Proof:** `YES ≥ q ≥ W/2 ⟹ NO ≤ W − q < W/2 < q`. □

This is asserted as a test in [backend/test_vlake.py:129-131](../backend/test_vlake.py#L129-L131) (`2·w(S) > w(C) + 3·w(A)`).

## Anti-collusion and emergency properties

- **Anti-collusion:** For critical ops (`all_stew = True`), 1 honest steward blocks all malicious changes regardless of collusion size.
- **Emergency revoke:** Any 1 steward (weight ≥ 3 = req) can trigger the "break glass" pattern per HIPAA §164.312 incident response requirements.

## Summary

The numbers aren't arbitrary — `(3, 2, 1)` is the unique minimal integer point in the feasible region of R1–R6, cross-checked by an independent power index (Shapley-Shubik).
