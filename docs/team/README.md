# Team documentation

Per-contributor write-ups of what each person designed, implemented, and
owns in V-Lake. Each file explains the *why* and *how* behind the code,
with exact `file:line` citations so reviewers can jump straight to the
implementation.

| Contributor | Areas of ownership | Doc |
|---|---|---|
| **Srinath** | Smart contract (`VLakeGovernance.sol`), Hyperledger Besu runtime, deploy pipeline, backend ↔ contract bridge, Weighted Quorum Consensus math, Merkle integrity (C1), envelope encryption for document uploads | [srinath.md](srinath.md) |
| **Rishav**  | Frontend portal (single-page React UI), 20-step demo orchestration, audit trails (governance / Merkle / document-access logs), Self-Sovereign Identity flow (C3) — registration, linkage, delegation, revocation, consent chain verification | [rishav.md](rishav.md) |
| **Manas**   | DuckDB in-process engine, federated data-source registry (C4) spanning 10 source types, per-connector ingest logic (LOCAL_FILE, S3/MinIO, PostgreSQL, MySQL, Kafka, MongoDB…), source seeding, predicate-injection / column-restriction enforcement, connector-side credential handling | [manas.md](manas.md) |

The four contributions labelled in the code (C1 Merkle, C2 WQC, C3 SSI,
C4 federated sources) are split across the team as follows:

- **C1 (Merkle integrity)** — Srinath owns the tree construction, proof
  generation, domain separation, and on-chain anchoring. Rishav exposes
  the Merkle history and forest root in the Audit view.
- **C2 (Weighted Quorum Consensus)** — Srinath owns the weights, the
  quorum matrix, the `compute_wqc` algorithm, and on-chain vote
  recording. Rishav owns the governance UI (proposals list, vote
  buttons, quorum progress meters).
- **C3 (Self-Sovereign Identity)** — Rishav owns the full flow end to
  end: subject registration, consent chain append, delegation,
  revocation, chain verification, and the SSI view in the portal.
- **C4 (Federated source registry)** — Manas owns every connector
  branch, the source-type registry, the seeding logic, and the
  DuckDB integration. Srinath's Merkle layer hashes whatever Manas
  loads in.
