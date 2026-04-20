# Srinath — Blockchain, Cryptography & Encrypted Document Layer

**Scope:** everything on the trust boundary. The smart contract that arbitrates
governance, the Besu runtime it lives on, the Merkle commitments that make
ingested data tamper-evident, the weighted-quorum math that decides whether a
proposal is executed, the wiring that keeps the Flask cache coherent with
on-chain state, and the envelope-encryption pipeline that turns PDF uploads into
auth-gated ciphertext blobs in MinIO.

---

## 1. The governance smart contract

`VLakeGovernance.sol` is the authoritative source of truth for every
privilege V-Lake grants. The Flask backend is an in-memory cache of this
contract; when the chain is reachable, every state-changing operation goes
through `_write_to_chain()` and the cache re-syncs from chain state at boot.

**File:** [contracts/VLakeGovernance.sol](../../contracts/VLakeGovernance.sol)
(835 lines, Solidity 0.8.19, compiled with optimizer + `viaIR`).

### Domain model (on-chain structs)

| Struct | Where | Purpose |
|---|---|---|
| `Dataset`                | [VLakeGovernance.sol:72](../../contracts/VLakeGovernance.sol#L72) | id, name, Merkle root, row count, source type, confidentiality flag |
| `AccessGrant`            | [VLakeGovernance.sol:90](../../contracts/VLakeGovernance.sol#L90) | grantee, level, allowed columns, row filter, expiry |
| `Proposal`               | [VLakeGovernance.sol:102](../../contracts/VLakeGovernance.sol#L102) | type, proposer, target, WQC state, status, quorum certificate |
| `CompliancePolicy`       | [VLakeGovernance.sol:127](../../contracts/VLakeGovernance.sol#L127) | HIPAA/GDPR/DPDPA rule set attached to datasets |
| `ComplianceAttestation`  | [VLakeGovernance.sol:137](../../contracts/VLakeGovernance.sol#L137) | hash-chained record of every policy check |
| `ConsentRecord`          | [VLakeGovernance.sol:150](../../contracts/VLakeGovernance.sol#L150) | C3 consent chain node — action, subject, prev_hash, hash |
| `SubjectDelegation`      | [VLakeGovernance.sol:162](../../contracts/VLakeGovernance.sol#L162) | time-bounded delegation created by a subject |
| `QueryLog`               | [VLakeGovernance.sol:172](../../contracts/VLakeGovernance.sol#L172) | every query recorded on-chain, linked to its attestation |

### State-changing entry points I own

| Function | Line | Callable by | What it does |
|---|---|---|---|
| `createDataset`            | [L322](../../contracts/VLakeGovernance.sol#L322) | Steward | Register a new dataset and emit `DatasetCreated` |
| `recordIngestion`          | [L354](../../contracts/VLakeGovernance.sol#L354) | Steward/Custodian | Anchor the Merkle root of a new batch |
| `updateForestRoot`         | [L376](../../contracts/VLakeGovernance.sol#L376) | Steward | Roll up per-dataset roots into a global forest root |
| `createProposal`           | [L417](../../contracts/VLakeGovernance.sol#L417) | Steward/Custodian | Start a WQC vote |
| `vote`                     | [L460](../../contracts/VLakeGovernance.sol#L460) | Steward/Custodian | Cast a weighted vote, auto-execute on quorum |
| `finalizeExpired`          | [L540](../../contracts/VLakeGovernance.sol#L540) | Anyone | Mark a proposal whose deadline passed as `EXPIRED` |
| `recordAttestation`        | [L650](../../contracts/VLakeGovernance.sol#L650) | Custodian/Analyst | Anchor a compliance attestation hash |
| `logQuery`                 | [L776](../../contracts/VLakeGovernance.sol#L776) | Anyone with a grant | On-chain query log |

Everything that mutates state emits a typed event so off-chain indexers can
rebuild state without trusting the RPC node — see the full event list starting
at [VLakeGovernance.sol:259](../../contracts/VLakeGovernance.sol#L259).

---

## 2. Hyperledger Besu runtime

**What runs:** `hyperledger/besu:24.1.0` inside the Docker bridge network,
started by [docker-compose.yml](../../docker-compose.yml) under the
`besu-node1` service.

**Why that version:** Besu 26.x dropped `--network=dev` and the `miner-*`
CLI flags, so newer images can't auto-produce blocks in a single-node dev
setup. The whole "deploy → smoke-test → frontend green" flow collapses
without mining, so the compose file pins 24.1.0 and the README documents
*do not bump this without porting to multi-node QBFT*.

Other runtime decisions I owned:

- **Ephemeral data path** (`--data-path=/tmp/besu-data`). Besu stores its
  chain under a short-lived directory instead of a named Docker volume
  because the dev genesis is regenerated on every container start, so a
  persisted chain would fight with a fresh state root on the next boot.
  Consequence: tearing the container down loses the chain and you must
  redeploy the contract. That's intentional, and the deploy script is
  idempotent (`scripts/deploy_contract.py --no-smoke-test` re-deploys
  cleanly in under 10 s).
- **user: root** in compose. The older image tried to write to a path
  the non-root user couldn't touch; running as root is fine because the
  container has no persistent state anyway.
- **`--min-gas-price=0`** so deployment and every subsequent tx cost zero
  ETH — the pre-funded dev account has 90,000 ETH but we use none of it.

## 3. Contract deployment pipeline

**Script:** [scripts/deploy_contract.py](../../scripts/deploy_contract.py)

Walkthrough of what it does from scratch:

1. **Compile** ([compile_contract, L86](../../scripts/deploy_contract.py#L86))
   — uses `py-solc-x` to install solc 0.8.19 if missing, loads
   `VLakeGovernance.sol`, and compiles with `{"optimizer": {"enabled": True,
   "runs": 200}, "viaIR": True}`. The `viaIR: true` is critical: without it,
   compiling the 835-line contract hits `Stack too deep` during codegen.
   The compiled ABI is written to `backend/contract_abi.json`, which is
   bind-mounted into the backend container at runtime.
2. **Deploy** ([deploy_contract, L155](../../scripts/deploy_contract.py#L155))
   — derives the three steward addresses from the public Besu/Truffle dev
   private keys (see banner at [L45-54](../../scripts/deploy_contract.py#L45)),
   signs a `CREATE` transaction with Steward-1's key, and waits for the
   receipt. Contracts deploy at block ~38 on a fresh chain.
3. **Verify** ([verify_deployment, L235](../../scripts/deploy_contract.py#L235))
   — reads `stewardCount`, `isSteward`, `roles`, `getWeights`, `datasetCount`,
   `proposalCount`, and the initial consent chain head, failing loudly if
   anything doesn't match what the constructor should have produced.
4. **Merge into `.env`**
   ([L219-241](../../scripts/deploy_contract.py#L219)) — reads any existing
   `.env`, preserves user-set keys like `VLAKE_MASTER_KEY`, and writes
   `CONTRACT_ADDRESS`, `BESU_RPC`, `CHAIN_ID`, `DEPLOYED_AT_BLOCK`,
   `DEPLOYED_AT`. This is a real fix — the original script used `open(env,
   "w")` which nuked every custom key on redeploy.

### Steward addresses are derived, not hardcoded

The original version of this script hardcoded `0x1111111111...`,
`0x2222222222...`, `0x3333333333...` as the steward addresses and tried to
sign transactions with unrelated dev keys — so the first `eth_sendRawTransaction`
died with *"from field must match key"*. The fix
([L45-54](../../scripts/deploy_contract.py#L45)) derives each address from
its private key via `Account.from_key(pk).address` so the deployer identity
is always consistent with the signing key. The dev genesis pre-funds the
derived address `0xf17f52151...`, so the deployer has gas to spare.

---

## 4. Backend ↔ contract bridge

**File:** [backend/app.py](../../backend/app.py), "Blockchain Integration
Layer" section at [L53–161](../../backend/app.py#L53).

The contract is the source of truth; the Flask cache is a read-through
performance layer. All five bridge helpers:

| Helper | Line | Job |
|---|---|---|
| `_init_blockchain`          | [L68](../../backend/app.py#L68)  | Load the ABI from the bind-mounted `contract_abi.json`, connect via web3, flag `_blockchain_available`, call `_sync_from_chain`. |
| `_sync_from_chain`          | [L101](../../backend/app.py#L101) | Read `getStewards()` and `datasetCount()` on boot into the in-memory cache `S`. |
| `_write_to_chain`           | [L119](../../backend/app.py#L119) | Generic "fire an on-chain tx" helper. Always writes to the local append-only `governance_audit_log.jsonl` first (so forensics work even if the chain is down), then tries the transact, then logs the tx hash on success. |
| `_anchor_merkle_root`       | [L142](../../backend/app.py#L142) | Called by `_post_ingest` after every new batch to call `recordIngestion(did, root, rc, lc, depth)`. |
| `_record_vote_on_chain`     | [L147](../../backend/app.py#L147) | Hooked into `vote_proposal` so every off-chain vote is mirrored to `contract.vote()`. |
| `_record_attestation_on_chain` | [L151](../../backend/app.py#L151) | Anchors compliance attestation hashes |
| `_record_consent_on_chain`  | [L156](../../backend/app.py#L156) | Appends SSI consent records to the on-chain chain |

### The "audit-log-first" pattern

`_write_to_chain` deliberately writes to a local JSONL audit file **before**
it tries to talk to Besu. That means the same call:

1. Produces a durable record on disk even if `web3.is_connected()` is false.
2. Tries the on-chain write.
3. If the on-chain write fails, appends a second line with the error so
   an operator can see the attempted tx and the failure mode.

Without this, a flaky chain would let governance decisions silently dissolve;
with it, the worst case is "chain state diverges from local state, but every
divergence is forensically visible."

### Startup sequencing

[backend/app.py `__main__` at L2010+](../../backend/app.py#L2010):

```
_init_encryption()    # master KEK + DEK store
_seed_all_sources()   # MinIO / Kafka / Mongo / Postgres demo data
_check_services()     # health-check every dependency
_init_blockchain()    # load ABI, connect to Besu, sync S
app.run(...)
```

The order matters: encryption comes first because it creates
`backend/data/.master.key` before anything else can touch the data dir;
blockchain comes last because it needs `_check_services` to have confirmed
Besu is reachable.

---

## 5. Weighted Quorum Consensus (C2) — weights & math

**File:** [backend/app.py, `compute_wqc` at L736–771](../../backend/app.py#L736)

### Role weights (Shapley-derived)

```
ROLE_WEIGHTS = {
    "DATA_STEWARD":   3,
    "DATA_CUSTODIAN": 2,
    "ANALYST":        1,
    "SUBJECT":        0,
    "NONE":           0,
}
```

Defined at [backend/app.py:724](../../backend/app.py#L724). These are not
arbitrary — they come from a Shapley-value calculation over the four role
types: a steward's marginal contribution to a quorum is three times a
subject's because a steward can form a quorum single-handedly with two
peers, while two custodians can't push a proposal without a steward, and
analysts/subjects are non-voting by default.

### Quorum configuration matrix

```
QUORUM_CONFIG = {
    "ASSIGN_CUSTODIAN":    standard,  thr=0.50, all_stew=False, cust_maj=False
    "ONBOARD_ANALYST":     standard,  thr=0.50, all_stew=True,  cust_maj=True
    "ACCESS_GRANT":        standard,  thr=0.50, all_stew=True,  cust_maj=True
    "REVOKE_CUSTODIAN":    standard,  thr=0.50, all_stew=False, cust_maj=False
    "REVOKE_ANALYST":      emergency, thr=0.00, all_stew=False, cust_maj=False
    "ATTACH_POLICY":       critical,  thr=0.67, all_stew=True,  cust_maj=False
    "TOGGLE_CONFIDENTIAL": critical,  thr=0.67, all_stew=True,  cust_maj=True
}
```

At [backend/app.py:726](../../backend/app.py#L726). Three quorum classes:

- **Standard (50%)** — routine grants and revocations.
- **Critical (67%)** — anything that reshapes compliance policy or opens
  confidential data.
- **Emergency (any single steward)** — the break-glass path. Only
  `REVOKE_ANALYST` uses it, so the ethics board can strip access without
  waiting for peers.

### The compute_wqc algorithm

Input: proposal `p`, vote map `{voter: bool}`.

1. Sum **total weight** `tw = |stewards|*3 + |custodians|*2` for the
   dataset the proposal targets.
2. Walk the votes, counting **yesWeight / noWeight**, **stewardYes/No**,
   **custodianYes/No**.
3. Compute the **required weight**: emergency → 3 (single steward),
   otherwise `ceil(tw * thr)`.
4. **weightMet** = `yesWeight >= rw` (for emergency: `stewardYes >= 1`).
5. **stewardsMet** = gate passes if `all_stew` is off OR every steward
   voted yes.
6. **custodiansMet** = gate passes if `cust_maj` is off, or there are no
   custodians, or strict majority of custodians voted yes.
7. **Approved** iff all three gates pass.
8. **Rejected** iff the reachable-yes-weight (assuming remaining voters
   vote yes) can no longer clear quorum, OR a required-steward voted no,
   OR a custodian majority is impossible.

When a proposal reaches a terminal state (approved or rejected), a
**quorum certificate** is minted — `sha256({pid, outcome, yw, nw, tw, rw, t})` —
and written back to the proposal for on-chain anchoring. See
[backend/app.py:765-766](../../backend/app.py#L765).

Early rejection matters because governance decisions block real work.
The algorithm finalises a proposal the instant it becomes unwinnable
instead of waiting for the 1-hour default deadline, so a steward who
votes no on an `ATTACH_POLICY` immediately frees the other stewards.

---

## 6. Merkle integrity (C1)

**File:** [backend/app.py, C1 section at L524–595](../../backend/app.py#L524)

The Merkle layer is what makes ingested data tamper-evident: anyone can
later prove a given row was (or wasn't) part of what was ingested, without
trusting the backend that served it.

### Domain separation

Every hash in V-Lake is tagged with its domain:

- **Leaves:** `sha256("vlake.leaf:" + idx + ":" + n_cols + ":" + canonical_row)` —
  [hash_leaf L541](../../backend/app.py#L541).
- **Inner nodes:** `sha256("vlake.node:" + level + ":" + left + ":" + right)` —
  [hash_node L545](../../backend/app.py#L545).
- **Forest roots:** `sha256("vlake.forest:" + idx + ":" + dataset_root)` —
  [compute_forest_root L589](../../backend/app.py#L589).
- **Empty tree sentinel:** `sha256("vlake.empty_tree")`.

Why domain separation: without it, an attacker could craft a row whose
canonical serialization happens to equal a valid inner-node hash, and claim
that node is a leaf. With the domain prefix, leaf and node hash spaces are
provably disjoint.

### Canonical row serialization

[_canonical_row L531](../../backend/app.py#L531) — stringifies with type
tags so `NULL`, `true`/`false`, and floats in different notations all
produce a single canonical encoding. Pipes in values are escaped
(`\|`) so they can't collide with the `|` delimiter, and backslashes
are double-escaped (`\\`). Without this, `("a", "b|c")` and
`("a|b", "c")` would hash identically.

### Tree construction

[build_merkle_tree L547](../../backend/app.py#L547) — odd nodes are
promoted unchanged to the next level (the standard "hash once" variant,
not the duplicate-last-element variant). This keeps proofs short for the
common unbalanced case and avoids the "double-spend via duplicate leaf"
footgun that the duplicate variant has.

### Proof API

- `get_merkle_proof(tree, idx)` — [L560](../../backend/app.py#L560).
  Returns a list of `{dir, hash, level}` steps climbing the tree.
- `verify_merkle_proof(leaf, proof, root)` — [L570](../../backend/app.py#L570).
  Pure function; anyone with the leaf, the proof, and the published root
  can verify without touching the database.
- `compute_forest_root()` — [L589](../../backend/app.py#L589). Rolls every
  dataset's Merkle root into a single "forest" root. A second-layer commitment
  that lets a single 32-byte value prove the state of every dataset at a
  given block.

### Ingestion flow

[_post_ingest L869](../../backend/app.py#L869):

1. Recompute Merkle tree from the DuckDB virtual table.
2. Update `S["datasets"][did]` with the new root and row count.
3. Append `{root, leafCount, timestamp}` to `S["merkle_roots"][did]`.
4. Refresh the in-memory `_merkle_cache` entry.
5. Call `_anchor_merkle_root` (which in turn calls `_write_to_chain("recordIngestion", ...)`).
6. Append to `backend/data/merkle_audit_log.jsonl` — the tamper-evident
   fallback for when the chain is down.
7. Call `_try_anchor_on_chain` (a second, defensive anchor path — see
   [L836](../../backend/app.py#L836)).

The two-path anchoring is deliberate: `_anchor_merkle_root` uses the
persistent web3 instance from `_init_blockchain`, while `_try_anchor_on_chain`
builds a fresh web3 per call. If the persistent instance has drifted (e.g.
Besu restarted), the second path still succeeds.

---

## 7. Envelope encryption for document uploads

**File:** [backend/app.py, encryption layer at L163–289](../../backend/app.py#L163)

Uploaded PDFs (and PNG/DOCX/TXT) never touch storage in plaintext. The
whole pipeline is:

```
upload bytes ──► extract searchable text (pypdf)
            └─► encrypt with per-dataset DEK
                 └─► PUT to MinIO bucket vlake-encrypted
```

### Key hierarchy (envelope encryption)

- **Master key (KEK)** — one per installation. Comes from the
  `VLAKE_MASTER_KEY` env var; if unset, a dev key is auto-generated at
  `backend/data/.master.key` with a loud warning. Production deployments
  must source this from a KMS / Vault / HSM and never touch disk.
- **Data encryption key (DEK)** — one per dataset. Generated lazily the
  first time a dataset needs to encrypt something, wrapped by the KEK,
  and persisted to `backend/data/deks.json` as `{did: base64(wrapped_dek)}`.
- **Ciphertext** — Fernet token (`gAAAAA...`, AES-128-CBC + HMAC-SHA256),
  stored in MinIO at `vlake-encrypted/datasets/<did>/<doc_id>.bin`.

Why envelope: rotating the KEK requires only re-wrapping the DEKs (a
tiny JSON file), not re-encrypting every blob. Per-dataset DEKs also mean
that compromising one DEK doesn't leak data from unrelated datasets.

### Functions

| Function | Line | Purpose |
|---|---|---|
| `_init_encryption`   | [L181](../../backend/app.py#L181) | Load or generate the KEK, load the DEK store |
| `_persist_dek_store` | [L217](../../backend/app.py#L217) | Atomic write of `deks.json` via `os.replace` |
| `_get_dataset_fernet`| [L226](../../backend/app.py#L226) | Return a Fernet bound to the dataset's DEK, minting one on first use |
| `_encrypt_bytes`     | [L241](../../backend/app.py#L241) | Encrypt raw bytes with the dataset DEK |
| `_decrypt_bytes`     | [L244](../../backend/app.py#L244) | Decrypt ciphertext with the dataset DEK |
| `_store_encrypted`   | [L247](../../backend/app.py#L247) | PUT to MinIO; fall back to local `backend/uploads/enc_<did>_<doc>.bin` if MinIO is unreachable |
| `_load_encrypted`    | [L274](../../backend/app.py#L274) | GET from MinIO or local disk — mirror of `_store_encrypted` |

### Upload endpoint

`POST /api/ingest/document` — [ingest_document_ep at L1117](../../backend/app.py#L1117):

1. Auth: caller must be a steward or an assigned custodian for the
   dataset, else `403`.
2. Read the upload into memory; hash it (SHA-256 for file integrity,
   not auth).
3. Write to a temp file for `pypdf` to read, extract up to 50k chars
   of searchable text, delete the temp file immediately.
4. Encrypt the raw bytes with `_encrypt_bytes(did, raw)`.
5. Store the ciphertext via `_store_encrypted`.
6. Record the document as a row in the dataset's DuckDB doc table —
   the `extracted_text` column is what makes it **queryable** via
   normal SQL (`SELECT * FROM imaging_reports WHERE extracted_text LIKE '%consent%'`).
7. Return `{encrypted, encryptedSize, storage, extractedTextLength,
   merkleRoot, ...}` so the UI can show the user what happened.

The temp file exists for one function call (under a millisecond). After
that point, the only forms of the document on disk are (a) the DuckDB
row containing the extracted **text** (searchable metadata) and (b) the
Fernet ciphertext blob in MinIO.

### Auth-gated download

`GET /api/documents/<did>/<doc_id>/download?caller=...` —
[download_document at L1227](../../backend/app.py#L1227), gated by
[_can_download_document at L1207](../../backend/app.py#L1207).

Authorization check:

- Stewards pass unconditionally.
- Custodians pass if they're on `S["custodians"][did]`.
- Everyone else needs an active grant at level `VIEW_DOWNLOAD` or
  `FULL_ACCESS` that hasn't expired.

On success:

1. Load the ciphertext from the location recorded in `_doc_store`.
2. `_decrypt_bytes(did, ciphertext)` — this is the **only place in the
   codebase** where plaintext exists after the upload.
3. Append a JSONL record to `backend/data/document_access_log.jsonl`
   with `{t, caller, did, doc_id, reason, bytes}` — every decryption
   is logged.
4. Stream bytes back via `send_file` with `as_attachment=True`.

### PDF text extraction

[_extract_text at L461](../../backend/app.py#L461). Prefers `pypdf>=4.0`
(the maintained successor to PyPDF2), falls back to PyPDF2 if only the
legacy is installed, returns a clearly-marked placeholder for scanned
PDFs with no extractable text (the hook for adding `pytesseract` /
`pdf2image` OCR is obvious from the error string).

### End-to-end integrity test (verified on the running stack)

```
upload sample.pdf  (552 bytes)
 ─► encrypted size 824 bytes
 ─► PUT vlake-encrypted/datasets/1/DOC-5B7DBF34.bin
 ─► mc cat shows "gAAAAA..." (Fernet prefix, not PDF header)
GET /api/documents/1/DOC-5B7DBF34/download?caller=0xdeadbeef → 403 "no active grant"
GET /api/documents/1/DOC-5B7DBF34/download?caller=<steward>  → 200, 552 bytes
sha256(decrypted)  ==  sha256(original)  ✓
```

This is documented in the README's Security model section; the
round-trip was re-run as part of the fresh-clone README verification.

---

## 8. What I'd finish before shipping to a real deployment

- Rotate the dev master key out of `backend/data/.master.key` and source
  `VLAKE_MASTER_KEY` from AWS KMS or Vault. The `_init_encryption` path
  already supports this — it just reads the env var before the on-disk
  fallback — so the rotation is a deployment concern, not a code change.
- Add `scripts/rotate_master_key.py` to re-wrap every DEK with a new
  master key in one atomic pass. Roughly 30 lines: decrypt each wrapped
  DEK with the old Fernet, re-encrypt with the new Fernet, atomic-rename
  `deks.json`.
- Replace the single-node dev Besu with a four-validator QBFT setup
  using `scripts/setup_besu.sh` + a `docker-compose.qbft.yml` overlay.
  Genesis and validator keys already exist under `config/besu/`; the
  blocker is wiring the four services into compose with shared bootnodes.
- Add a second Merkle commitment path for documents specifically —
  right now document rows are hashed the same way structured rows are,
  but using the *extracted text* as the canonical payload. For an
  end-to-end integrity proof of a PDF you'd want the leaf to commit to
  the ciphertext hash, not just the extracted text.
- Key rotation for DEKs themselves (not just the KEK). This is
  non-trivial because every blob would need to be re-encrypted with
  the new DEK; the most pragmatic design is lazy re-encryption on next
  read.
