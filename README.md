# V-Lake: Verifiable Lakehouse Access & Knowledge Engine

> A blockchain-enabled data lakehouse that lets independent organizations share
> regulated data without trusting any single one of them. Reference
> implementation of the V-Lake architecture, deployed end-to-end on Hyperledger
> Besu, MinIO, Kafka, MongoDB, PostgreSQL and DuckDB.

V-Lake answers a hard question for cross-organisation data collaborations: *how
do you let analysts query data from many federated sources, while every
governance decision (who can read what, who consented, what was actually
returned) is auditable, tamper-evident and provably enforced?*

The system is built around four contributions, each implemented as code in this
repository, not just described:

| # | Contribution | What it gives you | Where it lives |
|---|---|---|---|
| **C1** | Domain-separated Merkle commitments | Every ingested batch is hashed into a typed Merkle tree and the root is anchored on-chain. Anyone can later prove a row was (or wasn't) part of an ingestion. | [backend/app.py](backend/app.py) §C1, [contracts/VLakeGovernance.sol](contracts/VLakeGovernance.sol) |
| **C2** | Weighted Quorum Consensus (WQC) | Stewards, custodians, analysts and subjects each carry Shapley-derived voting weights. Proposals (grants, revocations, custodian assignments) execute only when WQC clears the right quorum. | [backend/app.py](backend/app.py) §C2 |
| **C3** | Self-Sovereign Identity + consent chain | Subjects own their DIDs and grant/revoke access through a hash-chained consent log that the contract verifies on read. | [backend/app.py](backend/app.py) §C3 |
| **C4** | Federated data-source registry | One ingestion abstraction over CSV/JSON, S3/MinIO, PostgreSQL, MySQL, Trino, Delta, Iceberg, HDFS, Kafka, MongoDB and document uploads. | [backend/app.py](backend/app.py) §C4 |

The smart contract that arbitrates all of this is in
[contracts/VLakeGovernance.sol](contracts/VLakeGovernance.sol). Cache state in
the Flask backend is hydrated from the contract on startup, so removing Besu
intentionally breaks the security guarantees: V-Lake refuses to silently
degrade.

---

## Architecture at a glance

```
                ┌────────────────────────────────────────────┐
                │              Frontend (nginx)              │
                │   single-page React UI · :3000             │
                └───────────────┬────────────────────────────┘
                                │ REST/JSON
                ┌───────────────▼────────────────────────────┐
                │           V-Lake Backend (Flask)           │
                │     governance cache · DuckDB engine       │
                │     envelope encryption (Fernet)           │
                │     PDF text extraction (pypdf)            │
                └─┬─────────┬─────────┬──────────┬──────────┬┘
                  │         │         │          │          │
        ┌─────────▼─┐  ┌────▼────┐ ┌─▼──────┐ ┌─▼─────┐ ┌──▼─────┐
        │   Besu    │  │  MinIO  │ │ Kafka  │ │ Mongo │ │  Pg    │
        │ QBFT, RPC │  │ S3 API  │ │ stream │ │ vitals│ │ labs   │
        │  :8545    │  │ :9000/1 │ │ :9093  │ │:27017 │ │ :5432  │
        └───────────┘  └─────────┘ └────────┘ └───────┘ └────────┘

      governance writes ──► VLakeGovernance.sol on Besu (chain id 1337)
      raw documents     ──► encrypted blobs in MinIO bucket `vlake-encrypted`
      structured rows   ──► DuckDB virtual tables hashed into Merkle trees
```

Every dependency runs as a container in [docker-compose.yml](docker-compose.yml).

---

## Quick start

You need Docker Desktop running, Python 3.9+ and roughly 6 GB of free RAM.

### 1. Clone

```bash
git clone https://github.com/<you>/v-lake.git
cd v-lake
```

### 2. Create your `.env`

V-Lake reads runtime configuration from a `.env` file at the repo root.
**This file is not checked in** (it would leak secrets), so the very first
thing you do is copy the template and edit it:

```bash
cp .env.example .env
```

Open `.env` and at minimum set:

| Variable | Why it matters | How to fill it |
|---|---|---|
| `VLAKE_MASTER_KEY` | Wraps every per-dataset encryption key. If you leave it blank the backend boots a *dev* key on first run — fine for local hacking, **not for production**. | `python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"` and paste the output |
| `POSTGRES_PASSWORD` | Password for the bundled Postgres. | Anything non-default for prod; the demo defaults work locally |
| `MINIO_ACCESS_KEY` / `MINIO_SECRET_KEY` | MinIO root creds. | Defaults `minioadmin`/`minioadmin` work locally; rotate before exposing MinIO outside Docker |
| `CONTRACT_ADDRESS` | Address of the deployed governance contract. | Leave blank — `scripts/deploy_contract.py` fills this in for you (see step 4) |

`docker-compose.yml` reads from this file automatically. If you ever change a
value, recreate the affected container with `docker compose up -d <service>
--force-recreate`.

### 3. Boot the stack

```bash
docker compose up -d   # postgres, minio, kafka, mongo, besu, backend, frontend
```

### 4. Deploy the governance contract

```bash
pip install py-solc-x web3 eth-account
python scripts/deploy_contract.py
```

The deploy script compiles `VLakeGovernance.sol`, deploys it to Besu, writes
the ABI to `backend/contract_abi.json`, and **merges** `CONTRACT_ADDRESS`,
`BESU_RPC`, `CHAIN_ID` and a couple of audit fields into your existing `.env`.
It does *not* overwrite keys you set yourself — `VLAKE_MASTER_KEY` and
everything else are preserved.

### 5. Recreate the backend so it picks up the new contract

```bash
docker compose up -d backend --force-recreate
```

Now open **http://localhost:3000** for the UI and
**http://localhost:9001** for the MinIO console (login `minioadmin` /
`minioadmin`). The first run takes ~60 s while images build and Besu produces
its first blocks.

A scripted version of the same flow is in [run.ps1](run.ps1) for Windows.

---

## Document upload, query and download

V-Lake handles unstructured uploads (PDF, PNG, JPEG, DOCX, TXT) the same way it
handles a SQL ingest: text is extracted, indexed, hashed into the Merkle tree,
and the *raw bytes* are encrypted before they touch storage.

```
upload ──► extract text (pypdf for PDF) ──► row in DuckDB doc table
       └─► encrypt raw bytes with the dataset's DEK
            └─► PUT to MinIO bucket `vlake-encrypted/datasets/<did>/<doc_id>.bin`
                 (falls back to local disk only if MinIO is unreachable)
```

**Querying contents.** The extracted text is a regular column in the dataset's
DuckDB table, so analysts query it the same way as any other field — including
SQL `LIKE`, `regexp_matches()`, or full-text predicates — and every query is
gated by the same WQC grants and HIPAA/GDPR column policies as structured data.
Scanned PDFs return a marker string today; OCR (`pytesseract` + `pdf2image`) is
the obvious extension and is wired in as a TODO in `_extract_text`.

**Downloading the original.** The plaintext bytes are only ever reassembled in
memory inside `GET /api/documents/<did>/<doc_id>/download`, which enforces the
same authorization gate as the query endpoint:

```bash
curl -OJ "http://localhost:5000/api/documents/1/DOC-AB12CD34/download?caller=0x...steward"
```

Stewards and assigned custodians always pass; everyone else needs an active
grant at level `VIEW_DOWNLOAD` or `FULL_ACCESS` that has not expired. Every
successful decryption is appended to `backend/data/document_access_log.jsonl`
and to the on-chain audit trail.

---

## Security model

| Concern | How V-Lake handles it |
|---|---|
| **Data at rest** | Envelope encryption: a master KEK (Fernet, from `VLAKE_MASTER_KEY`) wraps a unique DEK per dataset. Raw uploads are AES-128-CBC + HMAC-SHA256 (Fernet) encrypted before being stored in MinIO. |
| **Data in transit** | All inter-service traffic stays on the Docker bridge network. For external exposure, terminate TLS at the nginx frontend or an upstream reverse proxy (not bundled — production deployments differ). |
| **Authorization** | Every read goes through `_can_download_document` / `execute_query`, which checks role, dataset-specific custodian assignment, grant level, and expiry. Subjects only see rows their consent chain authorises. |
| **Auditability** | All governance state changes (`createDataset`, `vote`, `recordIngestion`, `recordAttestation`, `_appendConsent`) are written to the smart contract via `_write_to_chain`. If the chain is unreachable the backend logs the attempt locally and **refuses to claim production-grade safety**. |
| **Integrity** | Each ingested batch's Merkle root is anchored on-chain. The forest root over all datasets is computable from `compute_forest_root()` and lets verifiers prove the global state. |
| **Key management** | The dev master key lives at `backend/data/.master.key` (chmod 600, gitignored). For production, set `VLAKE_MASTER_KEY` from a real KMS — AWS KMS, HashiCorp Vault, or hardware-backed enclaves — and remove the on-disk file. |

### A note on the hardcoded keys you'll see in the source

Three files — [scripts/deploy_contract.py](scripts/deploy_contract.py),
[hardhat.config.js](hardhat.config.js), and [backend/app.py](backend/app.py) —
contain hex private keys in clearly-labelled blocks. **These are public test
fixtures**, not secrets:

- The three steward keys are the well-known Hyperledger Besu / Truffle
  dev-mode keys, pre-funded in the Besu dev genesis and documented in
  thousands of public repos.
- The two demo subject keys are the standard Hardhat/Foundry default
  account keys.

They only control identities on a local chain id 1337 with no real funds,
and they exist only so the bundled demo runs out of the box. **Before you
run V-Lake against any real network, delete these blocks and load steward
keys from a KMS, Vault, HSM, or hardware wallet.** GitHub's secret scanner
may still flag them — that flag is informational, not a leak.

### Generating a production master key

```bash
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
# paste into .env as VLAKE_MASTER_KEY=...
docker compose up -d backend --force-recreate
```

If you rotate the master key, you must re-wrap every entry in
`backend/data/deks.json`. A `scripts/rotate_master_key.py` helper is on the
roadmap; until then, treat key rotation as a planned maintenance window.

---

## Service map

| Service | Port | Purpose |
|---|---|---|
| Frontend | 3000 | Single-page UI (nginx-served) |
| Backend | 5000 | Flask API · DuckDB engine · governance cache |
| Besu | 8545 | Hyperledger Besu (QBFT, chain id 1337) |
| PostgreSQL | 5432 | Lab results (relational source) |
| MinIO | 9000 / 9001 | S3 API + console; holds enrollment CSV and **encrypted document blobs** |
| Kafka | 9093 | Adverse-event stream |
| MongoDB | 27017 | Real-time vitals |

## API surface (selected)

| Method | Path | Purpose |
|---|---|---|
| `GET`  | `/api/health` | Liveness |
| `GET`  | `/api/blockchain/status` | Besu connection + contract address + mode |
| `POST` | `/api/ingest/upload` | Structured CSV/JSON upload |
| `POST` | `/api/ingest/stream` | JSON-array ingest |
| `POST` | `/api/ingest/document` | Encrypted document upload (PDF, image, DOCX, TXT) |
| `GET`  | `/api/documents/<did>` | List documents in a dataset |
| `GET`  | `/api/documents/<did>/<doc_id>/download?caller=<addr>` | Auth-gated decrypted download |
| `POST` | `/api/proposals` | Create a governance proposal |
| `POST` | `/api/proposals/<pid>/vote` | Steward or custodian vote |
| `POST` | `/api/query` | Execute a SQL query, with grant + policy enforcement |

## Repository layout

```
.
├── backend/                # Flask backend
│   ├── app.py              # main service: governance, ingest, query, encryption
│   ├── requirements.txt
│   └── data/               # runtime state (gitignored)
├── contracts/              # Solidity smart contract
│   └── VLakeGovernance.sol
├── frontend/               # single-page UI
│   └── index.html
├── scripts/
│   ├── deploy_contract.py  # compiles + deploys VLakeGovernance to Besu
│   ├── seed_all.py         # seeds Postgres, MinIO, Kafka, MongoDB
│   ├── benchmark.py        # micro-benchmarks for the four contributions
│   └── evaluate.py
├── docker/                 # Dockerfiles for backend, frontend, besu
├── docker-compose.yml      # full stack
├── .env.example            # configuration template
└── README.md
```

---

## Stopping & cleanup

```bash
docker compose down            # stop containers, keep data
docker compose down -v         # stop + delete all volumes (full reset)
```

Because the dev Besu node uses an ephemeral data path (see
[docker-compose.yml](docker-compose.yml)), a `down -v` followed by a re-up will
require redeploying the contract. The deploy script writes the new
`CONTRACT_ADDRESS` back into `.env` automatically.

## Troubleshooting

| Symptom | Fix |
|---|---|
| `Cache-Only Mode — Blockchain Not Connected` banner | The backend can't reach Besu or no contract is deployed. Run `python scripts/deploy_contract.py` and recreate the backend container. |
| `Cannot connect to Besu` during deploy | Wait ~30 s after `docker compose up -d` so Besu can produce its first block. |
| `Encryption failed - check VLAKE_MASTER_KEY` | The configured `VLAKE_MASTER_KEY` is malformed. Regenerate one with the snippet under [Security model](#security-model). |
| `[PDF extraction unavailable: pip install pypdf]` | Old image — rebuild the backend (`docker compose build backend`). |
| `Unknown options: --miner-enabled` from Besu | You pulled a Besu version newer than 24.x. The compose file pins `hyperledger/besu:24.1.0` for that reason — don't change it without porting to QBFT or another consensus that produces blocks. |

## Limitations & roadmap

- **OCR for scanned PDFs.** `_extract_text` reports image-only PDFs but does
  not yet OCR them. Adding `pytesseract` + `pdf2image` is the obvious next step.
- **Key rotation.** Rotating the master KEK currently requires a manual
  re-wrap of `backend/data/deks.json`.
- **Real KMS integration.** The dev master key lives on disk. For real
  deployments, fetch `VLAKE_MASTER_KEY` from AWS KMS, GCP KMS, Vault, or an HSM.
- **Multi-node QBFT.** The bundled compose file runs a single Besu dev node.
  A `docker-compose.qbft.yml` overlay with four validators is on the roadmap.
- **TLS termination.** Production deployments should put nginx (or any reverse
  proxy) with a real certificate in front of the backend; the bundled stack
  assumes localhost.

## License

This is reference code released alongside the V-Lake systems paper. See
`LICENSE` (add your preferred OSS license before publishing).

## Citation

If you build on V-Lake, please cite the paper this implementation accompanies.
A `CITATION.cff` file is the easy way to do that — add one before the first
public release.
