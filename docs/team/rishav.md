# Rishav — Frontend Portal, Demo Orchestration, Audit & SSI

**Scope:** everything a human touches. The single-page React UI that drives
the whole system, the scripted "multi-site clinical trial" demo that walks
a viewer through all four contributions, the append-only audit trails that
prove what happened, the Self-Sovereign Identity flow (subject registration,
linkage, delegation, revocation), and the Flask endpoints that back all of it.

---

## 1. The portal (frontend SPA)

**File:** [frontend/index.html](../../frontend/index.html) — 1233 lines,
single self-contained file (React via CDN, no build step), served by nginx
on port 3000.

### Why one file, no build

A paper-demo repo needs to open cleanly on a reviewer's machine five
minutes after `git clone`. A Vite/webpack build adds npm/node as a
dependency and an extra minute of tooling friction. Using React from
JSDelivr + in-browser Babel means the frontend container is literally
nginx with one static file, and reviewers can poke at the UI source by
opening `frontend/index.html` in a text editor.

Tradeoffs I accepted: no tree-shaking, slower first paint, all styles
inline in a `<style>` block. For a research demo these are the right
calls; for production you'd switch to Vite.

### View structure

The portal is organised around seven views, each rendered based on
`activeView` state in the sidebar:

| View | Lines (approx) | Purpose |
|---|---|---|
| **Dashboard**   | [frontend/index.html:280–360](../../frontend/index.html#L280) | System status, blockchain health, dataset count, recent activity |
| **Datasets**    | [L370–460](../../frontend/index.html#L370) | Create datasets, view row counts / Merkle roots, ingest data |
| **Ingest**      | [L480–550](../../frontend/index.html#L480) | Structured upload (CSV/JSON), streaming JSON, encrypted document upload |
| **Query**       | [L620–680](../../frontend/index.html#L620) | SQL console with predicate-injection preview |
| **Governance**  | [L690–870](../../frontend/index.html#L690) | Proposals list, WQC state, vote buttons, grant revocation |
| **Compliance**  | [L880–920](../../frontend/index.html#L880) | HIPAA / GDPR / DPDPA policy attachment, attestation log |
| **SSI**         | [L930–1020](../../frontend/index.html#L930) | Subject registration, linkage, delegation, consent chain viewer |
| **Audit**       | [L1050–1110](../../frontend/index.html#L1050) | Merkle history, query log, attestation chain |
| **Demo**        | [L1110–1200](../../frontend/index.html#L1110) | Scripted walkthrough runner |

### The `api()` helper

[frontend/index.html:223](../../frontend/index.html#L223) — one function
that owns every fetch. Auto-prefixes `${API}` (the backend origin),
JSON-encodes non-FormData bodies, sets `Content-Type: application/json`
except when uploading files, and surfaces backend error strings to the
toast system. Every component goes through this helper; there is no
second way to hit the backend from the UI.

This matters for consistency: all error paths toast the same way, all
request bodies encode the same way, and if we ever need to add auth
headers it's one place to change.

### Shared refresh cycle

[frontend/index.html:306–312](../../frontend/index.html#L306) — the
Dashboard polls `/datasets`, `/proposals`, `/audit/queries`, and
`/grants` in parallel on a refresh tick and fans the results out via
`setState`. Every other view subscribes to the same refresh by sharing
a `useState` setter, so a governance vote that executes a proposal
updates the Datasets view's row counts and the Audit view's Merkle
history in the same React tick. No websockets needed — the demo runs
fast enough that polling on action is indistinguishable from push.

---

## 2. Demo orchestration (the 20-step walkthrough)

**Data:** [backend/app.py `DEMO_STEPS` at L1693](../../backend/app.py#L1693)
(20 steps, each tagged with the contribution it demonstrates).
**Executor:** [`demo_next` at L1731](../../backend/app.py#L1731).
**UI runner:** [frontend/index.html Demo view at L1110](../../frontend/index.html#L1110).

The demo is the single most important UX feature of V-Lake: it lets a
reviewer watch all four contributions in action without reading any code.
A click on *Next Step* posts to `/api/demo/next`, which advances the
server-side counter `S["demo_step"]`, dispatches into the corresponding
handler branch, and returns a structured result the UI renders as a
card with contribution tag, title, description, and a typed `details`
payload.

### The 20 steps

| # | Step | Contribution | What it does |
|---|---|---|---|
| 0 | create_datasets          | C4       | Create 5 datasets: enrollment, adverse events, labs, vitals, documents |
| 1 | ingest_enrollment        | C1+C4    | Read `trial_enrollment.csv` from a real MinIO bucket via DuckDB httpfs |
| 2 | ingest_adverse_events    | C1+C4    | Consume events from a real Kafka topic |
| 3 | ingest_labs              | C1+C4    | Federated query against PostgreSQL via `postgres_scanner` |
| 4 | ingest_vitals            | C1+C4    | Export a MongoDB collection via `pymongo` |
| 5 | ingest_documents         | C1+C4    | Download consent PDFs from MinIO, extract text, Merkle-commit |
| 6 | attach_policies          | Compliance | HIPAA + GDPR on confidential datasets |
| 7 | assign_custodian         | **C2**   | 2 stewards vote `ASSIGN_CUSTODIAN` → standard quorum clears |
| 8 | onboard_analyst          | **C2**   | All 3 stewards + custodian vote `ONBOARD_ANALYST` → PHI columns auto-restricted |
| 9 | register_participant     | **C3**   | Subject gets a DID, linked to enrollment with row filter `patient_id='P0001'` |
| 10 | query_sponsor           | C1       | Steward query — HIPAA policy auto-redacts PHI columns even for stewards |
| 11 | query_biostat           | predicate | Analyst query — predicate injection drops PHI columns |
| 12 | query_participant       | **C3**   | Subject sees only their own row via SSI-derived WHERE clause |
| 13 | delegate_investigator   | **C3**   | Subject delegates their row to a doctor — steward-independent (sovereign right) |
| 14 | query_investigator      | **C3**   | Doctor inherits row filter + scoped columns from the delegation |
| 15 | revoke_investigator     | **C3**   | Instant revocation with grant-cache invalidation |
| 16 | cross_source_query      | C4       | JOIN enrollment (CSV) × adverse_events (Kafka) in one SQL call |
| 17 | verify_merkle           | C1       | Recompute all roots, generate inclusion proofs |
| 18 | verify_consent          | **C3**   | Walk the SSI chain and verify every `prev_hash` linkage |
| 19 | emergency_revoke        | **C2**   | Single-steward break-glass via `REVOKE_ANALYST` |

### The `_iq` helper

[backend/app.py L1912](../../backend/app.py#L1912) — a tiny function that
lets demo steps reuse the real query endpoint without hitting the network.
It builds a fake Flask request context with the demo caller/dataset/query
and returns the parsed JSON the real endpoint would emit. This means
`query_sponsor`, `query_biostat`, `query_participant`, `query_investigator`
all go through *exactly the same* grant checks, predicate injection,
compliance evaluation, and on-chain logging as a real UI query. The
demo isn't a shortcut; it's a scripted user.

### Reset path

[`demo_reset` at L1719](../../backend/app.py#L1719) — wipes the in-memory
cache `S` back to the boot state, resets the grant cache and Merkle cache,
drops every DuckDB table, and clears the document store. A reviewer can
click Reset between runs and watch the whole system come back to a clean
slate in about 200 ms.

---

## 3. Audit trails — three independent logs

V-Lake runs three append-only JSONL logs on disk, each with a different
integrity story:

| Log | File | Written by | Purpose |
|---|---|---|---|
| **Governance audit** | `backend/data/governance_audit_log.jsonl` | `_write_to_chain` | Every on-chain state change attempt (successful or failed). Forensic trail when Besu is unreachable. |
| **Merkle audit**     | `backend/data/merkle_audit_log.jsonl`     | `_post_ingest`    | Every ingestion's Merkle root + leaf count + row count + timestamp. Tamper-evident without the chain. |
| **Document access**  | `backend/data/document_access_log.jsonl`  | `download_document` | Every decryption of a document blob, with caller + reason + byte count. |

I wired up the document-access log at
[backend/app.py:1252-1259](../../backend/app.py#L1252) so every
`GET /api/documents/<did>/<doc_id>/download` that clears the auth gate
appends `{t, caller, did, doc_id, reason, bytes}`. This is the paper
trail that tells you not just "who has access" but "who actually
exercised that access."

### In-memory audit state surfaced in the UI

The **Audit view** reads from:

- `GET /api/audit/queries?limit=50` — [backend/app.py:1489](../../backend/app.py#L1489)
  — returns the last 50 query executions with querier, dataset, query text,
  result-row count, compliance verdict, and attestation hash.
- `GET /api/audit/merkle/<did>` — [L1492](../../backend/app.py#L1492) —
  returns the full history of Merkle roots for a dataset so reviewers can
  watch it evolve across ingestions.
- `GET /api/merkle/forest` — [L1514](../../backend/app.py#L1514) —
  returns the current forest root over all datasets.
- `GET /api/compliance/attestations` — [L1455](../../backend/app.py#L1455)
  — returns the last 50 attestations, each of which is itself a
  hash-chained record (every attestation commits to the previous one).

### Query logging pipeline

Every query that clears predicate injection + compliance evaluation is
logged through the same pipeline:

1. `execute_query` ([L1304](../../backend/app.py#L1304)) runs the SQL
   against the dataset's DuckDB table with predicates injected from
   the caller's grant.
2. `check_compliance` ([L784](../../backend/app.py#L784)) walks the
   attached policies (HIPAA, GDPR, DPDPA), checks each rule (column
   sensitivity, temporal validity, minimum-necessary, audit), and
   produces a hash-chained attestation record.
3. The query is appended to `S["query_logs"]` with a row count, a
   result hash (so the frontend can prove later it hasn't been
   tampered with), and the attestation.
4. `_record_attestation_on_chain` ([L151](../../backend/app.py#L151))
   anchors the attestation hash on Besu.

Everything above is visible in the UI's Audit view in real time.

---

## 4. Self-Sovereign Identity (C3)

C3 is the most subtle contribution: a subject (trial participant) owns
their data access, not a steward. Stewards can *propose* grants for
analysts, but a subject can *unilaterally* register a DID, link their
data, delegate access to a delegate, and revoke — no steward approval
required, ever. This is the "sovereign right" described in the paper.

### Consent chain data model

`S["ssi_consents"]` is a hash-linked list of `ConsentRecord` dicts:

```
{
  subject:    "0x666...",
  action:     "LINK" | "DELEGATE" | "REVOKE",
  dataset:    "1",
  filter:     "patient_id='P0001'",
  delegate:   "0x777...",       # present for DELEGATE/REVOKE
  scope:      "age,gender,bp",  # present for DELEGATE
  expiresAt:  1775912524,       # present for DELEGATE
  timestamp:  1775909380,
  prev_hash:  "abc..." | "genesis",
  hash:       sha256(canonical(record)),
  signature:  <65-byte ECDSA sig over payload>,
  signer:     "0x666...",
}
```

### Append function

[`_append_consent` at L954](../../backend/app.py#L954). Every new record:

1. Reads the previous head (`S["ssi_consents"][-1]["hash"]`), or
   `"genesis"` if the chain is empty.
2. Sets `prev_hash = <head>`.
3. Canonicalises the record (`json.dumps(..., sort_keys=True,
   default=str)`) and computes `hash = sha256(...)`.
4. Signs `action || dataset || filter || timestamp || prev_hash` with
   the subject's private key (ECDSA over secp256k1, via `eth_account`).
5. Calls `_record_consent_on_chain` so the record is mirrored to
   `VLakeGovernance.sol`'s on-chain consent chain.

The private key used for signing in the demo is the well-known
Hardhat default account #5 / #6 key, clearly banner-labelled as a
public test fixture at [L962-968](../../backend/app.py#L962). In
production, subjects sign client-side (browser wallet or mobile
agent); the server-side signing is only so the bundled demo can
produce real signatures without a wallet.

### Chain verification

`GET /api/ssi/verify-chain` — [L1424](../../backend/app.py#L1424) —
walks the list, checks every record's `prev_hash` against the previous
record's `hash`, returns `{valid: bool, length, headHash}`. The UI's
SSI view polls this on load so broken chains are visible immediately.

The demo step **verify_consent** (step 18) calls this endpoint and
displays the result in a card. Tampering with a past record breaks
every subsequent hash, which is the whole point of a hash chain.

### The four SSI endpoints

| Endpoint | Line | Caller | Effect |
|---|---|---|---|
| `POST /api/auth/register-subject` | [L1039](../../backend/app.py#L1039) | Anyone (the subject) | Creates `S["ssi_did_registry"][addr]` with a `did:vlake:<hash>` identifier |
| `POST /api/subjects/link`         | [L1384](../../backend/app.py#L1384) | Steward/Custodian (on behalf of subject during onboarding) | Links a subject to a dataset with a row filter, emits `LINK` consent record |
| `POST /api/subjects/delegate`     | [L1395](../../backend/app.py#L1395) | Subject | Delegates their access to another address with a scope and duration, emits `DELEGATE` consent |
| `POST /api/subjects/revoke-delegation` | [L1408](../../backend/app.py#L1408) | Subject | Deactivates the delegate's grant, invalidates the grant cache, emits `REVOKE` consent |

The key property: `delegate` and `revoke-delegation` do **not** create a
proposal. They execute immediately. That's the "no steward approval
needed" part — the subject's consent chain record IS the authorization.

### Grant cache invalidation on revocation

When a subject revokes, [`delegate_access` at L1395](../../backend/app.py#L1395)
and [`revoke_delegation` at L1408](../../backend/app.py#L1408) both call
`cache_inv` ([L614](../../backend/app.py#L614)) to drop the delegate's
cached grant. Without this, a revocation would take up to 30 seconds to
propagate (the grant cache TTL at [L601](../../backend/app.py#L601)) —
which violates the "instant revocation" promise.

### Frontend SSI view

[frontend/index.html:930-1020](../../frontend/index.html#L930). Has four
forms:

- **Register subject** — binds a wallet address to a DID.
- **Link subject to dataset** — steward-initiated, creates the row-filtered
  grant.
- **Delegate access** — subject-initiated, takes a delegate address, scope
  (column list), and duration in seconds.
- **Revoke delegation** — one click; the consent chain immediately grows
  a REVOKE record and the UI re-polls `/ssi/verify-chain` to show the
  new head.

The **Consent Chain** panel below the forms is a live scrollable log of
every record in `S["ssi_consents"]`, colour-coded by action. The
verification badge is green when `valid: true` and red when broken.

---

## 5. Frontend-specific touches worth noting

- **Toast notifications** — a single `toast(msg, type)` helper used
  everywhere, backed by a small queue so multiple actions stack without
  flicker.
- **Role-aware UI** — the active user address determines which buttons
  render (stewards see "Revoke grant", analysts see their own restricted
  view, subjects see only the SSI and Audit tabs).
- **Code-friendly typography** — JetBrains Mono for all hashes and
  Merkle roots; Source Sans for body; Source Serif for headings. The
  choice is deliberate because a viewer spends a lot of time scanning
  hex strings and proof chains.
- **CORS wired on the Flask side** — [`add_cors` / `handle_preflight`
  at L37-49](../../backend/app.py#L37). Flask returns
  `Access-Control-Allow-Origin: *` and handles OPTIONS preflights
  manually so the SPA works when served from any origin (file://,
  localhost:3000, anything).

---

## 6. What I'd finish before shipping

- **Auth proper.** Right now the SPA passes `caller=<addr>` as a plain
  string. The backend has `_verify_caller` ([L1001](../../backend/app.py#L1001))
  but it isn't wired into every endpoint. A real deployment needs every
  mutating request to carry a signed challenge, and the frontend needs to
  integrate with MetaMask or WalletConnect for client-side signing.
- **Live updates.** Polling is fine for the demo but an audit-heavy
  production deployment wants a `/api/events` SSE stream so Merkle
  roots and compliance attestations appear in the UI the instant they
  land.
- **Frontend download for encrypted documents.** The download endpoint
  exists (Srinath wired it), but the SPA doesn't render a "Download"
  button for documents yet. Adding it is ~15 lines in the Datasets
  view — I wanted the auth flow cleaned up first.
- **Better SSI UX.** The delegation form currently takes raw addresses;
  a real subject doesn't know what their doctor's address is. Needs
  either a DID resolver (lookup `did:vlake:*` by human-readable handle)
  or a QR-code flow.
- **Dark mode.** One of the CSS variables away. I just didn't get to it.
