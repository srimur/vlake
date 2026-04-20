# Manas — DuckDB Engine, Federated Data Sources & Connector-Side Security

**Scope:** the data plane. DuckDB as the in-process query engine, the
federated source registry that lets a single SQL query span CSV, JSON,
PostgreSQL, MySQL, S3/MinIO, Kafka, MongoDB, Trino, Delta Lake, Iceberg and
HDFS, the per-source connection + ingest code paths, and the security
considerations on the connector side (sensitive-column classification,
policy-driven predicate injection, credential handling per source type,
and how the data I pull in gets protected by the encryption layer before
it ends up on disk).

---

## 1. Why DuckDB

V-Lake needs an in-process analytical engine that can:

- Join across heterogeneous sources in one query plan (CSV × Postgres ×
  Kafka-captured JSON).
- Run inside the Flask backend container with no separate server.
- Expose extensions for common data sources (Postgres, MySQL, S3/HTTPFS,
  Parquet, Delta, Iceberg).
- Re-hash its tables cheaply so the Merkle layer can recompute integrity
  commitments after every ingest.

DuckDB checks every box. The alternative would be Trino or Presto as a
separate server, which would double the container count and force
cross-network fetches for every query. By pulling rows into DuckDB
we get zero-copy `SELECT`s and one writer per table, which makes the
Merkle recompute deterministic.

**Engine init:** [backend/app.py L293-296](../../backend/app.py#L293):

```
import duckdb
duck = duckdb.connect(database=":memory:")
try: duck.execute("INSTALL httpfs; LOAD httpfs;")
except: pass
```

The engine is `:memory:` — DuckDB's on-disk mode would persist the
dataset tables through a restart, which conflicts with the
"everything is derived from the source" model. If Postgres changes,
we want the next ingest to pull fresh rows, not read a stale DuckDB
checkpoint.

**Extensions baked into the image:** [docker/Dockerfile.backend L13](../../docker/Dockerfile.backend#L13)
pre-installs `httpfs`, `postgres_scanner`, `mysql_scanner` at build
time, so the first query doesn't pay the extension-download latency.

---

## 2. The federated source registry (C4)

**Registry:** [backend/app.py `DATA_SOURCE_TYPES` at L297](../../backend/app.py#L297).
**Connector:** [backend/app.py `connect_source` at L341](../../backend/app.py#L341).
**Per-source ingest API:** `POST /api/sources/connect`
([L1051](../../backend/app.py#L1051)).

### Ten source types

| Type | Label | Required fields |
|---|---|---|
| `LOCAL_FILE`  | Local File Upload | `file_path` |
| `S3_MINIO`    | S3 / MinIO        | `endpoint, bucket, access_key, secret_key, path_prefix, file_format` |
| `POSTGRESQL`  | PostgreSQL        | `host, port, database, username, password, schema_name, table_name` |
| `MYSQL`       | MySQL / MariaDB   | `host, port, database, username, password, table_name` |
| `TRINO`       | Trino / Starburst | `host, port, catalog, schema_name, table_name, username` |
| `DELTA_LAKE`  | Delta Lake        | `path, storage_type` |
| `ICEBERG`     | Apache Iceberg    | `catalog_uri, warehouse, namespace, table_name` |
| `HDFS`        | HDFS / Hadoop     | `namenode_host, namenode_port, path, file_format` |
| `KAFKA`       | Kafka Stream      | `bootstrap_servers, topic, group_id, format` |
| `MONGODB`     | MongoDB           | `connection_string, database, collection` |

All ten are registered in `DATA_SOURCE_TYPES` with their human-readable
label and the field list the UI needs to render a connection form. The
registry is a single source of truth: the frontend's Ingest view renders
forms directly from `GET /api/sources/types`
([L1048](../../backend/app.py#L1048)).

### The `connect_source` dispatcher

[backend/app.py L341-453](../../backend/app.py#L341) — one function with
a branch per source type. Each branch:

1. Pulls config from the request, falling back to env vars as defaults
   (so `docker-compose.yml` can seed credentials without the UI
   re-entering them).
2. Loads the DuckDB extension if needed.
3. Runs the ingest query.
4. Registers the resulting table in the virtual-tables catalog via
   `_register`.

Every branch follows the same "connect, pull, register" pattern so
adding an 11th source is purely additive.

### Per-source implementation notes

- **LOCAL_FILE** ([L345-350](../../backend/app.py#L345)) — uses DuckDB's
  `read_csv_auto` / `read_json_auto`. Supports CSV and JSON/JSONL; other
  extensions raise a clear error. The type detector runs on the first
  1024 rows; if your CSV has an unusual schema, override via the
  structured upload endpoint that takes a raw `CREATE TABLE ... AS SELECT`
  statement.

- **POSTGRESQL** ([L352-367](../../backend/app.py#L352)) — uses DuckDB's
  `postgres_scanner` extension. The query is
  `CREATE OR REPLACE TABLE "<vlake_tbl>" AS SELECT * FROM postgres_scan('<conn>', '<schema>', '<table>')`.
  `postgres_scan` pushes predicates down into the remote Postgres when
  it can, so for simple projections the rows are filtered server-side.
  **Security note:** the password is scrubbed from the stored
  connector metadata at [L367](../../backend/app.py#L367) before it's
  written to `virtual_tables[did]["connector"]`, so it never shows up
  in a `GET /api/datasets` response.

- **S3_MINIO** ([L369-388](../../backend/app.py#L369)) — uses DuckDB's
  `httpfs` extension. The endpoint is SET via session variables right
  before the `read_csv_auto` call, and the path-style URL (`s3://bkt/prefix*.csv`)
  matches MinIO's default layout. Supports CSV, JSON, and Parquet per
  `file_format`. Credentials are stripped from the connector metadata
  at [L388](../../backend/app.py#L388).

- **KAFKA** ([L390-416](../../backend/app.py#L390)) — DuckDB doesn't
  have a Kafka extension, so I use `kafka-python-ng` directly.
  `auto_offset_reset='earliest'` means ingesting an existing topic
  replays every historical message; `consumer_timeout_ms=5000` caps
  the poll so a slow topic can't hang an ingest. Messages get
  materialized to a temp JSON file in `DATA_DIR` and then loaded
  via `read_json_auto`. The temp file is kept (not deleted) so a
  reviewer can inspect what was consumed.

- **MONGODB** ([L418-440](../../backend/app.py#L418)) — uses `pymongo`,
  finds with `{"_id": 0}` to strip the ObjectId (which otherwise
  breaks JSON serialization), writes to a temp JSON, loads via
  DuckDB. `serverSelectionTimeoutMS=5000` caps the connection wait.

- **MYSQL** ([L442-450](../../backend/app.py#L442)) — uses DuckDB's
  `mysql_scanner` extension, same pattern as Postgres. Simpler wiring
  because there's no schema namespace.

The four unimplemented source types — Trino, Delta Lake, Iceberg, HDFS —
are registered in `DATA_SOURCE_TYPES` but not in the dispatcher.
They're placeholders for future work; anyone extending the system
just adds a new `elif` branch following the same contract.

### `_register` — the virtual-table catalog

[backend/app.py L324-329](../../backend/app.py#L324):

```python
def _register(did, tbl, src, path=None, cfg=None):
    info = duck.execute(f'DESCRIBE "{tbl}"').fetchall()
    schema = [{"name": r[0], "type": r[1]} for r in info]
    rc = duck.execute(f'SELECT COUNT(*) FROM "{tbl}"').fetchone()[0]
    virtual_tables[str(did)] = {"source": src, "table": tbl, "schema": schema,
                                 "path": path, "row_count": rc,
                                 "connector": cfg or {},
                                 "connected_at": int(time.time())}
    return schema, rc
```

This is the single place the virtual-table catalog is mutated. Every
connector branch ends with a call to it, which means:

- The schema is always introspected from DuckDB itself (never trusted
  from the user input).
- The row count is real, not reported by the source.
- Connector metadata (scrubbed of credentials) is preserved so the UI
  can show "last ingested from postgres://... at <time>".

---

## 3. Data source seeding (so the demo has something to pull)

**File:** [backend/app.py `_seed_all_sources` at L1989](../../backend/app.py#L1989)

On backend startup, before Flask serves its first request, the service
pre-populates every backing store so the demo walkthrough has real
data to federate over. This runs once per container boot:

- **MinIO:** uploads `enrollment/trial_enrollment.csv` (8 rows of
  synthetic trial participants, covering US + EU sites, Treatment +
  Placebo arms, one withdrawn consent) and `documents/consent_P0001.txt`.
- **Kafka:** produces 6 adverse-event JSON messages to topic
  `adverse_events` with a range of severities and causalities.
- **MongoDB:** inserts 30 synthetic vitals readings in `vitals_stream`
  with HR / BP / SpO2 / temperature, keyed to patient IDs P0001-P0004.
- **PostgreSQL:** seeded via [scripts/init_postgres.sql](../../scripts/init_postgres.sql),
  which Docker runs at container init via the volume mount in
  [docker-compose.yml L35](../../docker-compose.yml#L35). Populates
  `lab_results` with blood work + biomarkers.

Seed idempotence: the MinIO branch checks `bucket_exists` before
creating; MongoDB checks `count_documents({}) == 0` before inserting.
So restarting the backend doesn't duplicate data. Kafka is not
idempotent — it always produces — but the demo resets the entire
DuckDB state anyway.

### Why the backend seeds the sources (not a separate script)

The older version of V-Lake had a `scripts/seed_all.py` that the
user ran manually after `docker compose up`. Two problems: (1) the
user had to remember to run it, (2) the Python deps for all five
source libraries had to be installed on the host. Moving seeding
into `_seed_all_sources` means every container boot self-heals: the
demo data is always present, the host Python environment needs
nothing beyond `web3` + `py-solc-x` for contract deployment.

---

## 4. The query side — predicate injection & column restriction

**File:** [backend/app.py predicate-injection section at L618-681](../../backend/app.py#L618)

The data I ingest is useful only if analysts can query it, and useful
*safely* only if the queries obey the grants and compliance policies
attached to the dataset. That enforcement happens at query time via
predicate and column injection — *before* the SQL hits DuckDB.

### `inject_predicates`

[backend/app.py L647-680](../../backend/app.py#L647). Takes the raw
user query, the grant, the table name, and the schema, and rewrites
it to enforce:

1. **Column restriction.** If the grant's `allowedColumns` is non-empty,
   `SELECT *` is rewritten to `SELECT col1, col2, col3` with only the
   allowed columns. Non-star queries are filtered — any referenced
   column not in the allow-list raises an error before the query runs.
2. **Row filter.** The grant's `rowFilter` (e.g. `patient_id='P0001'`)
   is AND'd into the WHERE clause. Subjects always see this injected.
3. **Validation.** `_validate_row_filter` at [L635](../../backend/app.py#L635)
   refuses anything that looks like SQL injection (semicolons, comment
   markers, dangerous keywords).

The injected SQL is what actually runs against DuckDB, so grant
enforcement is a single rewrite pass, not a post-filter. That's
important because a post-filter would leak row counts ("you aren't
allowed to see these rows, but we loaded them and then hid them").

### Column classification for compliance

**File:** [backend/app.py `COMPLIANCE_RULES` at L777](../../backend/app.py#L777)

```
HIPAA: name, patient_name, ssn, phone, email, address, dob, ip_address, ...
GDPR:  name, email, phone, address, dob, national_id, ip_address, ...
DPDPA: name, aadhaar, pan, phone, email, address, dob, ...
```

These are the baked-in sensitive-column lists per standard. When an
analyst is onboarded via `ONBOARD_ANALYST` ([_execute_proposal at
L911](../../backend/app.py#L911)), the backend intersects the dataset's
attached policies with the dataset's actual schema and restricts the
grant's `allowedColumns` to the non-sensitive columns. So onboarding
an analyst to a HIPAA-protected enrollment dataset automatically
strips `patient_name`, `contact_phone`, `contact_email` from their
grant — they can still query, they just can't see PHI.

`check_compliance` at [L784](../../backend/app.py#L784) runs the
same rules at query time as a second line of defence, producing an
attestation record that gets anchored on-chain. That means: even if
a grant was mis-computed, the query-time check will flag the
violation and the attestation log will record it.

---

## 5. Connector-side security

### Credential scrubbing

Every connector branch strips passwords / secret keys from the
metadata stored in `virtual_tables[did]["connector"]`:

- Postgres: `{k:v for k,v in config.items() if k!="password"}` at
  [L367](../../backend/app.py#L367).
- MySQL: same pattern at [L450](../../backend/app.py#L450).
- S3/MinIO: `if k not in ("access_key","secret_key")` at
  [L388](../../backend/app.py#L388).
- MongoDB: `if k!="connection_string"` at [L440](../../backend/app.py#L440)
  (because the URI contains credentials).

Without this, a steward hitting `GET /api/datasets` would see the
Postgres password of every federated source in the JSON response.
I made this audit-pass-safe from day one.

### Env-var fallbacks

Every branch reads its defaults from the environment, not from
hardcoded strings, so credentials flow in via `.env` →
`docker-compose.yml` → container env. The compose file forwards the
relevant vars with defaults via `${VAR:-default}` syntax — see
[docker-compose.yml L97-120](../../docker-compose.yml#L97). This is
the right pattern because it lets a production user rotate a
database password by editing `.env` and recreating the backend,
without touching any code.

### How connector data inherits the encryption layer

Structured rows from federated sources are loaded into DuckDB
in-memory — they aren't re-encrypted because (a) the source is
already authoritative, (b) the DuckDB state is in-process and
`:memory:`, (c) the compliance layer enforces column-level access
on every query, so unauthorized reads can't happen through the
query API.

**Documents** are different: they're raw files. When a document comes
from a federated source (e.g. the `ingest_documents` demo step at
[backend/app.py L1798-1820](../../backend/app.py#L1798), which pulls
consent PDFs from a MinIO bucket), the flow is:

1. Download the raw bytes from the source MinIO bucket.
2. Extract the searchable text via `_extract_text`
   ([L461](../../backend/app.py#L461)).
3. Call `ingest_document` ([L494](../../backend/app.py#L494)) to register
   the document in DuckDB with its extracted text.

The unstructured-document ingest path goes through the
encryption/storage layer Srinath owns. For the seed/demo path,
documents pulled from the `vlake-trial` source bucket are stored
as-is (they're demo fixtures, not user uploads), which is why the
download endpoint refuses to serve legacy (non-encrypted) documents
with a `409` — see the guard at [L1237](../../backend/app.py#L1237).
A future refactor would re-run seeded documents through the
encryption pipeline so **every** document in the system is
uniformly protected.

### MySQL/Postgres and DuckDB's credential handling

DuckDB's `postgres_scanner` and `mysql_scanner` extensions accept
the connection string as a SQL literal. That means the password
ends up in DuckDB's query log (`SHOW TABLES` is fine, but a
`EXPLAIN` on the virtual table shows the connection). I considered
wrapping these with a connection-secrets mechanism (pass the
password via an env var that DuckDB reads at runtime) but that
feature isn't stable across DuckDB versions. The current mitigation:
V-Lake doesn't expose `EXPLAIN` to users, and the connector config
is scrubbed before being stored in `virtual_tables`. For a real
production deployment, the better fix is to run Postgres behind a
service account with column-level GRANTs that match the V-Lake
grant structure, so even if the connection string leaked, the
leaked credentials couldn't exceed the grant.

---

## 6. What I'd finish before shipping

- **Implement Trino, Delta Lake, Iceberg, HDFS.** They're registered
  but not dispatched. DuckDB has extensions for Iceberg and Delta;
  Trino would need a `pytrino` client; HDFS is available via
  `hdfs3` or WebHDFS. Each is ~30 lines following the existing
  pattern.
- **Streaming ingestion for Kafka.** Right now I consume the topic
  once at ingest time. A real trial monitoring setup wants a
  long-running consumer that appends to the DuckDB table as
  messages arrive, with periodic Merkle re-anchors. This needs a
  background thread and careful coordination with the grant cache
  invalidation.
- **Schema drift detection.** If the upstream Postgres adds a
  column, the next ingest silently includes it in `SELECT *` and
  the Merkle root changes shape. A real deployment wants
  detection + a proposal-gated migration.
- **Connection pooling.** Every `connect_source` call opens a fresh
  Postgres / Mongo / Kafka client. For high-frequency ingests, a
  pool would cut the setup cost.
- **Query-time row-level security via Postgres RLS.** DuckDB
  currently pulls all rows a user "could" see, then DuckDB filters.
  Pushing V-Lake grants into Postgres RLS policies would prevent
  unauthorized rows from ever crossing the wire in the first place.
- **Connector-side field masking.** For HIPAA/GDPR columns we
  currently drop the column. A better UX is tokenization
  (`hash_sha256(ssn) AS ssn_token`) so analysts can still join on
  the identifier without seeing the value.
