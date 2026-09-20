# Changelog

All notable changes to tpt-streamforge are documented here. The format is based
on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project
adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- **SQLite sink/source** (`sqlite` feature, `tpt-stream-core`) — `SqliteSink`
  writes batches into a SQLite table (auto-creates schema from the first batch,
  batches inserts in a transaction, `overwrite()` mode drops the table first,
  checkpoint on finish). `SqliteSource` streams a SELECT query in chunks via a
  dedicated reader thread using `column_decltype` for type inference. Wired into
  `Pipeline::read_sqlite` / `write_sqlite`.
- **JS / WASM bindings (Phase 5)** — a wasm-bindgen crate (`tpt-stream-wasm`)
  exposing an in-memory `Engine` (from_csv, filter, map, sort, dedup,
  aggregate, to_csv, to_json_text):
  - `tpt-streamforge-node` — CJS fluent `Pipeline` wrapper for Node 18+,
    with file I/O (`readFile`/`writeFile`).
  - `tpt-streamforge-browser` — ESM wrapper for browsers, with
    `fromArrayBuffer`/`fromFile`/`toArrayBuffer` helpers.
  - Both packages bundle a ~250 KB wasm binary (well under the 10 MB target).
- **Sync-only core build (no tokio)** — `tpt-stream-core` now gates tokio,
  rayon, and async-trait behind an `async` feature (enabled by default), so the
  crate compiles for `wasm32-unknown-unknown` with `default-features = false`.
  `Error`/`Result`/`PipelineStats` moved to `tpt-stream-core::error`.
- **Webpack-based browser test harness** — `tpt-stream-wasm/browser/tests`
  bundles the ESM wrapper plus the wasm module through the real bundler path
  (wasm-bindgen + webpack) and runs 10 assertions in Node.
- **CI/release wiring**
  - `ci.yml`: JS job builds both wasm targets and runs the Node + browser
    suites.
  - `release.yml`: `quality` gate (cargo-deny) and an `npm` job that builds,
    tests, and publishes both packages on `v*` tags.
- **Expression engine fix** — `and`/`or` now use Kleene three-valued logic;
  previously `true OR <null>` incorrectly evaluated to null.
- **Python `map` docs** — clarified that `.map()` replaces the row schema with
  exactly the mapped columns.
- **PostgreSQL sink/source** (`postgres` feature, `tpt-stream-core`) —
  `PostgresSink` bulk-loads every batch with the `COPY ... FROM STDIN`
  protocol (text format, one round trip per batch) and auto-creates the table
  from the first batch's schema (`overwrite()` drops it first).
  `PostgresSource` streams a SELECT query in chunks from a dedicated reader
  thread, mapping declared column types to primitives and falling back to
  text for everything else. Wired into `Pipeline::read_postgres` /
  `write_postgres`. Integration tests run against a Postgres service
  container in CI (skipped locally unless `TPT_TEST_POSTGRES_URL` is set).
- **Cloud object storage sources/sinks** — S3 (`s3` feature), Google Cloud
  Storage via the S3-compatible XML API + HMAC keys (`gcs`), and Azure Blob
  via Shared Key signing (`azure`). Built on `rusty-s3` (sans-IO SigV4) and
  an in-house minimal HTTP/1.1-over-TLS client (`httpclient` module, on
  `rustls` + `rustls-rustcrypto`) for S3/GCS/HTTP, and `hmac`/`sha2`/`base64`
  for Azure — no cloud SDK dependency. Formats follow the key extension (`.csv`,
  `.jsonl`/`.ndjson`, `.json`, `.tptcol`); sinks stream multipart uploads /
  staged blocks with a configurable part size and abort/keep consistent state
  on failure. Wired into `Pipeline::read_s3` / `write_s3` / `read_gcs` /
  `write_gcs` / `read_azure_blob` / `write_azure_blob`. Offline integration
  tests use an in-process mock HTTP server; CI adds a LocalStack + Azurite
  job.
- **Telemetry hooks** — `TelemetryEvent` stream (source batch, stage batch,
  sink batch, done) plus cumulative `StageMetrics` via `Pipeline::on_progress`
  and `Pipeline::stage_stats`. Exposed in Python (`Pipeline.on_progress(cb)`,
  `Pipeline.stage_stats()`) and JavaScript (`Pipeline.onProgress(cb)`) with
  tests in all three languages.
- **Criterion benchmark suite for all pipeline stages** — `benches/stages.rs`
  covers CSV source, filter, map, select, aggregate, sort, dedup, hash join,
  and CSV→CSV on a shared 1M-row fixture; `scripts/compare_pandas_duckdb.py`
  runs the same operations under pandas and DuckDB for the README comparison.
- **Date/timestamp data types** — ISO `date` (`YYYY-MM-DD`) and `timestamp`
  (ISO 8601, microsecond precision) columns are inferred from CSV/JSONL
  input, stored as day/microsecond integers in `.tptcol` (type codes 6/7),
  sortable and comparable against ISO string literals in the expression
  language (`day >= '2024-01-01'`), serialized as ISO strings in JSON and
  over the FFI, mapped to `DATE`/`TIMESTAMP` columns in PostgreSQL, and
  round-tripped through SQLite and the WASM/Python surfaces.
- **`tptforge sql`** — a SQL frontend over the pipeline engine: single-table
  `SELECT` with `WHERE`, `GROUP BY` + `SUM`/`AVG`/`COUNT`/`MIN`/`MAX`,
  aliases, `ORDER BY`, and `LIMIT`, lowered onto filter/aggregate/sort/
  limit stages; unsupported SQL fails explicitly. Backed by the new
  `Pipeline::limit` stage and a small in-house recursive-descent parser
  (no `sqlparser` dependency, to keep the tree free of Apache-2.0-only
  crates).
- **Browser playground** — an interactive in-browser pipeline builder at
  `tpt-stream-wasm/browser/playground/` (filter/map/sort/dedup/aggregate
  stages over pasted CSV, all client-side WebAssembly); built with
  `npm run build:playground`.
- **Per-crate documentation** — every crate now ships its own README
  (crates.io `readme` metadata), `CHANGELOG.md`, and
  crates.io `categories`/`keywords` metadata; npm packages carry keywords.
- **`Pipeline::with_chunk_size` now applies to file sources** — `read_csv` /
  `read_jsonl` / `read_json` previously ignored the configured chunk size and
  always used the 65,536-row default.
- **`tptforge` CLI** (`tpt-stream-cli`) — `tptforge run pipeline.yaml`
  executes streaming pipelines from a YAML file (sources/sinks: CSV, JSONL,
  JSON, `.tptcol`, `.gz`, HTTP(S), SQLite, PostgreSQL, S3, GCS, Azure;
  stages: filter, map, select, aggregate, sort, dedup, join, expect) with a
  telemetry-driven progress bar, plus `tptforge schema FILE` and
  `tptforge preview FILE -n N` for data inspection. Ships with a Dockerfile
  and a CI image-build job.
- **Input tolerance & gzip** — `.csv.gz`/`.jsonl.gz` decompress transparently
  (`gzip` feature); plain HTTP(S) URLs stream as sources (`http` feature);
  `Pipeline::on_error` selects the malformed-row policy (`strict`, `skip`,
  or `quarantine:<path>` capturing dropped rows).
- **Data-quality checks** — `Pipeline::expect_checks` (and YAML `expect`
  stages) enforce row-count bounds, no-nulls, and stream-wide uniqueness;
  violations abort with `Error::DataQuality`.
- **`Pipeline::explain()` / `preview(n)` / `collect()`** — stage plan,
  first-rows inspection, and in-memory collection.
- **Python: full engine surface** — `.select`, `.join_csv`, `.read_jsonl`,
  `.read_json`, `.read_columnar`, `.write_jsonl`, `.write_json`,
  `.write_columnar`, `.read_sqlite`/`.write_sqlite`,
  `.read_postgres`/`.write_postgres`, `.read_s3`/`.write_s3`,
  `.read_gcs`/`.write_gcs`, `.read_azure`/`.write_azure`, `.read_http`,
  `.on_error`, `.expect`, `.explain`, `.preview`, `.collect`,
  `.to_pandas()` (via `collect()` + `pandas.DataFrame`, no Arrow
  dependency), plus `py.allow_threads` around runs so other Python threads
  keep moving.
- **Node: JSON input** — `readJsonFile(path)` / `readJsonLinesFile(path)`.
- **Runnable examples** — csv_to_jsonl, join_two_files, dedup_sort_pipeline,
  telemetry_progress, and an env-gated s3_to_postgres cloud ETL walkthrough;
  plus a copy-paste `templates/pipeline-starter` and a root `justfile`.
- **CI** — Dependabot config, a Docker image build job, and publish jobs
  moved into a protected `release` GitHub Environment.

### Fixed

- **Ragged CSV rows no longer corrupt data silently** — a row with the wrong
  field count previously shifted the column arena, mangling every following
  row and dropping data. It now fails with a line-numbered schema error
  (or is dropped/quarantined under the new error policies).
- `Pipeline::execute()` on an already-run pipeline now errors clearly instead
  of silently returning 0 rows (sources are one-shot).
- Telemetry: `SinkBatch.total_rows` counts rows written (was source rows),
  tail-drained rows (aggregation/sort/join output) now count toward
  `stage_stats`, and `collect()`/`to_pandas()` include tail rows.
- Python `execute()` releases the GIL while the pipeline streams.
- PostgreSQL sink retries once on connection loss and shuts down its
  connection driver task on finish.
- S3 sink clamps part sizes to the 5 GiB single-PUT ceiling.

### Changed

- **Dependency license policy: no Apache-2.0-only crates.** The project is
  dual MIT/Apache-2.0 so a consumer who wants MIT terms alone always has a
  legal path to the whole dependency tree; that broke wherever a dependency
  was offered under Apache-2.0 only (no MIT option). Removed: `arrow`/
  `arrow-*` (Python `to_arrow()`/`to_pandas()` now goes through `collect()`
  + `pandas.DataFrame`, no Arrow dependency), `sqlparser` (`tptforge sql`
  now uses a small in-house recursive-descent parser for its existing
  supported subset), and `ring` (dropped `ureq` entirely — its `rustls`
  dependency couldn't be steered off `ring` via Cargo feature unification —
  in favor of an in-house minimal HTTP/1.1-over-TLS client on `rustls` +
  the `rustls-rustcrypto` provider, pure Rust, MIT/Apache-2.0 dual). Note:
  `rustls-rustcrypto` is v0.0.2-alpha and far less battle-tested than
  `ring` for TLS; accepted as a deliberate trade-off of license purity
  against crypto-backend maturity. `ryu` (via `csv`/`serde_yaml`) and
  `target-lexicon` (build-time only, forced by `pyo3-build-config`) remain
  under investigation.
- `deny.toml` modernized for cargo-deny ≥ 0.18: allowlist-only licensing
  (any license not allowed is denied, keeping copyleft families out), the
  new `Unicode-3.0` (ICU crates via `url`) and `CDLA-Permissive-2.0`
  (Mozilla root store data via `ureq`/`rustls`) allowances, an MPL-2.0
  exception scoped to the build-time `cbindgen` header generator, and
  explicit versions on the workspace path dependencies (satisfies
  `bans.wildcards = "deny"`).

## [0.1.0] - placeholder

Initial engine work: streaming CSV/JSONL pipelines, filter/map/sort/dedup/
aggregate/join, C FFI, and the PyO3 Python package.

[unreleased]: https://github.com/anomalyco/tpt-streamforge