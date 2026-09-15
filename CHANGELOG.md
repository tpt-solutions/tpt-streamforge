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
  `ureq` with rustls for S3/GCS, and `hmac`/`sha2`/`base64` for Azure — no
  cloud SDK dependency. Formats follow the key extension (`.csv`,
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
- **`Pipeline::with_chunk_size` now applies to file sources** — `read_csv` /
  `read_jsonl` / `read_json` previously ignored the configured chunk size and
  always used the 65,536-row default.

### Changed

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