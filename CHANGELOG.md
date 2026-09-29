# Changelog

All notable changes to tpt-streamforge are documented here. The format is based
on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project
adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] - Unreleased

> Everything below ships together in the first release. It is not tagged yet:
> per the release process in `AGENTS.md`, tagging `v0.1.0` is a deliberate
> manual step because it triggers the PyPI and npm publish workflow. Replace
> this heading with the release date when the tag is pushed.

### Added

- **Data contracts, keyed sampling, typed FFI handles** — `expect` gains
  `Range` / `OneOf` / `Type` checks (Rust `Check::range/one_of/of_type`; Python
  `expect(ranges=…, one_of=…, types=…)`); new deterministic `Pipeline::sample
  (fraction, &keys, seed)` stage (Python `.sample(fraction, key, seed=0)`); the C
  header now uses distinct opaque `TptPipeline *` / `TptRecordBatch *` handle
  types instead of `void *` (F24 follow-up). The WASM bindings do not expose
  `expect`, so nothing changed there.

- **`tptforge` authoring & ops tooling** — `${VAR}` / `${VAR:-default}`
  substitution in pipeline string values (F22; `$${` escapes a literal `${`;
  unset variables are an error naming the variable and line); pipeline errors now
  carry `line L, column C`, the stage index and a did-you-mean hint, and every
  spec struct rejects unknown fields; new `validate`, `explain` and
  `run --dry-run`; `schema-json` plus a checked-in, freshness-tested
  `pipeline.schema.json` for editor autocomplete; `completions <shell>` and `man`
  (`clap_complete` / `clap_mangen`, both MIT-compatible); `init`, `doctor`,
  `convert`, `run --watch`; `schema --save` / `--against` (exit 2 on drift);
  `diff A B --key id` (external sort + merge pass); `run --manifest FILE`
  provenance JSON (counts, per-stage stats, SHA-256 of spec and local
  inputs/outputs). Checkpoint/resume is deliberately not included.

- **Release & supply-chain tooling** — `release.yml` builds `tptforge` for Linux x64, macOS arm64/x64 and Windows x64, and publishes them with `SHA256SUMS`, a build-provenance attestation, a CycloneDX SBOM and `install.sh` / `install.ps1` (checksum-verified). `[package.metadata.binstall]` for `cargo binstall`, Scoop/Homebrew templates in `packaging/`, `cargo audit` and SBOM jobs in CI, `.devcontainer/`, and a cross-platform `just setup` (the justfile no longer hardcodes `.venv/Scripts`).

- **Per-row dead-letter queue** — `Pipeline::dead_letter(path)` captures rows a
  *stage* rejects to a CSV file instead of aborting the run, so a long job
  finishes with the bad rows preserved rather than lost. The offending row is
  isolated by binary-narrowing the failing batch (O(log n) stage calls per bad
  row), and surviving rows continue **in their original order**. Stateful stages
  (aggregate, sort, dedup, join, expect) fail with a clear config error under a
  dead-letter queue rather than silently mis-aggregating, since narrowing would
  re-run them over a subset of their input.
- **`tracing` instrumentation** — a new opt-in `tracing` feature (off by
  default; the default dependency set and the wasm sync-only build are
  unchanged). Install any `tracing` subscriber and the engine emits the same
  event stream as `on_progress`, plus HTTP-retry and dead-letter events, under
  filterable targets (`tpt_stream_core::pipeline`, `::httpclient`,
  `::dead_letter`).
- **Prometheus metrics endpoint** — `tptforge run --metrics 127.0.0.1:9464`
  serves `/metrics` in Prometheus text format during a run (rows, batches,
  per-stage rows in/out, dead-letter rows, running gauge). Off by default and
  dependency-free — just `std::net::TcpListener`. OpenTelemetry is deliberately
  not included: an OTLP exporter would add a dependency the project's
  minimal-tree policy avoids.
- **Retry/backoff for network sources & sinks** — S3/GCS/Azure/HTTP requests can
  retry transient failures (connection resets, 408, 429, 5xx) with exponential
  backoff and jitter. Opt-in per store (`S3Store::with_retry`); the default is
  no retry, so existing behavior is unchanged. Client errors still fail fast.
  The PostgreSQL sink's retry is now configurable the same way
  (`PostgresSink::with_retry`), classifying failures on SQLSTATE so a dropped
  connection, deadlock, or serialization failure retries while a constraint
  violation fails immediately. Its default remains the sink's original single
  immediate reconnect.
- **Parallel CSV ingestion** — whole-buffer CSV inputs of at least 1 MiB are
  split at record boundaries and parsed across the rayon pool, making large
  CSV reads ~4x faster (465 ms → 116 ms on a 400k-row × 16-column input; see
  `cargo bench -p tpt-stream-core --bench phase3 -- csv_ingest`). The new
  `tpt_csv::find_chunk_boundaries` pre-scan is quote-aware, so slicing never
  breaks a quoted field; ragged-row errors still report whole-document line
  numbers, and type inference is unchanged. Applies to the whole-buffer
  `csv_to_batches` / `read_csv_batches` paths; the bounded-memory streaming
  `CsvSource` stays sequential by design, as do `.gz` inputs.
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

### Security

- **Input-size caps (F1 follow-up, F14)** - gzip output is capped at 64 GiB
  (decompression bombs), and CSV records, JSONL lines and JSON-array elements at
  16 MiB, for file, HTTP and cloud sources; exceeding a cap is an error. Both are
  configurable through the new `SourceLimits` (`CsvSource::with_limits`,
  `JsonlSource::with_limits`, `JsonArraySource::open_with_limits`,
  `HttpSource::with_limits`). The HTTP client gained `Agent::with_max_body` and
  `HttpSource::with_max_body`. `tpt-csv` gained `ReaderBuilder::max_record_bytes`
  and `Error::RecordTooLarge`.
- **Azure/GCS/SQLite/TLS hardening (F18-F20, F10)** - Azure blob keys are
  percent-encoded in the URL and the signed resource (keys with `?`, `#`, `%`,
  spaces or CR/LF no longer corrupt or split requests; `.`/`..` segments and NUL
  are rejected); GCS bucket names are validated against the naming rules; the
  SQLite source runs with `PRAGMA query_only=ON` and NUL in queries or
  identifiers is rejected; TLS root-store loading reports native-certificate
  errors, fails with an actionable message when no roots are available, and
  accepts an extra CA bundle (`TPT_EXTRA_CA_BUNDLE` or `Agent::with_ca_bundle`).
  `deny.toml` advisory ignores were re-verified (the vulnerable
  `rustls-webpki 0.102.8` only supplies algorithm-id constants; certificate
  validation uses 0.103.x) and now carry review-by dates.
- **PostgreSQL plaintext refusal (F2)** - the PostgreSQL source and sink connect
  without TLS, so they now refuse non-loopback TCP hosts unless
  `TPT_ALLOW_INSECURE_POSTGRES=1` is set (e.g. when tunnelling). Native TLS
  support is not included.

### Fixed

- **CI/release hardening (F11/F12)** — top-level `permissions: contents: read`; `NPM_TOKEN` scoped to the publish steps with `npm publish --provenance`; `--locked` builds; wasm-pack built from source instead of `curl | sh`; the container image runs as a non-root user and builds with `--locked`. Action SHA pinning and image digests remain TODO (marked in the files).

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
- **`pyo3` bumped 0.23 → 0.29** (`tpt-stream-py`) to clear two RustSec
  advisories (buffer overflow in `PyString::from_object`, missing `Sync`
  bound on `PyCFunction::new_closure`); migrated `PyObject` → `Py<PyAny>`,
  `Python::with_gil` → `Python::attach`, `Python::allow_threads` →
  `Python::detach` throughout the bindings.
- **`rusty-s3` bumped 0.7 → 0.10** (`tpt-stream-core`, `s3`/`gcs` features)
  to clear two `quick-xml` denial-of-service advisories; the crate now
  parses S3 XML responses with `instant-xml` instead, which is stricter
  about the S3 response namespace (`CreateMultipartUpload::parse_response`
  call site updated accordingly).
- `deny.toml` ignores three `rustls-webpki`/`rsa` advisories and one
  `paste` unmaintained advisory, all pinned transitively by
  `rustls-rustcrypto` 0.0.2-alpha (still its only release, no fix
  available) and none reachable through our client-only TLS usage — see
  the comments in `deny.toml` for the per-advisory rationale.

### Added
- **`SELECT DISTINCT` in the SQL frontend** — `tptforge sql` now supports
  `DISTINCT`, deduping on the *projected* columns, so
  `SELECT DISTINCT region, product` keeps two `north` rows that differ in
  `product`, while `SELECT DISTINCT *` compares whole rows. It runs after
  `WHERE` and before `ORDER BY`/`LIMIT`, and is skipped when `GROUP BY` is
  present (which already collapses the keys). The Postgres-style
  `DISTINCT ON (...)` is still rejected.
- **Python `limit` and the stage-level dead-letter queue** —
  `Pipeline.limit(n)`, `Pipeline.dead_letter(path)`, and
  `Pipeline.dead_letter_rows()`. The queue is deliberately rejected with stateful
  stages, because isolating a bad row would re-run them over a subset of their
  input.
- **Python `with_retry(attempts, base_delay_ms, max_delay_ms)`** — opt-in
  retry/backoff for S3, GCS, Azure, and PostgreSQL. It is *one-shot*: consumed by
  the next network `read_*`/`write_*`, so a policy cannot silently leak onto a
  later source. Only transient failures retry (transport errors, 408/429/5xx);
  the default is still no retry.
- **PEP 561 typing for the Python package** — a hand-written
  `tpt_streamforge/_native.pyi` and a `py.typed` marker, both shipped in the
  sdist and the wheel, plus `[project.urls]` metadata and a `pandas` extra.
  Editors and `mypy` now see the real API instead of an untyped extension
  module.
- **Pre-built-store pipeline methods** — `read_s3_store`, `write_s3_store`,
  `read_gcs_store`, `write_gcs_store`, and `write_postgres_sink` take an already
  configured store/sink, so a caller can apply options the shorthand has no
  argument for (`with_retry`, and for Postgres `overwrite()`). `GcsStore` also
  gained the `with_retry` it was missing.
- **Fallible columnar APIs** — `try_push`, `try_set`, `try_append_column`,
  `RecordBatch::try_append_rows`, and the `ColumnBuffer` equivalents return a
  `TypeMismatch` (or `table::AppendError`) instead of panicking, for callers
  whose types are not statically known — notably the untrusted `.tptcol` reader.
  The original names remain as thin documented wrappers, because a type mismatch
  there is a bug in the calling code rather than bad input.

### Fixed
- **Public-API `panic!`s on untrusted or untyped input** — the `.tptcol`
  decoder and the columnar batch append path now report a mismatch as an error
  instead of aborting the process, and the aggregate COUNT accumulator reports
  an inconsistent variant as `Error::Other` naming what it found.
- **Two CLI tests used TOML that could never parse** — `[source] csv = "..."`
  only works for *sinks* (which accept a scalar path shorthand); a source needs
  `[source.csv] path = "..."`.
- **`spec.txt` described an API that does not exist** — the `csv` crate (now
  in-house `tpt-csv`), a zero-copy FFI that was never built (`to_arrow` and the
  11 `arrow` crates were removed for the licensing policy), configurable CSV
  delimiters (fixed at `,`), 64-row/64 MB chunks (65,536 rows), a `Row`-mutating
  `map` closure, and both the Python and JS "target API" snippets. The file is
  now marked Implemented and defers to the per-crate READMEs.
- **`count_all` output-column naming was documented vaguely and inconsistently.**
  It genuinely differs per binding: Python names the output column after the
  `agg()` dict key, WASM always emits `count_all`, and the CLI emits
  `count_all` for the `"*"` key or `count_<col>` for a column key. All four
  READMEs, both wasm `.d.ts` files, and the `aggregate` rustdoc now state the
  exact rule and the difference.
- **The Prometheus metrics endpoint is hardened (F13)** — request line capped at
  8 KiB, headers at 100 lines / 16 KiB (over-limit requests get a `431`, not an
  unbounded buffer), a 5 s per-connection deadline with 2 s read/write timeouts,
  and **loopback-only by default**: `--metrics 0.0.0.0:9464` is now refused
  unless you also pass `--metrics-allow-remote`, because the endpoint is
  unauthenticated and would otherwise publish a job's row counts to the network.
  Fixing this surfaced a genuine Windows bug — closing a socket that still has
  unread request bytes queued sends an RST, which makes the peer discard a
  response it already received — so reject paths now drain the remainder
  (bounded) and the FIN is sent explicitly. That was also the cause of two
  flaky tests; they are now 10/10 clean.
- **The FFI C integration test is no longer excluded on Windows CI** —
  `rustc` emits a `cdylib` but no import library, so the test synthesizes one
  with `dlltool` (present in the `windows-latest` image) and links
  `-ltpt_stream_ffi`. A missing compiler or `dlltool` prints
  `SKIPPED <reason>` and passes, so a thin toolchain degrades to a documented
  no-op instead of a red build.
- **Repository hygiene** — removed a committed test log, a one-off patch script,
  and a personal absolute path from `todo.md`. `tpt-stream-cli/examples/pipeline.toml`
  is now explicitly a feature tour (it adds the `limit` stage) rather than a
  near-duplicate of `templates/pipeline-starter/pipeline.toml`, with a test
  guarding both against drift.

[0.1.0]: https://github.com/tpt-solutions/tpt-streamforge/releases/tag/v0.1.0
