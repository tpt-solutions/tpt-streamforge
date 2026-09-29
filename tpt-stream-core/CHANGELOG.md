# Changelog — tpt-stream-core

All notable changes to the engine crate. The workspace-wide history lives in
the [root CHANGELOG](../CHANGELOG.md).

## [Unreleased]

### Added
- **Data contracts** — `Check::Range { column, min, max }`, `Check::OneOf
  { column, allowed }` and `Check::Type { column, expected }` in `expect.rs`
  (constructors `Check::range` / `Check::one_of` / `Check::of_type`). Nulls are
  skipped by `Range`/`OneOf` (pair with `NoNulls`); NaN fails `Range`. `Check`
  no longer derives `Eq` (it now holds `f64`/`Value`).
- **Deterministic keyed sampling** — `Pipeline::sample(fraction, &key_columns,
  seed)` / `sample::Sample`: keeps a row when a stable hash (FNV-1a +
  SplitMix64, via the shared `agg::hash_values`) of `(seed, key)` is below
  `fraction`. Reproducible, independent of batching, key-consistent, stateless.
- **Per-row dead-letter queue** — `Pipeline::dead_letter(path)` captures rows a
  *stage* rejects (a `map` returning the wrong width for one row, a failing
  expression) to a CSV file instead of aborting the run, so a long job finishes
  and the rejected rows are preserved. The offending row is isolated by
  re-running the stage over progressively smaller halves (O(log n) stage calls
  per bad row); surviving rows continue through the pipeline **in their original
  order**. The file has `_dead_letter_stage` and `_error` columns ahead of the
  row's own fields, and is written lazily, so a clean run leaves it empty.
  This is the stage-level counterpart to `ErrorPolicy::Quarantine`, which
  handles malformed *input* rows; the two are independent.
  Stateful stages (`GroupByAgg`, `Sort`, `Deduplicate`, `HashJoin`, `expect`)
  return a clear `Error::Config` if they fail under a dead-letter queue rather
  than silently re-aggregating a subset of their input.
- **`tracing` instrumentation** behind the new opt-in `tracing` feature (off by
  default, so the dependency set and the wasm sync-only build are unchanged).
  A `tracing` subscriber sees the same event stream as `on_progress`, plus
  HTTP retry and dead-letter events, under filterable targets
  (`tpt_stream_core::pipeline`, `::httpclient`, `::dead_letter`).
- **Retry/backoff for network sources & sinks** — `httpclient::RetryPolicy`
  (attempts, base delay, cap) with exponential backoff and jitter, for the
  S3/GCS/Azure/HTTP request path. Only transient failures retry (transport
  errors, 408, 429, 5xx); a 4xx still fails fast. Opt-in via
  `S3Store::with_retry` / `AzureBlobStore::with_retry`; the default is no retry,
  so existing behavior is unchanged. `PostgresSink::with_retry` does the same
  for `COPY ... FROM STDIN`, classifying on SQLSTATE so a constraint violation
  fails immediately while a dropped connection, deadlock, or serialization
  failure retries. Its default is one immediate reconnect — the sink's previous
  behavior — so nothing changes unless asked.
- **Parallel whole-buffer CSV ingestion** — CSV inputs of at least
  `source::PARALLEL_CSV_MIN_BYTES` (1 MiB) are split at record boundaries with
  `tpt_csv::find_chunk_boundaries` and parsed across the rayon pool, then
  assembled into batches in slice order. Applies to `csv_to_batches` and
  `read_csv_batches` (the join build-relation reader); the bounded-memory
  streaming `CsvSource` stays sequential by design, and `.gz` inputs stay on
  the streaming decompression path. Measured **~4.0x faster** ingestion
  (465 ms → 116 ms) on a 400k-row x 16-column input
  (`cargo bench -p tpt-stream-core --bench phase3 -- csv_ingest`).
  Slicing is transparent: identical batches, identical type inference, and
  ragged-row errors still report whole-document line numbers.
- `source::csv_to_batches_sequential` — parse in-memory CSV on the calling
  thread, bypassing the threshold (used by the bench, and an escape hatch for
  callers that must stay single-threaded).
- `date` (`YYYY-MM-DD` → days since epoch) and `timestamp` (ISO 8601,
  microseconds since epoch) data types, inferred from CSV/JSONL input,
  sortable, comparable against ISO strings in the expression language,
  round-tripped through `.tptcol`, SQLite, PostgreSQL, JSON, and the WASM /
  Python surfaces.
- `Pipeline::limit(n)` stage (used by the SQL `LIMIT` lowering).
- `Pipeline::on_error` error policies: `strict`, `skip`, `quarantine:<path>`.
- `Pipeline::expect_checks` data-quality stage and `Error::DataQuality`.
- `Pipeline::explain()`, `preview(n)`, `collect()`.
- gzip source support and plain HTTP(S) URL sources.
- Telemetry: stage names in `StageMetrics`, sink-row totals, tail-drain
  accounting.

- **Input-size caps** - `SourceLimits` (record/line/element size, default
  16 MiB; gzip decompressed output, default 64 GiB) applied by `CsvSource`,
  `JsonlSource`, `JsonArraySource`, `HttpSource` and the cloud sources;
  `with_limits` / `JsonArraySource::open_with_limits`, `HttpSource::with_max_body`,
  `httpclient::Agent::with_max_body`.
- TLS roots: `httpclient::Agent::with_ca_bundle` and `TPT_EXTRA_CA_BUNDLE`;
  native-certificate errors are logged (`tracing`) and an empty root store is a
  clear error instead of an opaque handshake failure.

### Security
- Azure blob keys are percent-encoded (and `.`/`..`/NUL rejected); GCS bucket
  names are validated; SQLite source uses `PRAGMA query_only=ON` and rejects NUL
  in queries/identifiers; PostgreSQL refuses plaintext connections to
  non-loopback hosts unless `TPT_ALLOW_INSECURE_POSTGRES=1`.

### Fixed
- Ragged CSV rows (wrong field count) aborted with a line-numbered schema
  error instead of silently shifting column packing and corrupting data.
- Re-running an executed pipeline errors instead of silently yielding 0 rows.
- `SinkBatch.total_rows` counts rows written to the sink.

### Changed
- `rusty-s3` bumped 0.7 → 0.10 (`s3`/`gcs` features) to clear two
  `quick-xml` denial-of-service RustSec advisories; it now parses S3 XML
  responses with `instant-xml`, which enforces the S3 response namespace
  more strictly than `quick-xml` did.

## [0.1.0] - 2026

Initial engine: streaming CSV/JSONL/JSON + `.tptcol` I/O, filter/map/select,
external merge sort, hash aggregation, hash join, bloom dedup, expression
language, SQLite/PostgreSQL/S3/GCS/Azure sources and sinks, telemetry hooks.
