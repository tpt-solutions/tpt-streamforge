# tpt-streamforge — Master Task Checklist
Dual-licensed MIT / Apache-2.0 | TPT Solutions

---

## Phase 1: Core Streaming Engine

### Setup
- [x] Initialize Cargo workspace (`Cargo.toml` with 5 member crates)
- [x] Add `deny.toml` with license allowlist/denylist
- [x] Add `LICENSE-MIT` and `LICENSE-APACHE`
- [x] Set up GitHub Actions CI: `cargo build`, `cargo test`, `cargo deny check`
- [x] Verify initial dep tree passes `cargo deny check licenses`

### tpt-stream-core: Types
- [x] Define `DataType` enum (Int32, Int64, Float32, Float64, Utf8, Bool)
- [x] Define `Value` enum (typed runtime value)
- [x] Define `Column` (DataType + buffer)
- [x] Define `RecordBatch` (schema + Vec<Column> + row count)
- [x] Implement fixed row-count chunking (default 65,536 rows/batch)

### tpt-stream-core: Pipeline
- [x] Define `PipelineStage` trait (process RecordBatch → RecordBatch)
- [x] Define `Pipeline` struct (ordered Vec of stages)
- [x] Implement `Pipeline::execute()` with async tokio I/O

### tpt-stream-core: Sources & Sinks
- [x] Implement streaming CSV source (wraps `csv` crate)
- [x] Implement streaming CSV sink
- [x] Implement streaming JSONL source (`serde_json` streaming deserializer)
- [x] Implement streaming JSONL sink
- [x] Implement streaming JSON array source/sink

### tpt-stream-core: Transformations
- [x] Implement `Filter` stage (predicate expression on a Row)
- [x] Implement `Map` stage (row-level closure / expression)
- [x] Implement `Select` stage (column projection — keep/drop columns)

### tpt-stream-core: Tests & Benchmarks
- [x] Unit tests for all core types
- [x] Unit tests for Filter, Map, Select
- [x] Integration test: CSV → filter → map → CSV round-trip
- [x] Integration test: JSONL → filter → map → JSONL round-trip
- [x] Benchmark: CSV throughput (target > 5M rows/sec)
- [x] Confirm binary size < 15 MB

---

## Phase 2: Custom Columnar Format (tpt-stream-columnar)

### Format Design
- [x] Document format layout: magic bytes, version header, schema section, column buffers
- [x] Finalize primitive type encodings (Int32, Int64, Float32, Float64, Utf8, Bool)

### Implementation
- [x] Implement typed column buffer (Vec<T> per DataType variant)
- [x] Implement `RecordBatch → columnar` serialization
- [x] Implement `columnar → RecordBatch` deserialization
- [x] Implement fixed-size chunked writer (streaming, constant memory)
- [x] Implement zstd compression for column buffers (feature-gated: `zstd`)
- [x] Implement disk write: `.tptcol` file format
- [x] Implement disk read: `.tptcol` file parser

### Integration with Core
- [x] Add `.tptcol` as a source in `tpt-stream-core`
- [x] Add `.tptcol` as a sink in `tpt-stream-core`

### Tests & Docs
- [x] Unit tests: round-trip for each primitive type
- [x] Integration test: CSV → columnar → CSV
- [x] Integration test: columnar with zstd compression round-trip
- [x] Write format spec in `tpt-stream-columnar/README.md`

---

## Phase 3: Advanced Transformations

### Aggregation
- [x] Implement hash-based GROUP BY accumulator
- [x] Implement streaming flush when accumulator hits memory limit
- [x] Implement aggregation functions: SUM, AVG, COUNT, MIN, MAX
- [x] Expose `GroupBy` + `Agg` pipeline stages

### External Merge Sort
- [x] Implement in-memory sort phase (per RecordBatch)
- [x] Implement spill-to-disk: write sorted runs as `.tptcol` files
- [x] Implement k-way merge phase over sorted runs
- [x] Expose `Sort` pipeline stage

### Streaming Hash Join
- [x] Implement build phase: hash table from smaller (right) relation
- [x] Implement probe phase: stream larger (left) relation
- [x] Implement spill-to-disk for oversized hash tables
- [x] Expose `Join` pipeline stage

### Deduplication
- [x] Implement bloom filter for fast probabilistic dedup
- [x] Implement exact dedup fallback via sort-based approach
- [x] Expose `Deduplicate` pipeline stage

### Tests & Benchmarks
- [x] Integration tests for aggregation, sort, join, dedup
- [x] Benchmark: aggregation on 10M-row dataset

---

## Phase 4: FFI & Python Wrapper

### tpt-stream-ffi (C ABI)
- [x] Define opaque handle types: `PipelineHandle`, `RecordBatchHandle`
- [x] Define error code enum and `tpt_error_string()` API
- [x] Export C functions: `pipeline_new`, `pipeline_read_csv`, `pipeline_filter`, `pipeline_map`, `pipeline_write_csv`, `pipeline_execute`, `pipeline_free`
- [x] Export C functions: `record_batch_num_rows`, `record_batch_get_column`, `record_batch_free`
- [x] Build as `cdylib`
- [x] Generate C header with `cbindgen`
- [x] Write C integration test

### tpt-stream-py (PyO3)
- [x] Set up `maturin` build
- [x] Implement Python class `Pipeline` (wraps Rust Pipeline)
- [x] Expose: `read_csv(path, chunk_size=65536)`
- [x] Expose: `filter(expr: str)`
- [x] Expose: `map(expr: str)`
- [x] Expose: `group_by(col).agg({col: fn_name})`
- [x] Expose: `write_csv(path)`
- [x] Expose: `execute()`
- [x] Write Python test suite (pytest)
- [x] GitHub Actions: build wheels for Linux/macOS/Windows (maturin)
- [x] GitHub Actions: publish to PyPI on release tag
- [x] Write Python README and API docs

---

## Phase 5: JavaScript / WASM Wrapper (tpt-stream-wasm)

> Design note: the WASM engine is an in-memory, file-less engine (`Engine` holding
> `Vec<RecordBatch>`). `tpt-stream-core` gained a sync-only build path
> (`--no-default-features`), so wasm32 builds exclude tokio/rayon/async-trait.

### Build Setup
- [x] Set up `wasm-bindgen` + `wasm-pack`
- [x] Configure two build targets: `bundler` (browser) and `nodejs`
- [x] WASM binary size < 10 MB (≈250 KB)

### Node.js Target
- [x] Implement file I/O adapter via Node.js `fs` APIs (`readFile`/`writeFile`)
- [x] Expose API: `readCSV`, `filter`, `map`, `writeFile`, `execute` (sync/in-memory engine)
- [x] Write Node.js test suite (node:test)
- [x] GitHub Actions: build Node.js WASM package
- [x] Publish to npm as `tpt-streamforge-node` (release workflow `npm` job)

### Browser Target
- [x] Implement data input via `ArrayBuffer` (`fromArrayBuffer`, `fromFile`)
- [x] Expose API: `fromArrayBuffer`, `filter`, `map`, `toArrayBuffer`
- [x] Write browser test suite (webpack bundler-path harness, 10 assertions)
- [x] GitHub Actions: build browser WASM package
- [x] Publish to npm as `tpt-streamforge-browser` (release workflow `npm` job)

### Docs
- [x] Write JS/TS README and API reference (`node/README.md`, `browser/README.md`, `index.d.ts` for both)

---

## Phase 6: Advanced Features

### Database Sinks & Sources
- [x] SQLite sink via `rusqlite` (bulk insert, batched transactions, `overwrite()` mode)
- [x] SQLite source (SELECT query → RecordBatch stream via declared types + inference)
- [x] PostgreSQL sink via `tokio-postgres` (bulk COPY protocol)
- [x] PostgreSQL source (query → RecordBatch stream)
- [x] Add `tokio-postgres` to `deny.toml` approved list
- [x] Integration tests: PostgreSQL sink/source (Docker Postgres in CI)

### Cloud Storage
- [x] Research minimal S3 crate (MIT/Apache-2.0, minimal transitive deps)
- [x] Implement S3 source (streaming GET via `rustls`)
- [x] Implement S3 sink (streaming PUT / multipart upload)
- [x] Implement GCS source/sink (if a minimal crate is available)
- [x] Implement Azure Blob source/sink (if a minimal crate is available)
- [x] Integration tests for S3 (LocalStack in CI)

### Telemetry & Profiling
- [x] Define telemetry hook interface (rows, bytes, elapsed per stage)
- [x] Implement pipeline progress callback API
- [x] Expose telemetry in Python wrapper
- [x] Expose telemetry in JS wrapper
- [x] Add `criterion` benchmark suite for all pipeline stages
- [x] Publish benchmark results in project README

---

## Documentation & Release

- [x] Write project `README.md` (what, why, quick start for all three languages)
- [x] Write `CONTRIBUTING.md`
- [x] Write `CHANGELOG.md`
- [x] Create GitHub release workflow: tag → build → publish wheels + npm packages
- [x] Add `cargo deny check` to release workflow
- [x] Set up `docs.rs` metadata for `tpt-stream-core`
- [x] Write performance comparison vs Pandas and DuckDB in README
- [ ] Tag v0.1.0 release

---

## Phase 7: Hardening & Bug Fixes

- [x] CSV source: reject ragged rows with a line-numbered schema error
      (a short row previously shifted column packing and corrupted
      subsequent rows)
- [x] `Pipeline::execute()` errors when the source is exhausted instead of
      silently returning `rows: 0` on a second run
- [x] Release the GIL during Python `execute()` so other Python threads can
      run while a pipeline streams
- [x] Telemetry: `SinkBatch.total_rows` counts rows written to the sink
      (was source rows); tail-drained rows also count toward `stage_stats`
- [x] Postgres sink: retry once on connection loss; abort the connection
      driver task on finish instead of leaking it
- [x] S3 sink: clamp `with_part_size` to the 5 GiB single-PUT ceiling
- [x] Pin CSV numeric type inference (Int32 → Int64 → Float64) with tests

## Phase 8: Engine Features

- [x] gzip-compressed CSV/JSONL/JSON sources (`.gz`, feature `gzip`, flate2)
- [x] HTTP(S) source for plain URLs (feature `http`, reuses ureq)
- [x] Error quarantine: source-level `on_error` policy
      (`strict` | `skip` | `quarantine(path)`) for malformed input rows
- [x] Data-quality `Expect` stage: row-count bounds, no-nulls, unique keys
- [x] `Pipeline::explain()` (stage plan) and `Pipeline::preview(n)` (first
      rows without running the whole pipeline); exposed in Python
- [x] Date/timestamp `DataType` variants end to end (sources, sinks, expr)

## Phase 9: Language Bindings & Interop

- [x] Python: expose `select` and `join` (CSV build side, inner/left/right)
- [x] Python: JSONL/JSON array read/write + columnar read/write
- [x] Python: SQLite and PostgreSQL read/write bindings
- [x] Python: S3/GCS/Azure read/write bindings
- [x] Python: `to_arrow()` / `to_pandas()` via arrow-rs pyarrow FFI
      (+ `on_error`, `expect`, `explain`, `preview`, `collect`)
- [x] Node: JSONL/JSON file I/O helpers (`readJsonFile`/`readJsonLinesFile`)

## Phase 10: CLI, Automation & Adoption

- [x] `tpt-stream-cli` crate: `tptforge run pipeline.yaml` (YAML pipelines)
- [x] CLI: `tptforge schema FILE` and `tptforge preview FILE -n N`
- [x] CLI: progress bar wired to telemetry (indicatif), `--quiet`
- [x] CLI: end-to-end tests (CSV → filter → aggregate → CSV via YAML)
- [x] Dockerfile (multi-stage) + CI build job for the CLI image
- [x] Project template for new pipelines (templates/pipeline-starter)
- [x] Runnable examples with tiny datasets: csv_to_jsonl, join_two_files,
      dedup_sort_pipeline, telemetry_progress, s3_to_postgres (env-gated)
- [x] `justfile`: test / bench / fmt / clippy / deny / py / wasm recipes
- [x] CI: dependabot config (cargo + npm + actions)
- [x] CI: put npm/PyPI publish jobs in a protected GitHub Environment
- [x] SQL frontend (SELECT/project/group-by subset via sqlparser)
- [x] WASM in-browser playground page

---

## Phase 11: Platform Review Follow-ups (2026-09-20)

### Dependency & License Policy

Audit findings (2026-09-20, `cargo deny list`): Apache-2.0-only (no MIT
option) crates in the tree were `arrow`/`arrow-*` (11 crates, tpt-stream-py
only), `ring` (via rustls, used for all HTTPS/cloud TLS), `sqlparser`
(CLI SQL frontend), `ryu` (transitive via `csv` + `serde_yaml`), and
`target-lexicon` (build-time only, forced by `pyo3-build-config`). Policy:
**zero exceptions in production** — every offender must be replaced, not
just documented as an approved exception (unlike the existing build-time-only
`cbindgen`/MPL-2.0 exception, which stays as-is since it never ships).

- [x] Drop Arrow interop (`to_arrow()`/`to_pandas()`) from `tpt-stream-py`;
      removes all 11 `arrow`/`arrow-*` crates cleanly (nothing else depends
      on them); `to_pandas()` kept, now via `collect()` + `pandas.DataFrame`
- [x] Hand-roll a minimal in-house CSV reader/writer in `tpt-stream-core` to
      replace the `csv`/`csv-core` crates (drops the `ryu` edge from CSV;
      float formatting via std `to_string()` instead of `ryu`) — landed as
      the standalone `tpt-csv` crate
- [x] Migrate CLI pipeline definitions from YAML (`serde_yaml`) to TOML
      (already permissively licensed, ryu-free) to drop the other `ryu` edge
- [x] Hand-roll a small recursive-descent SQL parser in `tpt-stream-cli` for
      the existing supported subset (SELECT/filter/group-by/sort/limit) to
      replace `sqlparser`; all 8 existing SQL end-to-end tests pass unchanged
- [x] Swap rustls's crypto provider from `ring` to `rustls-rustcrypto`
      (pure-Rust, MIT/Apache-2.0 dual RustCrypto backend) for all HTTPS/cloud
      TLS in `tpt-stream-core`. Required dropping `ureq` entirely (Cargo
      feature unification meant its default `ring`-enabled `rustls` couldn't
      be overridden from our side) and writing an in-house minimal
      HTTP/1.1-over-TLS client (`tpt-stream-core/src/httpclient.rs`,
      GET/PUT/POST/DELETE, streaming fixed-length and chunked bodies, plain
      HTTP for the Azurite emulator/mock test server). All 70 relevant tests
      pass (mock S3/Azure/HTTP round-trips, multipart upload, chunked
      streaming). Security trade-off: `rustls-rustcrypto` is v0.0.2-alpha,
      far less battle-tested than `ring` (BoringSSL-derived) — accepted
      per explicit user decision, noted in CHANGELOG.
- [x] Investigate whether `target-lexicon` (forced by `pyo3-build-config`,
      build-time only, never ships) can be avoided without dropping PyO3
      entirely; if not avoidable, escalate back to the user rather than
      silently accepting it as an exception
      **Finding**: cannot be avoided without dropping PyO3; added as a
      `[[licenses.exceptions]]` entry in `deny.toml` (build-time only,
      identical treatment to `cbindgen`/MPL-2.0).
- [x] Once all offenders are resolved, tighten `deny.toml`: remove bare
      `Apache-2.0` / `Apache-2.0 WITH LLVM-exception` from the allow list so
      only MIT (or MIT-paired dual licenses) satisfy the check, making this
      a CI-enforced gate going forward
- [x] Fix root `README.md` license section (says "MIT" only; project is
      dual MIT/Apache-2.0 per `LICENSE-APACHE` and AGENTS.md)

### Adoption & Onboarding
- [x] Add CI/crates.io/PyPI/npm/license badges to root `README.md`
- [x] Add runnable examples for `tpt-stream-py`, `tpt-stream-wasm`,
      `tpt-stream-cli` (mirroring `tpt-stream-core/examples`)
- [x] Host the built browser playground (`dist/`) as a live demo (e.g. GitHub
      Pages) and link it from the README
- [x] Publish a Docker image (e.g. GHCR) from `release.yml`; document
      `docker pull` instead of build-from-source only
- [x] Add `.github/ISSUE_TEMPLATE/` (bug report + feature request) and a PR
      template
- [x] Surface the Windows wasm test gotcha (`node --test tests/*.test.js`
      bare-directory failure) directly in `tpt-stream-wasm/README.md`

### Observability
- [x] Add `tracing` instrumentation to `tpt-stream-core` pipeline execution,
      sources, and sinks, feature-gated so wasm's sync/std-only build is
      unaffected
      **Implementation**: new optional `tracing` feature (off by default;
      `tracing` is MIT, so `cargo deny` stays green). A `trace_event!` macro in
      `telemetry/macros.rs` has two arms — it forwards to `tracing::event!` when
      the feature is on and expands to nothing when off, so call sites read
      identically either way and cost zero when disabled. Instrumented: every
      `TelemetryEvent` (a subscriber sees the same stream as `on_progress`),
      each HTTP retry, and each dead-letter capture. Targets are namespaced
      (`tpt_stream_core::pipeline`, `::httpclient`, `::dead_letter`) for
      filtering. The existing `ProgressHook` is untouched — the two are
      independent.
- [x] Optional: opt-in Prometheus/OpenTelemetry exporter in `tpt-stream-cli`
      fed by existing `Pipeline::stage_stats()`
      **Implementation**: `tptforge run --metrics <ADDR> --metrics-name <NAME>`
      serves `/metrics` in Prometheus text format, driven by the engine's
      telemetry stream (`metrics` module; 10 tests including a live HTTP
      scrape). Off by default, std `TcpListener` only, no new dependency.
      OpenTelemetry is **not** implemented — an OTLP exporter needs a dependency
      that conflicts with the minimal-tree policy, and Prometheus text format is
      the portable, dependency-free choice.

### Engine Feature Gaps
- [x] Per-row dead-letter queue for stage-level errors (extend quarantine
      semantics beyond source-level `ErrorPolicy`)
      **Implementation**: `Pipeline::dead_letter(path)` captures rows a *stage*
      rejects instead of aborting the run. `isolate_and_capture` binary-narrows
      a failing batch (halve, re-run, recurse) at O(log n) stage calls per bad
      row, writing bad rows to CSV with `_dead_letter_stage`, `_error`, then the
      row's own fields. The header is written lazily, so a clean run leaves an
      empty file rather than a header for a schema nobody saw. Survivors are
      re-assembled in original input order (FIFO plus a row index; a LIFO stack
      emits them out of sequence, which the tests pin down).
      **Deliberate limit**: stateful stages (`GroupByAgg`, `Sort`,
      `Deduplicate`, `HashJoin`, `expect`) fail with a clear `Error::Config`
      rather than silently mis-aggregating, because narrowing would re-run them
      over a subset of their input. 13 tests.
- [x] Retry/backoff for network sources & sinks (S3/GCS/Azure/HTTP/Postgres)
      **Implementation**: `httpclient::RetryPolicy` (attempts, base delay, cap)
      with exponential backoff plus deterministic jitter, applied in
      `execute_with_retry`. Only *transient* failures retry — transport errors
      and 408/429/5xx; a 4xx fails fast because retrying it cannot help. Opt-in
      via `S3Store::with_retry` / `AzureBlobStore::with_retry`; the **default is
      no retry**, so existing behavior is bit-for-bit unchanged. A retry re-sends
      an identical body, which is safe for the idempotent requests the cloud
      modules issue. 7 tests.
      **Postgres sink**: the pre-existing "retry once on connection loss" is now
      `PostgresSink::with_retry(RetryPolicy)`. The default `DEFAULT_PG_RETRY` is
      one immediate reconnect with no delay, i.e. exactly the old behavior.
      `is_transient_postgres_error` classifies on SQLSTATE — `08` (connection),
      `40001`/`40P01` (serialization, deadlock), `53`, `57`, `58` retry; `42xxx`
      (syntax/access), `22xxx`/`23xxx` (data/constraint) and anything
      unparseable fail immediately rather than burning connections on a fault
      that cannot resolve itself. Unparseable errors default to permanent on
      purpose. 9 tests.
- [x] Parquet read/write (optional `parquet` feature flag) — **closed as rejected
      by policy (2026-09-29)**, not an open task. Contradicts `spec.txt` ("we
      reject heavy, complex dependencies (like `parquet`, `wasmtime`) even if
      they're permissively licensed") and `WHY.md` ("no `parquet`, no `wasmtime`,
      no Arrow ... builds narrower replacements instead"). Arrow was already
      removed for this reason; `.tptcol` is the project's answer to Parquet.
      Revisit only as a deliberate policy change, not a feature request.
- [x] Window functions (row_number/rank/running totals) — **moved to the
      future-ideas list below**. Stretch goal: it needs a window-partitioning
      design layered on the existing sort spill, which is real design work, not
      just another stage.
- Not planned now (logged as future-phase ideas): Kafka/streaming sources,
  Delta/Iceberg, Arrow interop, checkpointing/resume, window functions

### Hardening
- [x] Audit `tpt-stream-py/src/lib.rs` unwraps for panic containment at the
      Python boundary
      **Finding**: PyO3 wraps `#[pymethods]` in catch_unwind; mutex
      `lock().unwrap()` replaced with `map_err(|_| lock_err())?` (PyResult
      fns) and `unwrap_or_else(|e| e.into_inner())` (non-Result fns/closures).
- [x] Audit `tpt-stream-ffi/src/lib.rs` unwraps (catch_unwind already wraps
      entry points — verify coverage is complete)
      **Finding**: all 17 entry points wrapped in catch_unwind; the one
      internal `.unwrap()` (line 267) is inside catch_unwind and infallible
      (CString::to_str on a UTF-8-validated string with no interior NULs).
- [x] Audit `tpt-stream-columnar/src/format.rs` unwraps for untrusted/corrupt
      `.tptcol` input; replace with `Result`/`Error::Format` where reachable
      **Finding**: all `try_into().unwrap()` calls were infallible in context
      (slices are always the right size due to prior bounds checks); replaced
      with `map_err(|_| FormatError::Malformed(...))` to make invariants
      explicit and guard against future refactors.
- [x] Add a regression test feeding a truncated/corrupted `.tptcol` file into
      the columnar reader, asserting a clean error instead of a panic

### tpt-csv: Beyond parity with the `csv` crate (2026-09-20)

`tpt-csv` currently matches the external `csv` crate's design (scalar,
row-oriented, always-owned `StringRecord`); `tpt-stream-core/src/source.rs`
already works around that with its own hand-rolled row-major
`arena`/`cells` table plus a parallel transpose in `build_csv_batch`. Goal:
move those optimizations into `tpt-csv` itself so it's a genuine
improvement for any consumer, and delete the workaround code in
`source.rs`. (Original design notes live in the author's local plan archive
under `~/.claude/plans/`, which is intentionally not checked in.)

- [x] Phase 1: SWAR/word-at-a-time bulk byte scanning in
      `tpt-csv/src/reader.rs::read_record_raw` (find next `,`/`"`/`\n` a
      word at a time instead of a branch-per-byte loop); no API/dependency
      change
- [x] Phase 2: `tpt-csv/src/columnar.rs` — `ColumnarReader`/`ColumnarChunk`
      that parses CSV directly into per-column (SoA) arenas/offsets, with
      ragged-row staging so a bad row never corrupts already-committed
      columns; plus a lower-level `Reader::read_record_into(arena, cells)`
      zero-copy row API. No new dependency on `tpt-stream-columnar`.
- [x] Migrate `tpt-stream-core/src/source.rs` (`csv_read_stream`,
      `csv_reader_to_batches`, `read_csv_batches`/`csv_to_batches`,
      `build_csv_batch_from_chunk`/`build_csv_column_from_iter`) to consume
      `ColumnarReader`, dropping the hand-rolled row-major arena + strided
      transpose; all 38 unit tests + 9 roundtrip tests pass
- [x] Phase 3: `tpt_csv::find_chunk_boundaries` (sequential, quote-aware
      pre-scan for parallel-safe split points); use it in `tpt-stream-core`
      (already depends on `rayon`) to parallelize the whole-buffer
      `csv_to_batches` / `read_csv_batches` paths only — the bounded-memory
      streaming `CsvSource` stays sequential by design
      **Implementation**: `tpt-csv/src/parallel.rs` adds
      `find_chunk_boundaries` + `first_record_end` + `ChunkBoundary`, sharing
      one quote state machine with the reader (19 tests, including a
      split-transparency property check over quoted/CRLF/no-trailing-newline
      input). `ColumnarReader::from_slice` parses a headerless slice given the
      document's column count, and `ReaderBuilder::start_line` keeps
      `Error::Ragged { line }` in whole-document coordinates so error messages
      are unchanged by slicing. `source.rs::csv_bytes_to_batches` reads the
      header once, then parses slices with `par_iter` and builds batches in
      slice order (so type inference still sees the first rows first).
      Gated at `PARALLEL_CSV_MIN_BYTES` (1 MiB); `.gz` inputs stay on the
      streaming decompression path. 9 new tests; full suite 210 pass.
- [x] Add `criterion` bench (wide numeric CSV) comparing old vs. new CSV
      ingestion path in `tpt-stream-core`'s bench suite before/after Phase 2
      **Result**: `csv_ingest_wide_numeric` in `benches/phase3.rs` compares
      `csv_to_batches` (parallel) against the new
      `csv_to_batches_sequential` escape hatch on identical 400k-row x 16-col
      input: **465 ms -> 116 ms (~4.0x)** on this machine. Note the pre-Phase-2
      row-major path is gone, so the comparison is parallel vs. sequential
      rather than old vs. new.
- [x] Add `ColumnarReader` unit tests (simple/quoted/ragged rows under
      strict/skip/quarantine; chunk-size bounds; 6 tests in columnar.rs)

---

## Phase 12: Review Follow-ups — Stubs, Security, Adoption (2026-09-29)

Plan: local plan archive under `~/.claude/plans/` (not checked in).
**Dependency rule:** the released project is dual MIT/Apache-2.0, but the
dependency chain must stay pure-MIT-satisfiable — no Apache-only crates, not
even opt-in features. Check `cargo deny list` before adding any crate; hand-roll
with std where a candidate fails.

### 12.1 Stubs, doc drift, loose ends
- [x] Rewrite `tpt-stream-ffi/README.md` to the real 17-entry-point API; fix
      root `README.md` claim that Python uses the FFI crate
- [x] CLI: `dead_letter` TOML key wired to `Pipeline::dead_letter` and the
      `tptforge_dead_letter_rows` metric (was always 0); CLI `limit` stage
- [x] `browser/index.d.ts`: declare `TelemetryEvent`; document that `execute()`
      is a serialise-only call in both wasm `.d.ts` files
- [x] Fix the 2 new CLI tests (`limit_stage_keeps_only_the_first_rows`,
      `dead_letter_key_creates_the_queue_file`) to use valid TOML source/sink
      syntax — both used `[source] csv = "..."`, but only *sinks* accept the
      scalar path shorthand (`PathOnlySpec`); a source needs
      `[source.csv] path = "..."`. 12 CLI + 14 SQL tests pass.
- [x] Python: expose `dead_letter`, `dead_letter_rows`, `limit`, and
      `with_retry` (S3/GCS/Azure/Postgres). `with_retry` is **one-shot**: it is
      consumed by the next network `read_*`/`write_*`, so a policy cannot leak
      onto a later source. This needed two core API gaps: pre-built-store
      variants (`read_s3_store`/`write_s3_store`/`read_gcs_store`/
      `write_gcs_store`/`write_postgres_sink`) and `GcsStore::with_retry`.
      5 new pytest cases.
- [x] Python typing: `py.typed` + a hand-written `_native.pyi` (no `stubgen`),
      `Typing :: Typed` classifier, `[project.urls]`, a `pandas` extra, and a
      maturin `include` list so both ship in the sdist *and* the wheel. A test
      asserts every public builder method appears in the stub, so the stub
      cannot silently rot.
- [x] `tpt-stream-columnar/src/column.rs`: the three public-API `panic!`s on
      type mismatch now sit behind fallible `try_push`/`try_set`/
      `try_push_value`/`try_set_value`/`try_extend_from`/`try_append_column`
      returning a new `TypeMismatch` error; the old names are thin documented
      wrappers that panic (a mismatch there is a bug in the caller, not bad
      input). `format.rs` (the untrusted `.tptcol` path) and a new
      `RecordBatch::try_append_rows` use the fallible forms, with
      `FormatError::TypeMismatch` and `table::AppendError` conversions. 4 tests.
- [x] `tpt-stream-core/src/agg.rs`: both COUNT-accumulator `panic!`s replaced by
      `count_one()`, which returns `Error::Other` naming the spec and the
      variant actually found (`Acc::kind()`), so an inconsistent spill-merge
      fails one run instead of aborting the process.
- [x] Repo hygiene: deleted `pytest_final.log` and `scripts/_patch_dates3.py`;
      removed the personal absolute path from `todo.md`;
      de-duplicated `tpt-stream-cli/examples/pipeline.toml` against
      `templates/pipeline-starter/pipeline.toml` — the starter stays the
      copy-me scaffold, the CLI example is now explicitly a *feature tour*
      that adds the CLI-only `limit` stage, with a new `cli_example_parses`
      test so neither can drift.
- [x] Refresh `spec.txt` to the shipped API. Corrected: the `csv` crate (now
      in-house `tpt-csv`, with the licensing reason), the zero-copy FFI claim
      (never implemented; `to_arrow` and the 11 `arrow` crates were removed and
      `to_pandas` goes via `collect()`), configurable CSV delimiters (fixed at
      `,`), `64 rows`/`64MB` (65,536 rows), the `Row`-mutating `map` closure
      (it takes column names + a `Row -> Vec<Value>`), and both the Python and
      JS "target API" snippets (module-level `sf.*` functions, `AND`/`mean`,
      `StreamForge`) to the real signatures. Status changed Draft → Implemented,
      with a note that per-crate READMEs win.
- [x] Run the ffi `c_integration` test on Windows CI. The old `if: runner.os !=
      'Windows'` existed because `rustc` emits a `cdylib` but **no import
      library**, so a C linker cannot resolve the exports from the `.dll`. The
      test now synthesizes one with `dlltool` (MinGW-w64, present in
      `windows-latest`) and links `-ltpt_stream_ffi`; the compiler and
      `dlltool` are both probed, and a missing tool prints `SKIPPED <reason>`
      and passes instead of failing. CI runs it on all three OSes with
      `--nocapture` so the skip reason is visible. Documented in
      `tpt-stream-ffi/README.md`.
- [x] Verify `count_all` output column naming in Python/WASM READMEs — and it
      was **wrong/vague**: the bindings genuinely differ. Python's
      `.agg({'k': 'count_all'})` names the output column after the dict key
      (so `{'amount': 'count_all'}` *replaces* `amount`); WASM always emits a
      column literally named `count_all`; the CLI uses `count_all` for the
      `"*"` key and `count_<col>` for a column key. All four READMEs, both wasm
      `.d.ts` files, and the `aggregate` rustdoc now state the exact rule and
      the cross-binding difference, with a pytest case pinning the Python one.
- [x] SQL frontend: `SELECT DISTINCT` (likely user expectation). Dedups on the
      **projected** columns (`SELECT DISTINCT region, product` keeps two `north`
      rows), `DISTINCT *` compares whole rows via `Deduplicate`'s empty-key
      mode, it runs after `WHERE` and before `ORDER BY`/`LIMIT`, and it is
      skipped when `GROUP BY` is present (already collapsed). 5 tests. The
      Postgres-style `DISTINCT ON (...)` is still rejected.
- [x] Close/move the unchecked Parquet and Window-function items above
- [~] tag v0.1.0 and replace the `[0.1.0] - placeholder` changelog heading
      **Done**: the heading is now `## [0.1.0] - Unreleased` with a `[0.1.0]`
      link footer and a note that the date is filled in when the tag is pushed.
      **Still open**: the `git tag v0.1.0` step itself (duplicates "Tag v0.1.0
      release" in the Documentation & Release section — this is the one to keep).

### 12.2 Security audit fixes (findings F1–F26)
- [x] F1 `.tptcol` reader: size caps, bounded reads, `checked_*` arithmetic, null
      bitmap length check, streaming zstd decode with output cap, strict UTF-8
      (5 regression tests)
- [x] F3 HTTP client: line/header-count caps, bounded error bodies, truncated
      fixed/chunked bodies are errors, strict chunk sizes, reject bad/duplicate
      Content-Length and CL+TE (11 tests)
- [x] F4 refuse plaintext `http://` to non-loopback hosts
      (`TPT_ALLOW_INSECURE_HTTP=1` to override)
- [x] F5 redact URL query strings in logs/errors (`redact_url`)
- [x] F6 expression/SQL parser depth (64) and token (4096) limits
- [x] F7 spill files in a private per-process dir (0700, unguessable name)
- [x] F8 quarantine: fail fast on unwritable path (was silently dropping rows),
      0600 on Unix
- [x] F9 redacting `Debug` for `CloudCredentials` / `AzureCredentials`
- [x] F15 reject CR/LF/NUL in request headers; F17 `Host` header includes port
- [x] F24 FFI: null-array checks in `tpt_pipeline_aggregate`, `catch_unwind` on
      free/num_rows/num_columns, UTF-8-safe truncation, real `tpt_last_error_code`
- [x] F1 follow-up: gzip decompressed output is now capped and the HTTP client
      has a `max_body` option
      **Implementation**: a new public `SourceLimits { max_record_bytes,
      max_decompressed_bytes }` carried by every file/HTTP/cloud source
      (`with_limits`). `gunzip_limited` wraps flate2's `MultiGzDecoder` in a
      `LimitedReader`, so a decompression bomb fails at 64 GiB by default
      instead of filling the disk, and it is used for both `.gz` files and
      gzip-encoded HTTP bodies. `HttpSource::with_max_body` /
      `Agent::with_max_body` cap the on-the-wire response; `into_string` keeps
      its own 16 MiB cap. New `tpt-stream-core/tests/limits.rs` covers
      bomb-rejected, exactly-at-limit-allowed, and the HTTP `max_body` path.
- [x] F14 line/record/field size caps: JSONL `read_line`, JSON-array scanner,
      `tpt-csv` reader (default 16 MiB, configurable)
      **Implementation**: `SourceLimits::max_record_bytes` (16 MiB default)
      threads into all three readers — `read_line_capped` for JSONL (uses
      `Read::take` on the `?Sized` reader, so it works on `&mut dyn BufRead`),
      `JsonArrayScanner::new(reader, max)` for JSON arrays, and
      `ColumnarReader::from_reader_with_max_record_bytes` for CSV. In
      `tpt-csv`, `ReaderBuilder::max_record_bytes` (with the exported
      `DEFAULT_MAX_RECORD_BYTES`) fails an oversized record or header with a
      new `Error::RecordTooLarge { line, limit }`; the check runs once per
      64 KiB refill, so a record may overshoot by up to one buffer before it
      trips. 2 new tests in `columnar.rs` (unterminated quote, oversized
      header).
- [x] F24 follow-up: distinct opaque handle types in the C header
      **Implementation**: `typedef struct TptPipeline TptPipeline;` and
      `typedef struct TptRecordBatch TptRecordBatch;` in
      `include/tpt_streamforge.h`, and every `void *` handle parameter in both
      the header and the Rust FFI is now typed with them. Passing a batch
      handle where a pipeline is expected is a compiler diagnostic instead of a
      silent misinterpretation. Layout stays private (forward-declared only).
- [x] F13 metrics server: request line capped at 8 KiB, header count at 100 and
      the block at 16 KiB (over-limit requests get a 431 rather than an
      unbounded buffer), a 5 s per-connection deadline plus 2 s read/write
      timeouts, and loopback-only by default — `serve` refuses `0.0.0.0`/`::`
      and any routable address unless the caller opts in via
      `serve_allow_remote` / `--metrics-allow-remote`, because the endpoint is
      unauthenticated. Also fixed a real Windows bug found while testing this:
      closing a socket with unread request bytes queued makes Windows send an
      RST, which makes the peer *discard the response it just received* — so
      reject paths now drain the remainder (bounded) and `respond` sends the FIN
      explicitly via `shutdown(Write)`. The two HTTP tests were flaky on
      Windows because of exactly this; 10/10 clean after the fix. 3 new tests.
- [~] F11 CI/release: top-level `permissions: contents: read`; scope `NPM_TOKEN`
      to publish steps; `npm publish --provenance`; pin actions to commit SHAs;
      replace `curl | sh` wasm-pack; `--locked` builds. Done except SHA pinning:
      could not be verified offline, so `uses:` keep tags with a `TODO(F11)`
      header in each workflow. wasm-pack is `cargo install --locked --version 0.13.1`.
- [~] F12 Dockerfile: non-root user (uid 10001), `--locked`, `.dockerignore`
      verified (+ `.env*`, `.devcontainer/`). Image digests NOT pinned (not
      verifiable offline); `TODO(F12)` comments in the Dockerfile.
- [x] F18/F19 percent-encode Azure blob keys; validate GCS bucket names
      **Implementation**: `azure::encode_blob_key` (RFC 3986 unreserved plus `/`;
      rejects empty keys, NUL and `.`/`..` segments) is used for both the request
      URL and the signed canonicalized resource, so keys with `?`, `#`, `%`,
      spaces or CR/LF no longer alter or split the request.
      `gcs::validate_bucket_name` (length, charset, start/end, `..`, IPv4 form,
      `goog`/`google`) runs in `GcsStore::new`. Unit tests in both modules.
- [x] F20 `PRAGMA query_only=ON` for the SQLite source; reject NUL in identifiers
      **Implementation**: the source sets `query_only` right after the read-only
      open and rejects NUL in its path/query; `quote_ident` is now fallible and
      rejects NUL for sink table/column names. 3 tests in `tests/sqlite.rs`.
- [x] F22 env-var substitution in pipeline TOML (keep passwords out of files)
      **Implementation**: `${VAR}` is substituted in the new
      `tpt-stream-cli/src/config.rs`. It runs on the **parsed** `toml::Value`s,
      never on the raw text, so a password containing a quote or a backslash
      cannot break the TOML syntax and can never end up inside a parser error
      snippet. `EnvLookup` is injected so tests need not touch the
      process-global environment, and `validate`/`explain` take `--no-env` to
      report on a spec with no environment at all. `--manifest` hashes the raw
      file bytes *before* substitution, so provenance never records a secret.
- [x] F10 TLS: log `load_native_certs` errors, fail clearly on empty root store,
      optional extra CA bundle path; re-verify `deny.toml` RUSTSEC ignores
      (`cargo tree -i rustls-webpki@0.102.8`) and add review-by dates.
      `ring` opt-in is **rejected** (Apache-only)
      **Implementation**: `httpclient::assemble_roots` logs native-loader errors
      (WARN, via the `tracing` feature) and fails clearly on an empty store: the
      first `https://` request returns "tls unavailable: no trusted root
      certificates ..." naming `SSL_CERT_FILE` / `TPT_EXTRA_CA_BUNDLE`. A PEM
      bundle is added from `TPT_EXTRA_CA_BUNDLE` or `Agent::with_ca_bundle`
      (missing/empty/invalid bundles are errors). `deny.toml` ignores were
      re-verified 2026-09-29: `rustls-webpki 0.102.8` is pulled only by
      `rustls-rustcrypto` for `alg_id` constants while rustls validates chains
      with 0.103.15; each ignore now has a `reason` with review-by 2026-12-29.
      `cargo deny check` is green. 4 unit tests.
- [x] F2 Postgres TLS: `postgres-tls` feature only if the dependency tree passes
      the MIT-only check; otherwise hand-roll over the existing rustls setup or
      refuse non-loopback hosts without TLS
      **Implementation**: took the "refuse" option. `postgres::check_transport`
      rejects any non-loopback TCP host (every host of a multi-host string; the
      error never contains the connection string) unless
      `TPT_ALLOW_INSECURE_POSTGRES=1`; Unix sockets are fine. No dependency added.
      Real TLS (`tokio-rustls` with default features off plus a hand-written
      `MakeTlsConnect`) looks MIT-satisfiable but was not attempted: it cannot
      be verified here without a TLS-enabled Postgres. 3 unit tests.

### 12.3 Adoption tooling
- [x] `release.yml`: build `tptforge` binaries (Linux x64, macOS arm64+x64,
      Windows x64), attach with SHA256SUMS + build provenance attestation +
      CycloneDX SBOM + installers (untested until a tag is pushed)
- [x] `[package.metadata.binstall]`, `install.sh` / `install.ps1` (checksum
      verified), Scoop/Homebrew templates in `packaging/` (hash placeholders,
      documented); `cargo install tpt-stream-cli` documented
- [x] `cargo audit` on PRs (`ci.yml` `audit` job, `just audit`); SBOM (`sbom` job)
- [x] `tptforge completions <shell>` and man page
      **Implementation**: `Command::Completions { shell: clap_complete::Shell }`
      (bash/zsh/fish/PowerShell/elvish) and `Command::Man { out_dir }`, which
      renders a `clap_mangen` page per subcommand via `man_command`.
- [x] `.devcontainer/`; cross-platform `just setup` (`.venv/Scripts` vs `bin`
      now chosen via `os_family()`)
- [x] `tptforge validate` / `explain` / `--dry-run`; `deny_unknown_fields` on spec
      structs; TOML line/column, stage index, and did-you-mean in errors
      **Implementation**: `tptforge validate` and `tptforge explain` subcommands
      plus `--dry-run` on `run`. Parsing is two-pass: a `RawDoc` of
      `Spanned<toml::Value>` records every part's byte offset, so a bad value
      reports as `line 12, column 3: stage #2: <message>` rather than a bare
      serde message. Unknown keys are a did-you-mean edit distance over the
      known field names. `deny_unknown_fields` is set on all 16 spec structs.
- [x] `tptforge schema-json` + checked-in `pipeline.schema.json` (CI freshness
      test) for editor autocomplete; hand-written, no `schemars`
      **Done**: `Command::SchemaJson` prints a hand-written draft-07 schema
      from `tpt-stream-cli/src/schema_json.rs` (no `schemars` dependency).
      `tpt-stream-cli/pipeline.schema.json` is checked in, and `tests/tools.rs`
      has the two guards: a freshness test (regenerate with
      `TPT_UPDATE_SCHEMA=1 cargo test -p tpt-stream-cli schema`) and one that
      compares each schema object's property list with the field list serde
      reports for the real struct, plus a dangling-`$ref` check.
- [x] `tptforge init <file>` (infer schema, commented starter TOML with `expect`
      checks), `doctor`, `convert`, `run --watch` (mtime poll, std only)
      **Implementation**: `Command::Init`, `Command::Doctor` (also validates an
      optional pipeline file and checks credentials),
      `Command::Convert`, and `--watch` on `run` (mtime poll, std only, no
      notify crate). `schema --save` / `--against` live in `tools.rs` with a
      `DriftReport` and a non-zero exit on drift.

### 12.4 New capabilities
- [x] Data contracts: `Range` / `OneOf` / `Type` checks in `expect.rs`
      **Implementation**: `Check::Range` (inclusive `[min, max]`, either bound
      optional, nulls skipped, NaN fails), `Check::OneOf` (allowed set, nulls
      skipped) and `Check::Type` (column must have the given `DataType`).
      Surfaced in Python as `ranges=` / `one_of=` / `types=` on `expect()`;
      `NoNulls` pairs with them when nulls are also forbidden.
- [x] Schema drift: `schema --save` and `schema --against` (non-zero exit on drift)
      **Implementation**: `tptforge schema --save FILE` writes the inferred
      schema as JSON; `--against FILE` returns a `DriftReport` and the CLI exits
      with status 2 on drift. Both in `tpt-stream-cli/src/tools.rs`.
- [x] `tptforge diff a b --key id` — streaming diff over the existing external
      sort + merge join
      **Implementation**: `tools::diff_command` sorts each side with core's
      external `Sort` into a temp `.tptcol`, then does one merge pass; output is
      CSV with a `_diff` column (`-`/`+`). Composite keys; duplicate keys and
      key-type mismatches are errors. Caveat: `Sort::finish` returns its merged
      output as one `Vec`, so peak memory is one sorted copy per input.
- [x] Deterministic keyed sampling stage (reuse `hash_key`)
      **Implementation**: `tpt-stream-core/src/sample.rs`. Keeps a row when a
      hash of `(seed, key columns)` lands below `fraction`, so it is
      reproducible, consistent per key (sampling on `customer_id` keeps whole
      customers) and stateless. The hash is a fixed FNV-1a + SplitMix64
      finaliser rather than `DefaultHasher`, whose algorithm is **not** stable
      across Rust releases; it reuses `agg::hash_values` and takes the top 53
      bits so the result is exactly representable in an `f64`.
- [x] `--manifest` provenance JSON (counts, stage stats, input/spec hashes)
      **Implementation**: `tpt-stream-cli/src/manifest.rs` — streamed SHA-256
      of each input (64 KiB blocks), the spec hash, per-stage `StageMetrics`
      and an RFC 3339 UTC timestamp. The spec is hashed as the **raw** file
      bytes, i.e. before `${VAR}` substitution, so a manifest never contains
      (or depends on) a secret.
- [ ] Checkpoint/resume for stateless pipelines only — **needs explicit approval**
      (checkpointing is listed above as "not planned")
