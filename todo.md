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





