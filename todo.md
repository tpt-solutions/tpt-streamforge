# tpt-streamforge — Master Task Checklist
Dual-licensed MIT / Apache-2.0 | TPT Solutions

---

## Phase 1: Core Streaming Engine

### Setup
- [ ] Initialize Cargo workspace (`Cargo.toml` with 5 member crates)
- [ ] Add `deny.toml` with license allowlist/denylist
- [ ] Add `LICENSE-MIT` and `LICENSE-APACHE`
- [ ] Set up GitHub Actions CI: `cargo build`, `cargo test`, `cargo deny check`
- [ ] Verify initial dep tree passes `cargo deny check licenses`

### tpt-stream-core: Types
- [ ] Define `DataType` enum (Int32, Int64, Float32, Float64, Utf8, Bool)
- [ ] Define `Value` enum (typed runtime value)
- [ ] Define `Column` (DataType + buffer)
- [ ] Define `RecordBatch` (schema + Vec<Column> + row count)
- [ ] Implement fixed row-count chunking (default 65,536 rows/batch)

### tpt-stream-core: Pipeline
- [ ] Define `PipelineStage` trait (process RecordBatch → RecordBatch)
- [ ] Define `Pipeline` struct (ordered Vec of stages)
- [ ] Implement `Pipeline::execute()` with async tokio I/O

### tpt-stream-core: Sources & Sinks
- [ ] Implement streaming CSV source (wraps `csv` crate)
- [ ] Implement streaming CSV sink
- [ ] Implement streaming JSONL source (`serde_json` streaming deserializer)
- [ ] Implement streaming JSONL sink
- [ ] Implement streaming JSON array source/sink

### tpt-stream-core: Transformations
- [ ] Implement `Filter` stage (predicate expression on a Row)
- [ ] Implement `Map` stage (row-level closure / expression)
- [ ] Implement `Select` stage (column projection — keep/drop columns)

### tpt-stream-core: Tests & Benchmarks
- [ ] Unit tests for all core types
- [ ] Unit tests for Filter, Map, Select
- [ ] Integration test: CSV → filter → map → CSV round-trip
- [ ] Integration test: JSONL → filter → map → JSONL round-trip
- [ ] Benchmark: CSV throughput (target > 5M rows/sec)
- [ ] Confirm binary size < 15 MB

---

## Phase 2: Custom Columnar Format (tpt-stream-columnar)

### Format Design
- [ ] Document format layout: magic bytes, version header, schema section, column buffers
- [ ] Finalize primitive type encodings (Int32, Int64, Float32, Float64, Utf8, Bool)

### Implementation
- [ ] Implement typed column buffer (Vec<T> per DataType variant)
- [ ] Implement `RecordBatch → columnar` serialization
- [ ] Implement `columnar → RecordBatch` deserialization
- [ ] Implement fixed-size chunked writer (streaming, constant memory)
- [ ] Implement zstd compression for column buffers (feature-gated: `zstd`)
- [ ] Implement disk write: `.tptcol` file format
- [ ] Implement disk read: `.tptcol` file parser

### Integration with Core
- [ ] Add `.tptcol` as a source in `tpt-stream-core`
- [ ] Add `.tptcol` as a sink in `tpt-stream-core`

### Tests & Docs
- [ ] Unit tests: round-trip for each primitive type
- [ ] Integration test: CSV → columnar → CSV
- [ ] Integration test: columnar with zstd compression round-trip
- [ ] Write format spec in `tpt-stream-columnar/README.md`

---

## Phase 3: Advanced Transformations

### Aggregation
- [ ] Implement hash-based GROUP BY accumulator
- [ ] Implement streaming flush when accumulator hits memory limit
- [ ] Implement aggregation functions: SUM, AVG, COUNT, MIN, MAX
- [ ] Expose `GroupBy` + `Agg` pipeline stages

### External Merge Sort
- [ ] Implement in-memory sort phase (per RecordBatch)
- [ ] Implement spill-to-disk: write sorted runs as `.tptcol` files
- [ ] Implement k-way merge phase over sorted runs
- [ ] Expose `Sort` pipeline stage

### Streaming Hash Join
- [ ] Implement build phase: hash table from smaller (right) relation
- [ ] Implement probe phase: stream larger (left) relation
- [ ] Implement spill-to-disk for oversized hash tables
- [ ] Expose `Join` pipeline stage

### Deduplication
- [ ] Implement bloom filter for fast probabilistic dedup
- [ ] Implement exact dedup fallback via sort-based approach
- [ ] Expose `Deduplicate` pipeline stage

### Tests & Benchmarks
- [ ] Integration tests for aggregation, sort, join, dedup
- [ ] Benchmark: aggregation on 10M-row dataset

---

## Phase 4: FFI & Python Wrapper

### tpt-stream-ffi (C ABI)
- [ ] Define opaque handle types: `PipelineHandle`, `RecordBatchHandle`
- [ ] Define error code enum and `tpt_error_string()` API
- [ ] Export C functions: `pipeline_new`, `pipeline_read_csv`, `pipeline_filter`, `pipeline_map`, `pipeline_write_csv`, `pipeline_execute`, `pipeline_free`
- [ ] Export C functions: `record_batch_num_rows`, `record_batch_get_column`, `record_batch_free`
- [ ] Build as `cdylib`
- [ ] Generate C header with `cbindgen`
- [ ] Write C integration test

### tpt-stream-py (PyO3)
- [ ] Set up `maturin` build
- [ ] Implement Python class `Pipeline` (wraps Rust Pipeline)
- [ ] Expose: `read_csv(path, chunk_size=65536)`
- [ ] Expose: `filter(expr: str)`
- [ ] Expose: `map(expr: str)`
- [ ] Expose: `group_by(col).agg({col: fn_name})`
- [ ] Expose: `write_csv(path)`
- [ ] Expose: `execute()`
- [ ] Write Python test suite (pytest)
- [ ] GitHub Actions: build wheels for Linux/macOS/Windows (maturin)
- [ ] GitHub Actions: publish to PyPI on release tag
- [ ] Write Python README and API docs

---

## Phase 5: JavaScript / WASM Wrapper (tpt-stream-wasm)

### Build Setup
- [ ] Set up `wasm-bindgen` + `wasm-pack`
- [ ] Configure two build targets: `bundler` (browser) and `nodejs`

### Node.js Target
- [ ] Implement file I/O adapter via Node.js `fs` APIs
- [ ] Expose async API: `readCSV`, `filter`, `map`, `writeCSV`, `execute`
- [ ] Write Node.js test suite (Vitest or Jest)
- [ ] GitHub Actions: build Node.js WASM package
- [ ] Publish to npm as `tpt-streamforge-node`

### Browser Target
- [ ] Implement data input via `ArrayBuffer` (no direct filesystem access)
- [ ] Expose async API: `fromArrayBuffer`, `filter`, `map`, `toArrayBuffer`
- [ ] Write browser test suite
- [ ] GitHub Actions: build browser WASM package
- [ ] Confirm WASM binary size < 10 MB
- [ ] Publish to npm as `tpt-streamforge-browser`

### Docs
- [ ] Write JS/TS README and API reference

---

## Phase 6: Advanced Features

### Database Sinks & Sources
- [ ] SQLite sink via `rusqlite` (bulk insert, batched transactions)
- [ ] SQLite source (SELECT query → RecordBatch stream)
- [ ] PostgreSQL sink via `tokio-postgres` (bulk COPY protocol)
- [ ] PostgreSQL source (query → RecordBatch stream)
- [ ] Add `tokio-postgres` to `deny.toml` approved list
- [ ] Integration tests: SQLite sink/source
- [ ] Integration tests: PostgreSQL sink/source (Docker Postgres in CI)

### Cloud Storage
- [ ] Research minimal S3 crate (MIT/Apache-2.0, minimal transitive deps)
- [ ] Implement S3 source (streaming GET via `rustls`)
- [ ] Implement S3 sink (streaming PUT / multipart upload)
- [ ] Implement GCS source/sink (if a minimal crate is available)
- [ ] Implement Azure Blob source/sink (if a minimal crate is available)
- [ ] Integration tests for S3 (LocalStack in CI)

### Telemetry & Profiling
- [ ] Define telemetry hook interface (rows, bytes, elapsed per stage)
- [ ] Implement pipeline progress callback API
- [ ] Expose telemetry in Python wrapper
- [ ] Expose telemetry in JS wrapper
- [ ] Add `criterion` benchmark suite for all pipeline stages
- [ ] Publish benchmark results in project README

---

## Documentation & Release

- [ ] Write project `README.md` (what, why, quick start for all three languages)
- [ ] Write `CONTRIBUTING.md`
- [ ] Write `CHANGELOG.md`
- [ ] Create GitHub release workflow: tag → build → publish wheels + npm packages
- [ ] Add `cargo deny check` to release workflow
- [ ] Set up `docs.rs` metadata for `tpt-stream-core`
- [ ] Write performance comparison vs Pandas and DuckDB in README
- [ ] Tag v0.1.0 release
