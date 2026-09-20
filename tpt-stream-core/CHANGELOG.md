# Changelog — tpt-stream-core

All notable changes to the engine crate. The workspace-wide history lives in
the [root CHANGELOG](../CHANGELOG.md).

## [Unreleased]

### Added
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

### Fixed
- Ragged CSV rows (wrong field count) aborted with a line-numbered schema
  error instead of silently shifting column packing and corrupting data.
- Re-running an executed pipeline errors instead of silently yielding 0 rows.
- `SinkBatch.total_rows` counts rows written to the sink.

## [0.1.0] - 2026

Initial engine: streaming CSV/JSONL/JSON + `.tptcol` I/O, filter/map/select,
external merge sort, hash aggregation, hash join, bloom dedup, expression
language, SQLite/PostgreSQL/S3/GCS/Azure sources and sinks, telemetry hooks.
