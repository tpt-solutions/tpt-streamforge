# Changelog — tpt-streamforge (Python)

Per-package history. The workspace-wide log lives in the
[root CHANGELOG](../CHANGELOG.md).

## [Unreleased]

### Added
- Full engine surface on `Pipeline`: `select`, `join_csv`,
  `read_jsonl`, `read_json`, `read_columnar`, `write_jsonl`, `write_json`,
  `write_columnar`, `read_sqlite`/`write_sqlite`,
  `read_postgres`/`write_postgres`, `read_s3`/`write_s3`,
  `read_gcs`/`write_gcs`, `read_azure`/`write_azure`, `read_http`,
  `on_error`, `expect`, `explain`, `preview`, `collect`.
- `to_arrow()` / `to_pandas()` export via arrow's PyArrow FFI (requires the
  `pyarrow` package at runtime).
- Runs release the GIL (`py.allow_threads`), so other Python threads keep
  executing while a pipeline streams.
- Progress events, per-stage `stage_stats()`, and date/timestamp columns
  (rendered as ISO strings).

## [0.1.0] - 2026
Initial PyO3 package: fluent `Pipeline` (read_csv, filter, map, group_by +
agg, sort, dedup, write_csv, execute), `TptError`, abi3 wheels via maturin.
