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
- `to_pandas()` export via `collect()` + `pandas.DataFrame` (requires the
  `pandas` package at runtime; no Arrow dependency, to keep the dependency
  tree free of Apache-2.0-only crates).
- Runs release the GIL (`py.allow_threads`), so other Python threads keep
  executing while a pipeline streams.
- Progress events, per-stage `stage_stats()`, and date/timestamp columns
  (rendered as ISO strings).

### Changed
- `pyo3` bumped 0.23 → 0.29 to clear two RustSec advisories (buffer
  overflow in `PyString::from_object`, missing `Sync` bound on
  `PyCFunction::new_closure`): `PyObject` → `Py<PyAny>`,
  `Python::with_gil` → `Python::attach`, `py.allow_threads` →
  `py.detach`.

## [0.1.0] - 2026
Initial PyO3 package: fluent `Pipeline` (read_csv, filter, map, group_by +
agg, sort, dedup, write_csv, execute), `TptError`, abi3 wheels via maturin.
