# Changelog — tpt-stream-ffi

Per-crate history. The workspace-wide log lives in the
[root CHANGELOG](../CHANGELOG.md).

## [Unreleased]

### Changed
- Distinct opaque handle types in the C header: `TptPipeline *` and
  `TptRecordBatch *` replace `void *`, so passing a batch to a pipeline
  function (or vice versa) is now a compile-time error in C. Source-compatible
  for C callers that only store the handles; callers that declared `void *p`
  still compile (implicit conversion) but lose the check. ABI is unchanged.

### Added
- Error code mapping for the new engine error kinds
  (`Cloud`, `DataQuality` -> `TPT_ERR_EXEC`).
- Cell rendering for `date` / `timestamp` values (ISO strings).

## [0.1.0] - 2026
Initial C ABI: `tpt_pipeline_*` construction/execution, CSV source/sink,
filter/map/aggregate/sort/dedup stages, `tpt_record_batch_*` readers,
`tpt_last_error_message`, cbindgen-generated header, C integration test.
