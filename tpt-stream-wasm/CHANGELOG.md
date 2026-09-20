# Changelog — tpt-stream-wasm

Per-crate history (see also `node/` and `browser/` package notes). The
workspace-wide log lives in the [root CHANGELOG](../CHANGELOG.md).

## [Unreleased]

### Added
- Date/timestamp support in the engine (ISO string I/O, sorting, grouping,
  JSON serialization).
- In-browser pipeline playground under `browser/playground/`.

## [0.1.0] - 2026
Initial wasm-bindgen engine: `Engine.from_csv`, filter, map, sort, dedup,
aggregate, `to_csv`, `to_json_text`; CJS Node wrapper and ESM browser
wrapper with per-batch progress callbacks.
