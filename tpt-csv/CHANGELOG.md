# Changelog — tpt-csv

Per-crate history. The workspace-wide log lives in the
[root CHANGELOG](../CHANGELOG.md).

## [Unreleased]

### Added
- Initial crate: `Reader`/`ReaderBuilder` (streaming, RFC 4180 subset, quoting,
  embedded newlines, `\r\n`, optional headers, line tracking) and
  `Writer`/`WriterBuilder` (minimal quoting), plus `StringRecord` (reused
  field buffers) and `Error`/`Result`. Zero dependencies — `std` only — so the
  engine's CSV path no longer pulls in `csv`/`csv-core`/`ryu`
  (`ryu` is Apache-2.0-only; this crate keeps the tree consumable under MIT
  terms alone). Float formatting uses the standard library rather than `ryu`.
