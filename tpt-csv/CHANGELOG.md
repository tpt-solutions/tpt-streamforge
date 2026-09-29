# Changelog — tpt-csv

Per-crate history. The workspace-wide log lives in the
[root CHANGELOG](../CHANGELOG.md).

## [Unreleased]

### Added
- `ReaderBuilder::max_record_bytes` (default `DEFAULT_MAX_RECORD_BYTES`, 16 MiB)
  and `ColumnarReader::from_reader_with_max_record_bytes`: a record larger than
  the cap (e.g. an unterminated quoted field) fails with the new
  `Error::RecordTooLarge` instead of buffering the rest of the input. Checked
  once per 64 KiB refill, so the cap may be exceeded by up to one buffer.
- `parallel` module for splitting a whole in-memory CSV buffer into slices that
  can be parsed independently and concurrently:
  - `find_chunk_boundaries(buf, records_per_chunk)` — a single quote-aware
    pre-scan returning `ChunkBoundary { offset, record }` values that always
    begin a record, so slicing at them yields exactly the records one
    sequential pass would produce (embedded newlines, escaped quotes, and
    `\r\n` included). Never emits a boundary at `buf.len()`.
  - `first_record_end(buf)` — offset just past the header record, for
    separating headers from data.
  - `ColumnarReader::from_slice(reader, num_columns, first_record)` — parses a
    headerless slice given the document's column count.
  - `ReaderBuilder::start_line(line)` — sets the record number reported for the
    first record, so a slice's `position()` and `Error::Ragged { line }` stay
    in whole-document coordinates.
  No new dependencies; the boundary scan shares the reader's exact quote state
  machine.
- Initial crate: `Reader`/`ReaderBuilder` (streaming, RFC 4180 subset, quoting,
  embedded newlines, `\r\n`, optional headers, line tracking) and
  `Writer`/`WriterBuilder` (minimal quoting), plus `StringRecord` (reused
  field buffers) and `Error`/`Result`. Zero dependencies — `std` only — so the
  engine's CSV path no longer pulls in `csv`/`csv-core`/`ryu`
  (`ryu` is Apache-2.0-only; this crate keeps the tree consumable under MIT
  terms alone). Float formatting uses the standard library rather than `ryu`.
