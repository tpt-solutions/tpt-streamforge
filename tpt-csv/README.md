# tpt-csv

Minimal, dependency-free streaming CSV reader and writer, written for
[tpt-streamforge](https://github.com/tpt-solutions/tpt-streamforge).

It exists so that the ETL engine's dependency tree can be consumed under MIT
terms alone: the `csv` crate (excellent, but Apache-2.0-only through its `ryu`
edge) is replaced by this crate, which pulls in **no crates at all** — only
`std` — and formats floats with the standard library instead of `ryu`.

## API

```rust
use tpt_csv::{ReaderBuilder, StringRecord, WriterBuilder};

// Reading: streaming, one record at a time, buffers reused across records.
let mut reader = ReaderBuilder::new()
    .has_headers(true)   // default; `headers()` reads/returns the first record
    .from_reader("a,b\n1,2\n".as_bytes());
assert_eq!(reader.headers()?.iter().collect::<Vec<_>>(), vec!["a", "b"]);

let mut record = StringRecord::new();
while reader.read_record(&mut record)? {
    println!("{record:?}");
}

// Writing: quotes only when needed, flushes on every record and on drop.
let mut out = Vec::new();
let mut writer = WriterBuilder::new().from_writer(&mut out);
writer.write_record(["a", "b,c"])?;   // -> a,"b,c"\n
# Ok::<(), tpt_csv::Error>(())
```

Any `Read`/`Write` works — files, `Vec<u8>`, network readers, gzip decoders —
so the engine can stream CSV from disk, from an HTTP response body, or from
object storage without buffering the whole object.

## Format support (RFC 4180 subset)

| feature | supported |
| --- | --- |
| field separator | `,` |
| record separator | `\n` or `\r\n` |
| quoted fields | `"` — may contain commas, quotes (`""`), and embedded newlines |
| header row | optional, via `ReaderBuilder::has_headers` |
| ragged records | permitted — `StringRecord::len()` reports the actual field count |
| invalid UTF-8 | `Error::Utf8 { line }` (never lossy-decoded) |
| custom delimiter / escape char | not supported (not needed by the engine) |

The reader deliberately does **not** enforce a fixed field count per record.
Callers that care (the engine's CSV source, which reports ragged rows as
line-numbered errors or quarantines them) inspect `record.len()` and decide.
`ReaderBuilder::flexible` and `WriterBuilder::flexible` are accepted as no-ops
so switching from the `csv` crate is a one-line dependency change.

## Design notes

- **No allocation per record on the read path**: `StringRecord` keeps one
  `String` plus a `Vec<usize>` of field ends, both cleared and reused; fields
  are borrowed slices via `get`/`iter`.
- **Fixed 64 KiB read buffer** through `BufReader`, so peak memory is the
  buffer plus the current record, not the file.
- **Quote-only-when-needed** on write (matches the `csv` crate's default), so
  output is byte-identical for ordinary data and stays Excel-friendly.
- **Line numbers** are tracked for error messages: `Reader::position()` (the
  line the next record starts on) and `Error::Utf8 { line }`.
- **Lenient about a lone trailing `\r`**: it is kept as data unless followed by
  `\n`, so files that mix line endings do not silently lose characters.

## Tests

```sh
cargo test -p tpt-csv
```

Covers plain/quoted/embedded-newline fields, escaped quotes, `\r\n`, empty
fields, ragged rows, headers, positions, invalid UTF-8, and EOF edge cases
(`""`, trailing field without a newline, empty input).
