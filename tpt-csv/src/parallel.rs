//! Quote-aware pre-scan for splitting a whole in-memory CSV buffer into slices
//! that can be parsed independently and concurrently.
//!
//! A byte offset is only a safe split point if it lands on a record boundary:
//! splitting inside a quoted field would hand a worker a fragment that starts
//! mid-field. [`find_chunk_boundaries`] walks the buffer once with the same
//! quote state machine as [`crate::Reader`] and reports the offsets where a
//! record ends, so each slice parses to exactly the records the single
//! sequential pass would have produced.
//!
//! ```no_run
//! use tpt_csv::find_chunk_boundaries;
//!
//! let buf = std::fs::read("big.csv").unwrap();
//! for b in find_chunk_boundaries(&buf, 65_536) {
//!     // Safe to parse `&buf[..b.offset]` and `&buf[b.offset..]` separately.
//!     let _ = &buf[b.offset..];
//! }
//! ```

use std::ops::ControlFlow;

/// A record-aligned split point in a CSV buffer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChunkBoundary {
    /// Byte offset of the first record of the next slice.
    pub offset: usize,
    /// 1-based record number of the record that *starts* at `offset` — the
    /// first record of the next slice, matching what
    /// [`crate::Reader::position`] reports for it. Callers that already
    /// consumed a header record add 1 for whole-document positions.
    pub record: u64,
}

/// Byte offset just past the first record in `buf` — i.e. where data rows begin
/// once a header record has been consumed.
///
/// `None` when `buf` contains no complete record (empty, or a single
/// unterminated line). As in the reader, a `\r` before the `\n` belongs to the
/// record, so the returned offset points *after* the line ending.
pub fn first_record_end(buf: &[u8]) -> Option<usize> {
    let mut end = None;
    scan_records(buf, |offset, _| {
        end = Some(offset);
        ControlFlow::Break(())
    });
    end
}

/// Split `buf` into roughly `records_per_chunk`-record slices, returning the
/// record-aligned offsets that separate them.
///
/// Every returned offset begins a record, so slicing `buf` at consecutive
/// returned offsets (using `0` and `buf.len()` as the outer edges) yields
/// slices that parse to exactly the records a single sequential pass over
/// `buf` would produce — including fields with embedded newlines, escaped
/// quotes, and `\r\n` endings.
///
/// Offsets are relative to `buf`, and each `record` is the 1-based number of
/// the record starting at its `offset` (so `offset` and `record` always refer
/// to the same record). A boundary is never emitted at `buf.len()` (which
/// would leave an empty trailing slice), and `records_per_chunk == 0` returns
/// no boundaries, leaving the caller with a single slice.
pub fn find_chunk_boundaries(buf: &[u8], records_per_chunk: usize) -> Vec<ChunkBoundary> {
    let mut out = Vec::new();
    if records_per_chunk == 0 {
        return out;
    }
    let target = records_per_chunk as u64;
    let mut since: u64 = 0;
    scan_records(buf, |offset, ended_record| {
        since += 1;
        if since == target {
            since = 0;
            if offset < buf.len() {
                // `scan_records` reports the record that just ended; the
                // boundary starts the one after it.
                out.push(ChunkBoundary {
                    offset,
                    record: ended_record + 1,
                });
            }
        }
        ControlFlow::Continue(())
    });
    out
}

/// Walk `buf` as a CSV record stream, invoking `on_record_end(offset, record)`
/// with the byte offset just past each record's terminator and that record's
/// 1-based ordinal. Returning [`ControlFlow::Break`] stops the scan.
///
/// This mirrors the quote state machine in `Reader::read_record_into` exactly
/// — the same `in_quotes` / `maybe_closing_quote` transitions, and the same
/// "a `"` only opens a quoted field while the field is still empty" rule — so
/// the offsets it reports always land on a record boundary.
///
/// It deliberately builds no field data and validates no UTF-8; it only
/// answers "where does this record end". A final record with no trailing
/// newline never ends at a `\n` and so is not reported here; callers include
/// it by treating the buffer's end as the last slice's end.
fn scan_records(buf: &[u8], mut on_record_end: impl FnMut(usize, u64) -> ControlFlow<()>) {
    let mut in_quotes = false;
    let mut maybe_closing_quote = false;
    // Whether the current field has no bytes yet — the reader's
    // `arena.len() == field_start` test for "a quote here opens a field".
    let mut field_empty = true;
    let mut record: u64 = 1;
    for (i, &b) in buf.iter().enumerate() {
        if maybe_closing_quote {
            maybe_closing_quote = false;
            if b == b'"' {
                // Escaped quote: the `"` is field data, so the field is no
                // longer empty and the quoted field reopens.
                in_quotes = true;
                field_empty = false;
                continue;
            }
            // The closing quote ended the field; fall through so this byte is
            // handled as the next unquoted byte.
        }

        if in_quotes {
            if b == b'"' {
                in_quotes = false;
                maybe_closing_quote = true;
            } else {
                field_empty = false;
            }
            continue;
        }

        match b {
            b'"' if field_empty => in_quotes = true,
            b',' => field_empty = true,
            b'\n' => {
                if on_record_end(i + 1, record).is_break() {
                    return;
                }
                record += 1;
                field_empty = true;
            }
            _ => field_empty = false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::reader::ReaderBuilder;
    use crate::record::StringRecord;

    fn read_all(bytes: &[u8]) -> Vec<Vec<String>> {
        let mut reader = ReaderBuilder::new().has_headers(false).from_reader(bytes);
        let mut record = StringRecord::new();
        let mut out = Vec::new();
        while reader.read_record(&mut record).unwrap() {
            out.push(record.iter().map(String::from).collect());
        }
        out
    }

    /// The core safety property: slicing at the reported boundaries and
    /// parsing each slice on its own must yield exactly what one sequential
    /// pass yields.
    fn assert_boundaries_are_transparent(csv: &str) {
        let bytes = csv.as_bytes();
        let expected = read_all(bytes);
        for records_per_chunk in [1, 2, 3, 5, 64] {
            let bounds = find_chunk_boundaries(bytes, records_per_chunk);
            // Monotonic, in range, and never at the very end.
            let mut prev = 0;
            for b in &bounds {
                assert!(b.offset > prev, "records_per_chunk={records_per_chunk}");
                assert!(b.offset < bytes.len());
                prev = b.offset;
            }
            let mut actual = Vec::new();
            let mut start = 0;
            for b in &bounds {
                actual.extend(read_all(&bytes[start..b.offset]));
                start = b.offset;
            }
            actual.extend(read_all(&bytes[start..]));
            assert_eq!(
                actual, expected,
                "records_per_chunk={records_per_chunk} csv={csv:?}"
            );
        }
    }

    #[test]
    fn boundaries_land_on_record_ends() {
        // Records "a,b" / "1,2" / "3,4" / "5,6" end at offsets 4, 8, 12, 16.
        // With 2 records per chunk the first boundary follows record 2; the
        // second would fall at `buf.len()` and is therefore not emitted.
        let bounds = find_chunk_boundaries(b"a,b\n1,2\n3,4\n5,6\n", 2);
        assert_eq!(
            bounds,
            vec![ChunkBoundary {
                offset: 8,
                record: 3
            }]
        );
    }

    #[test]
    fn no_boundary_at_end_of_buffer() {
        // The last record ends exactly at the buffer end, so no boundary is
        // emitted for it: an empty trailing slice is never produced.
        let bounds = find_chunk_boundaries(b"a\nb\nc\n", 1);
        assert_eq!(
            bounds,
            vec![
                ChunkBoundary {
                    offset: 2,
                    record: 2
                },
                ChunkBoundary {
                    offset: 4,
                    record: 3
                },
            ]
        );
    }

    #[test]
    fn zero_records_per_chunk_yields_no_boundaries() {
        assert!(find_chunk_boundaries(b"a\nb\n", 0).is_empty());
    }

    #[test]
    fn empty_buffer_has_no_boundaries() {
        assert!(find_chunk_boundaries(b"", 4).is_empty());
    }

    #[test]
    fn unterminated_final_record_yields_no_trailing_boundary() {
        let bounds = find_chunk_boundaries(b"a\nb", 1);
        assert_eq!(
            bounds,
            vec![ChunkBoundary {
                offset: 2,
                record: 2
            }]
        );
    }

    #[test]
    fn embedded_newlines_do_not_create_boundaries() {
        let bounds = find_chunk_boundaries(b"\"x\ny\",z\n1,2\n", 1);
        assert_eq!(
            bounds,
            vec![ChunkBoundary {
                offset: 8,
                record: 2
            }]
        );
    }

    #[test]
    fn boundaries_are_transparent_for_plain_rows() {
        assert_boundaries_are_transparent("a,b\n1,2\n3,4\n5,6\n7,8\n");
    }

    #[test]
    fn boundaries_are_transparent_for_quoted_fields() {
        assert_boundaries_are_transparent(
            "a,b\n\"x,y\",2\n\"line1\nline2\",4\n\"say \"\"hi\"\"\",6\n,\n",
        );
    }

    #[test]
    fn boundaries_are_transparent_for_crlf() {
        assert_boundaries_are_transparent("a,b\r\n1,2\r\n\"q\r\nr\",4\r\n");
    }

    #[test]
    fn boundaries_are_transparent_without_trailing_newline() {
        assert_boundaries_are_transparent("a,b\n1,2\n3,4");
    }

    #[test]
    fn boundary_record_ordinals_are_accurate() {
        // The boundary starts data record 2, which is document record 3
        // (1-based, counting the header).
        let bounds = find_chunk_boundaries("h1,h2\n1,2\n3,4\n5,6\n".as_bytes(), 2);
        assert_eq!(
            bounds[0],
            ChunkBoundary {
                offset: 10,
                record: 3
            }
        );
    }

    #[test]
    fn boundary_ordinals_match_reader_positions() {
        let csv = "h1,h2\n1,2\n3,4\n5,6\n7,8\n";
        for b in find_chunk_boundaries(csv.as_bytes(), 1) {
            let mut reader = ReaderBuilder::new()
                .has_headers(false)
                .from_reader(&csv.as_bytes()[..b.offset]);
            let mut record = StringRecord::new();
            while reader.read_record(&mut record).unwrap() {}
            assert_eq!(reader.position().line(), b.record, "offset={}", b.offset);
        }
    }

    #[test]
    fn first_record_end_of_plain_header() {
        assert_eq!(first_record_end(b"a,b\n1,2\n"), Some(4));
    }

    #[test]
    fn first_record_end_ignores_newlines_inside_quotes() {
        assert_eq!(first_record_end(b"\"a\nb\",c\n1,2\n"), Some(8));
    }

    #[test]
    fn first_record_end_handles_crlf() {
        assert_eq!(first_record_end(b"a,b\r\n1,2\r\n"), Some(5));
    }

    #[test]
    fn first_record_end_of_unterminated_record_is_none() {
        assert_eq!(first_record_end(b"a,b"), None);
    }

    #[test]
    fn first_record_end_of_empty_buffer_is_none() {
        assert_eq!(first_record_end(b""), None);
    }
}
