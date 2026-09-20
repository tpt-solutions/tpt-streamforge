use std::io::{BufRead, BufReader, Read};

use crate::error::{Error, Result};
use crate::record::StringRecord;

/// Where a reader currently is in its input, for error messages.
#[derive(Debug, Clone, Copy, Default)]
pub struct Position {
    line: u64,
}

impl Position {
    /// 1-based line number of the record about to be (or just) read.
    pub fn line(&self) -> u64 {
        self.line
    }
}

#[derive(Debug, Clone, Copy)]
pub struct ReaderBuilder {
    has_headers: bool,
}

impl Default for ReaderBuilder {
    fn default() -> Self {
        ReaderBuilder { has_headers: true }
    }
}

impl ReaderBuilder {
    pub fn new() -> Self {
        ReaderBuilder::default()
    }

    /// Whether the first record is a header row, consumed by
    /// [`Reader::headers`] rather than [`Reader::read_record`]. Defaults to
    /// `true`.
    pub fn has_headers(&mut self, yes: bool) -> &mut Self {
        self.has_headers = yes;
        self
    }

    /// Accepted for API compatibility with the `csv` crate; this reader
    /// never enforces a fixed field count per record (callers that care
    /// check `record.len()` themselves), so this is always a no-op.
    pub fn flexible(&mut self, _yes: bool) -> &mut Self {
        self
    }

    pub fn from_reader<R: Read>(&self, reader: R) -> Reader<R> {
        Reader {
            inner: BufReader::with_capacity(64 * 1024, reader),
            has_headers: self.has_headers,
            headers: None,
            line: 1,
        }
    }
}

/// A streaming CSV reader over any [`Read`]r.
///
/// Fields may be quoted with `"`; a literal `"` inside a quoted field is
/// written as `""`. Records end at `\n` or `\r\n`. Rows are not required to
/// have the same number of fields — callers that need that invariant check
/// `record.len()` themselves (this matches the engine's own ragged-row
/// handling, which wants to see and report the deviation, not have it
/// swallowed by the parser).
pub struct Reader<R> {
    inner: BufReader<R>,
    has_headers: bool,
    headers: Option<StringRecord>,
    line: u64,
}

impl<R: Read> Reader<R> {
    /// The header record. Reads it from the input on first call, then
    /// returns the cached copy.
    pub fn headers(&mut self) -> Result<&StringRecord> {
        if self.headers.is_none() {
            let mut record = StringRecord::new();
            self.read_record_raw(&mut record)?;
            self.headers = Some(record);
        }
        Ok(self.headers.as_ref().expect("just set"))
    }

    /// Current position (the line the *next* `read_record` will start at).
    pub fn position(&self) -> Position {
        Position { line: self.line }
    }

    /// Read one record into `record`, reusing its buffers. Returns `Ok(true)`
    /// if a record was read, `Ok(false)` at EOF.
    ///
    /// On the first call, if the reader was built with `has_headers(true)`
    /// and [`headers`](Reader::headers) hasn't been called yet, the header
    /// row is skipped automatically (matching the `csv` crate's behavior).
    pub fn read_record(&mut self, record: &mut StringRecord) -> Result<bool> {
        if self.has_headers && self.headers.is_none() {
            let mut header_record = StringRecord::new();
            let got = self.read_record_raw(&mut header_record)?;
            self.headers = Some(header_record);
            if !got {
                record.clear();
                return Ok(false);
            }
        }
        self.read_record_raw(record)
    }

    fn read_record_raw(&mut self, record: &mut StringRecord) -> Result<bool> {
        record.clear();
        let mut field: Vec<u8> = Vec::with_capacity(32);
        let mut in_quotes = false;
        let mut maybe_closing_quote = false;
        let mut pushed_any_field = false;
        // Set as soon as any byte of this record has been consumed, so that a
        // final record whose only content is an empty quoted field (`""`) is
        // still reported instead of being mistaken for an exhausted input.
        let mut record_started = false;
        let start_line = self.line;

        loop {
            let buf = self.inner.fill_buf()?;
            if buf.is_empty() {
                if pushed_any_field || !field.is_empty() || record_started {
                    push_field(record, field, start_line)?;
                    return Ok(true);
                }
                return Ok(false);
            }
            let mut consumed = 0;
            let mut record_done = false;
            for &b in buf {
                consumed += 1;
                record_started = true;

                if maybe_closing_quote {
                    maybe_closing_quote = false;
                    if b == b'"' {
                        field.push(b'"');
                        in_quotes = true;
                        continue;
                    }
                    // else: the quote really closed the field; fall through
                    // and process `b` under the "not in quotes" rules below.
                }

                if in_quotes {
                    if b == b'"' {
                        in_quotes = false;
                        maybe_closing_quote = true;
                    } else {
                        field.push(b);
                    }
                    continue;
                }

                match b {
                    b'"' if field.is_empty() => {
                        in_quotes = true;
                    }
                    b',' => {
                        push_field(record, std::mem::take(&mut field), start_line)?;
                        pushed_any_field = true;
                    }
                    b'\n' => {
                        self.line += 1;
                        if field.last() == Some(&b'\r') {
                            field.pop();
                        }
                        record_done = true;
                        break;
                    }
                    _ => field.push(b),
                }
            }
            self.inner.consume(consumed);
            if record_done {
                push_field(record, field, start_line)?;
                return Ok(true);
            }
        }
    }
}

fn push_field(record: &mut StringRecord, bytes: Vec<u8>, line: u64) -> Result<()> {
    let text = std::str::from_utf8(&bytes).map_err(|_| Error::Utf8 { line })?;
    record.push_field(text);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn read_all(text: &str) -> Vec<Vec<String>> {
        let mut reader = ReaderBuilder::new()
            .has_headers(false)
            .from_reader(text.as_bytes());
        let mut out = Vec::new();
        let mut record = StringRecord::new();
        while reader.read_record(&mut record).unwrap() {
            out.push(record.iter().map(String::from).collect());
        }
        out
    }

    #[test]
    fn simple_rows() {
        assert_eq!(
            read_all("a,b,c\n1,2,3\n"),
            vec![
                vec!["a".to_string(), "b".to_string(), "c".to_string()],
                vec!["1".to_string(), "2".to_string(), "3".to_string()],
            ]
        );
    }

    #[test]
    fn no_trailing_newline() {
        assert_eq!(read_all("a,b"), vec![vec!["a".to_string(), "b".to_string()]]);
    }

    #[test]
    fn crlf_line_endings() {
        assert_eq!(
            read_all("a,b\r\nc,d\r\n"),
            vec![
                vec!["a".to_string(), "b".to_string()],
                vec!["c".to_string(), "d".to_string()],
            ]
        );
    }

    #[test]
    fn quoted_field_with_comma_and_newline() {
        assert_eq!(
            read_all("\"hello, world\",\"line1\nline2\"\n"),
            vec![vec!["hello, world".to_string(), "line1\nline2".to_string()]]
        );
    }

    #[test]
    fn escaped_quote_inside_quoted_field() {
        assert_eq!(
            read_all("\"she said \"\"hi\"\"\"\n"),
            vec![vec!["she said \"hi\"".to_string()]]
        );
    }

    #[test]
    fn empty_fields() {
        assert_eq!(
            read_all("a,,c\n,,\n"),
            vec![
                vec!["a".to_string(), "".to_string(), "c".to_string()],
                vec!["".to_string(), "".to_string(), "".to_string()],
            ]
        );
    }

    #[test]
    fn ragged_rows_are_not_rejected() {
        assert_eq!(
            read_all("a,b,c\n1,2\n"),
            vec![
                vec!["a".to_string(), "b".to_string(), "c".to_string()],
                vec!["1".to_string(), "2".to_string()],
            ]
        );
    }

    #[test]
    fn headers_are_skipped_by_read_record() {
        let mut reader = ReaderBuilder::new()
            .has_headers(true)
            .from_reader("a,b\n1,2\n3,4\n".as_bytes());
        assert_eq!(
            reader.headers().unwrap().iter().collect::<Vec<_>>(),
            vec!["a", "b"]
        );
        let mut record = StringRecord::new();
        assert!(reader.read_record(&mut record).unwrap());
        assert_eq!(record.iter().collect::<Vec<_>>(), vec!["1", "2"]);
        assert!(reader.read_record(&mut record).unwrap());
        assert_eq!(record.iter().collect::<Vec<_>>(), vec!["3", "4"]);
        assert!(!reader.read_record(&mut record).unwrap());
    }

    #[test]
    fn empty_input_has_no_records() {
        assert_eq!(read_all(""), Vec::<Vec<String>>::new());
    }

    #[test]
    fn position_line_tracks_records() {
        let mut reader = ReaderBuilder::new()
            .has_headers(false)
            .from_reader("a\nb\nc\n".as_bytes());
        let mut record = StringRecord::new();
        assert_eq!(reader.position().line(), 1);
        reader.read_record(&mut record).unwrap();
        assert_eq!(reader.position().line(), 2);
        reader.read_record(&mut record).unwrap();
        assert_eq!(reader.position().line(), 3);
    }

    #[test]
    fn empty_quoted_field_at_eof_is_a_record() {
        assert_eq!(
            read_all("\"\""),
            vec![vec!["".to_string()]]
        );
    }

    #[test]
    fn trailing_empty_field_without_newline() {
        assert_eq!(
            read_all("a,"),
            vec![vec!["a".to_string(), "".to_string()]]
        );
    }

    #[test]
    fn invalid_utf8_is_an_error() {
        let mut reader = ReaderBuilder::new()
            .has_headers(false)
            .from_reader(&b"a,\xff\xfe\n"[..]);
        let mut record = StringRecord::new();
        assert!(matches!(
            reader.read_record(&mut record),
            Err(Error::Utf8 { .. })
        ));
    }
}
