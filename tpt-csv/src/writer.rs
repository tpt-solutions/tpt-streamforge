use std::io::Write;

use crate::error::Result;

#[derive(Debug, Clone, Copy, Default)]
pub struct WriterBuilder {
    _reserved: (),
}

impl WriterBuilder {
    pub fn new() -> Self {
        WriterBuilder::default()
    }

    /// Accepted for API compatibility with the `csv` crate; this writer
    /// never enforces a fixed field count per record, so this is a no-op.
    pub fn flexible(&mut self, _yes: bool) -> &mut Self {
        self
    }

    pub fn from_writer<W: Write>(&self, writer: W) -> Writer<W> {
        Writer {
            inner: Some(writer),
            buf: Vec::with_capacity(8 * 1024),
        }
    }
}

/// A CSV writer that quotes a field only when it contains a comma, quote,
/// or newline (the common "minimal quoting" style, matching the `csv`
/// crate's default). Buffers a batch of records internally and flushes them
/// to the underlying writer on every call and once more on drop.
pub struct Writer<W: Write> {
    inner: Option<W>,
    buf: Vec<u8>,
}

impl<W: Write> Writer<W> {
    /// Write one record, quoting fields as needed. Accepts anything
    /// producing `&str`-like fields, e.g. `&StringRecord`, `&[String]`, or
    /// any `IntoIterator<Item = &str>`.
    pub fn write_record<I, T>(&mut self, fields: I) -> Result<()>
    where
        I: IntoIterator<Item = T>,
        T: AsRef<str>,
    {
        let mut first = true;
        for field in fields {
            if !first {
                self.buf.push(b',');
            }
            first = false;
            write_field(&mut self.buf, field.as_ref());
        }
        self.buf.push(b'\n');
        self.flush_buf()
    }

    fn flush_buf(&mut self) -> Result<()> {
        if let Some(w) = self.inner.as_mut() {
            w.write_all(&self.buf)?;
        }
        self.buf.clear();
        Ok(())
    }
}

fn write_field(out: &mut Vec<u8>, field: &str) {
    let needs_quoting = field
        .bytes()
        .any(|b| b == b',' || b == b'"' || b == b'\n' || b == b'\r');
    if !needs_quoting {
        out.extend_from_slice(field.as_bytes());
        return;
    }
    out.push(b'"');
    for b in field.bytes() {
        if b == b'"' {
            out.push(b'"');
        }
        out.push(b);
    }
    out.push(b'"');
}

impl<W: Write> Drop for Writer<W> {
    fn drop(&mut self) {
        let _ = self.flush_buf();
        if let Some(mut w) = self.inner.take() {
            let _ = w.flush();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write_all(rows: &[&[&str]]) -> String {
        let mut buf = Vec::new();
        {
            let mut writer = WriterBuilder::new().from_writer(&mut buf);
            for row in rows {
                writer.write_record(row.iter().copied()).unwrap();
            }
        }
        String::from_utf8(buf).unwrap()
    }

    #[test]
    fn plain_fields_are_not_quoted() {
        assert_eq!(write_all(&[&["a", "b", "c"]]), "a,b,c\n");
    }

    #[test]
    fn comma_triggers_quoting() {
        assert_eq!(write_all(&[&["a,b", "c"]]), "\"a,b\",c\n");
    }

    #[test]
    fn embedded_quote_is_doubled() {
        assert_eq!(write_all(&[&["say \"hi\""]]), "\"say \"\"hi\"\"\"\n");
    }

    #[test]
    fn embedded_newline_triggers_quoting() {
        assert_eq!(write_all(&[&["line1\nline2"]]), "\"line1\nline2\"\n");
    }

    #[test]
    fn empty_field_is_fine() {
        assert_eq!(write_all(&[&["a", "", "c"]]), "a,,c\n");
    }

    #[test]
    fn string_record_round_trips_through_writer() {
        use crate::record::StringRecord;
        let mut record = StringRecord::new();
        record.push_field("x");
        record.push_field("y,z");
        let mut buf = Vec::new();
        {
            let mut writer = WriterBuilder::new().from_writer(&mut buf);
            writer.write_record(&record).unwrap();
        }
        assert_eq!(String::from_utf8(buf).unwrap(), "x,\"y,z\"\n");
    }
}
