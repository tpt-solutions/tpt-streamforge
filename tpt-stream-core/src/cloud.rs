//! Shared plumbing for cloud object-storage sources and sinks: format
//! detection from object keys, batch → bytes encoding, and body → batch
//! decoding. Used by `s3`, `gcs`, and `azure` modules.

use crate::error::{Error, Result};
use crate::table::RecordBatch;
use crate::value::Value;

/// On-the-wire data formats understood by cloud sources/sinks. The format is
/// picked from the object key's extension (`.csv`, `.jsonl`/`.ndjson`,
/// `.json`, `.tptcol`); unknown extensions default to CSV.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CloudFormat {
    Csv,
    Jsonl,
    Json,
    Columnar,
}

impl CloudFormat {
    pub fn detect(key: &str) -> Self {
        let ext = key
            .rsplit('.')
            .next()
            .unwrap_or_default()
            .to_ascii_lowercase();
        match ext.as_str() {
            "jsonl" | "ndjson" => CloudFormat::Jsonl,
            "json" => CloudFormat::Json,
            "tptcol" => CloudFormat::Columnar,
            _ => CloudFormat::Csv,
        }
    }
}

/// Incrementally serialize batches into an upload buffer. `encode` is called
/// once per batch in stream order; `finish` emits any closing bytes (the JSON
/// array's `]`).
#[derive(Debug, Clone)]
pub(crate) struct BatchEncoder {
    format: CloudFormat,
    wrote_first: bool,
}

impl BatchEncoder {
    pub(crate) fn new(format: CloudFormat) -> Self {
        BatchEncoder {
            format,
            wrote_first: false,
        }
    }

    pub(crate) fn encode(&mut self, out: &mut Vec<u8>, batch: &RecordBatch) -> Result<()> {
        if batch.num_rows() == 0 && self.wrote_first {
            return Ok(());
        }
        match self.format {
            CloudFormat::Csv => {
                let mut writer = csv::WriterBuilder::new().from_writer(&mut *out);
                let mut record = csv::StringRecord::new();
                if !self.wrote_first {
                    for name in batch.column_names() {
                        record.push_field(name);
                    }
                    writer.write_record(&record).map_err(Error::Csv)?;
                    record.clear();
                    self.wrote_first = true;
                }
                for row in 0..batch.num_rows() {
                    record.clear();
                    for column in batch.columns() {
                        match column.get(row) {
                            Some(Value::Null) => record.push_field(""),
                            Some(v) => record.push_field(&v.to_string()),
                            None => record.push_field(""),
                        };
                    }
                    writer.write_record(&record).map_err(Error::Csv)?;
                }
                // csv::Writer flushes into `out` on drop.
                drop(writer);
            }
            CloudFormat::Jsonl => {
                for row in 0..batch.num_rows() {
                    serde_json::to_writer(
                        &mut *out,
                        &crate::sink::batch_to_json_object(batch, row),
                    )
                    .map_err(Error::Json)?;
                    out.push(b'\n');
                }
                self.wrote_first = true;
            }
            CloudFormat::Json => {
                if !self.wrote_first {
                    out.extend_from_slice(b"[");
                } else {
                    out.extend_from_slice(b",");
                }
                for row in 0..batch.num_rows() {
                    serde_json::to_writer(
                        &mut *out,
                        &crate::sink::batch_to_json_object(batch, row),
                    )
                    .map_err(Error::Json)?;
                    if row + 1 < batch.num_rows() {
                        out.extend_from_slice(b",");
                    }
                }
                self.wrote_first = true;
            }
            CloudFormat::Columnar => {
                let bytes = tpt_stream_columnar::format::encode_batch(batch, false)
                    .map_err(|e| Error::Other(e.to_string()))?;
                out.extend_from_slice(&bytes);
                self.wrote_first = true;
            }
        }
        Ok(())
    }

    pub(crate) fn finish(&mut self, out: &mut Vec<u8>) {
        match self.format {
            CloudFormat::Json => {
                if self.wrote_first {
                    out.extend_from_slice(b"]");
                } else {
                    out.extend_from_slice(b"[]");
                }
            }
            CloudFormat::Csv if !self.wrote_first => {
                // Header-only stream: nothing to emit without a schema.
            }
            _ => {}
        }
    }
}

/// Decode an object body into batches pushed to `tx`, `chunk_rows` rows per
/// batch. Runs on the source's reader thread.
#[cfg(feature = "async")]
pub(crate) fn decode_object_stream(
    tx: &crate::source::BatchTx,
    body: Box<dyn std::io::BufRead + Send>,
    format: CloudFormat,
    chunk_rows: usize,
    policy: &crate::source::ErrorPolicy,
) {
    match format {
        CloudFormat::Csv => crate::source::csv_read_stream(tx, body, chunk_rows, policy),
        CloudFormat::Jsonl => crate::source::jsonl_read_stream(tx, body, chunk_rows, policy),
        CloudFormat::Json => crate::source::json_array_read_stream(tx, body, chunk_rows),
        CloudFormat::Columnar => crate::source::columnar_read_stream(tx, body),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn format_detect_by_extension() {
        assert_eq!(CloudFormat::detect("data/file.csv"), CloudFormat::Csv);
        assert_eq!(CloudFormat::detect("a/b/events.jsonl"), CloudFormat::Jsonl);
        assert_eq!(CloudFormat::detect("a/b/events.ndjson"), CloudFormat::Jsonl);
        assert_eq!(CloudFormat::detect("a/b/data.json"), CloudFormat::Json);
        assert_eq!(
            CloudFormat::detect("a/b/data.tptcol"),
            CloudFormat::Columnar
        );
        assert_eq!(CloudFormat::detect("a/b/noextension"), CloudFormat::Csv);
        assert_eq!(CloudFormat::detect("a/b/data.JSON"), CloudFormat::Json);
    }

    #[test]
    fn csv_encoder_writes_header_once() {
        let batches = crate::source::csv_to_batches("id,name\n1,a\n2,b\n", 1).unwrap();
        let mut encoder = BatchEncoder::new(CloudFormat::Csv);
        let mut out = Vec::new();
        encoder.encode(&mut out, &batches[0]).unwrap();
        encoder.encode(&mut out, &batches[1]).unwrap();
        encoder.finish(&mut out);
        assert_eq!(String::from_utf8(out).unwrap(), "id,name\n1,a\n2,b\n");
    }

    #[test]
    fn json_array_encoder_wraps() {
        let batches = crate::source::csv_to_batches("id\n1\n2\n", 10).unwrap();
        let mut encoder = BatchEncoder::new(CloudFormat::Json);
        let mut out = Vec::new();
        encoder.encode(&mut out, &batches[0]).unwrap();
        encoder.finish(&mut out);
        assert_eq!(String::from_utf8(out).unwrap(), r#"[{"id":1},{"id":2}]"#);
    }

    #[test]
    fn json_array_encoder_empty_stream() {
        let mut encoder = BatchEncoder::new(CloudFormat::Json);
        let mut out = Vec::new();
        encoder.finish(&mut out);
        assert_eq!(String::from_utf8(out).unwrap(), "[]");
    }
}
