use crate::pipeline::{Error, Result};
use crate::table::RecordBatch;
use crate::value::Value;
use tokio::io::AsyncWriteExt;

#[async_trait::async_trait]
pub trait Sink: Send {
    async fn write_batch(&mut self, batch: &RecordBatch) -> Result<()>;
    async fn finish(&mut self) -> Result<()>;

    /// Bytes written so far, when the sink can know (0 otherwise). Used by
    /// pipeline telemetry after `finish()`.
    fn bytes_out(&self) -> u64 {
        0
    }
}

// ---------------------------------------------------------------------------
// CSV
// ---------------------------------------------------------------------------

pub struct CsvSink {
    path: String,
    file: Option<tokio::fs::File>,
    buffer: Vec<u8>,
    wrote_header: bool,
    bytes_out: u64,
}

impl CsvSink {
    pub fn open(path: impl Into<String>) -> Self {
        CsvSink {
            path: path.into(),
            file: None,
            buffer: Vec::with_capacity(64 * 1024),
            wrote_header: false,
            bytes_out: 0,
        }
    }

    pub fn bytes_out(&self) -> u64 {
        self.bytes_out
    }

    async fn ensure_file(&mut self) -> Result<()> {
        if self.file.is_none() {
            self.file = Some(tokio::fs::File::create(&self.path).await?);
        }
        Ok(())
    }

    async fn flush_buffer(&mut self) -> Result<()> {
        if self.buffer.is_empty() {
            return Ok(());
        }
        let file = self.file.as_mut().expect("file set");
        file.write_all(&self.buffer).await?;
        self.bytes_out += self.buffer.len() as u64;
        self.buffer.clear();
        Ok(())
    }
}

impl std::fmt::Debug for CsvSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CsvSink")
            .field("path", &self.path)
            .field("bytes_out", &self.bytes_out)
            .finish()
    }
}

#[async_trait::async_trait]
impl Sink for CsvSink {
    async fn write_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        self.ensure_file().await?;
        let mut writer = tpt_csv::WriterBuilder::new().from_writer(&mut self.buffer);

        let mut record = tpt_csv::StringRecord::new();
        if !self.wrote_header {
            for name in batch.column_names() {
                record.push_field(name);
            }
            writer.write_record(&record).map_err(Error::Csv)?;
            record.clear();
            self.wrote_header = true;
        }

        for row in 0..batch.num_rows() {
            record.clear();
            for column in batch.columns() {
                match column.get(row) {
                    Some(Value::Null) => record.push_field(""),
                    Some(v) => record.push_field(&v.to_string()),
                    None => record.push_field(""),
                }
            }
            writer.write_record(&record).map_err(Error::Csv)?;
        }

        // tpt_csv::Writer flushes into the underlying &mut buffer on drop.
        drop(writer);

        // If roughly 1MB of CSV has accumulated, flush to disk.
        if self.buffer.len() >= 1024 * 1024 {
            self.flush_buffer().await?;
        }
        Ok(())
    }

    async fn finish(&mut self) -> Result<()> {
        self.ensure_file().await?;
        self.flush_buffer().await?;
        if let Some(file) = self.file.as_mut() {
            file.flush().await?;
            file.sync_all().await?;
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// JSONL
// ---------------------------------------------------------------------------

pub struct JsonlSink {
    path: String,
    file: Option<tokio::fs::File>,
    buffer: Vec<u8>,
    bytes_out: u64,
}

impl JsonlSink {
    pub fn open(path: impl Into<String>) -> Self {
        JsonlSink {
            path: path.into(),
            file: None,
            buffer: Vec::with_capacity(64 * 1024),
            bytes_out: 0,
        }
    }

    pub fn bytes_out(&self) -> u64 {
        self.bytes_out
    }

    async fn ensure_file(&mut self) -> Result<()> {
        if self.file.is_none() {
            self.file = Some(tokio::fs::File::create(&self.path).await?);
        }
        Ok(())
    }

    async fn flush_buffer(&mut self) -> Result<()> {
        if self.buffer.is_empty() {
            return Ok(());
        }
        let file = self.file.as_mut().expect("file set");
        file.write_all(&self.buffer).await?;
        self.bytes_out += self.buffer.len() as u64;
        self.buffer.clear();
        Ok(())
    }
}

impl std::fmt::Debug for JsonlSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JsonlSink")
            .field("path", &self.path)
            .field("bytes_out", &self.bytes_out)
            .finish()
    }
}

pub(crate) fn value_to_json(value: &Value) -> serde_json::Value {
    match value {
        Value::Int32(v) => serde_json::Value::from(*v),
        Value::Int64(v) => serde_json::Value::from(*v),
        Value::Float32(v) => serde_json::Number::from_f64(*v as f64)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        Value::Float64(v) => serde_json::Number::from_f64(*v)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        Value::Utf8(s) => {
            if s.is_empty() {
                serde_json::Value::Null
            } else {
                serde_json::Value::String(s.clone())
            }
        }
        Value::Bool(b) => serde_json::Value::Bool(*b),
        // Dates/timestamps serialize as ISO strings.
        Value::Date(_) | Value::Timestamp(_) => serde_json::Value::String(value.to_string()),
        Value::Null => serde_json::Value::Null,
    }
}

pub(crate) fn batch_to_json_object(
    batch: &RecordBatch,
    row: usize,
) -> serde_json::Map<String, serde_json::Value> {
    let mut obj = serde_json::Map::with_capacity(batch.num_columns());
    for column in batch.columns() {
        let value = column.get(row).unwrap_or(Value::Null);
        obj.insert(column.name().to_string(), value_to_json(&value));
    }
    obj
}

#[async_trait::async_trait]
impl Sink for JsonlSink {
    async fn write_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        self.ensure_file().await?;
        for row in 0..batch.num_rows() {
            serde_json::to_writer(&mut self.buffer, &batch_to_json_object(batch, row))
                .map_err(Error::Json)?;
            self.buffer.push(b'\n');
        }
        if self.buffer.len() >= 1024 * 1024 {
            self.flush_buffer().await?;
        }
        Ok(())
    }

    async fn finish(&mut self) -> Result<()> {
        self.ensure_file().await?;
        self.flush_buffer().await?;
        if let Some(file) = self.file.as_mut() {
            file.flush().await?;
            file.sync_all().await?;
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// JSON array
// ---------------------------------------------------------------------------

pub struct JsonArraySink {
    path: String,
    pretty: bool,
    file: Option<tokio::fs::File>,
    buffer: Vec<u8>,
    wrote_first: bool,
    bytes_out: u64,
}

impl JsonArraySink {
    pub fn open(path: impl Into<String>, pretty: bool) -> Self {
        JsonArraySink {
            path: path.into(),
            pretty,
            file: None,
            buffer: Vec::with_capacity(64 * 1024),
            wrote_first: false,
            bytes_out: 0,
        }
    }

    pub fn bytes_out(&self) -> u64 {
        self.bytes_out
    }

    async fn ensure_file(&mut self) -> Result<()> {
        if self.file.is_none() {
            self.file = Some(tokio::fs::File::create(&self.path).await?);
        }
        Ok(())
    }

    async fn flush_buffer(&mut self) -> Result<()> {
        if self.buffer.is_empty() {
            return Ok(());
        }
        let file = self.file.as_mut().expect("file set");
        file.write_all(&self.buffer).await?;
        self.bytes_out += self.buffer.len() as u64;
        self.buffer.clear();
        Ok(())
    }
}

impl std::fmt::Debug for JsonArraySink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JsonArraySink")
            .field("path", &self.path)
            .field("pretty", &self.pretty)
            .field("bytes_out", &self.bytes_out)
            .finish()
    }
}

#[async_trait::async_trait]
impl Sink for JsonArraySink {
    async fn write_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        self.ensure_file().await?;
        if !self.wrote_first {
            self.buffer.extend_from_slice(b"[");
            self.wrote_first = true;
        } else {
            self.buffer.extend_from_slice(b",");
        }
        if self.pretty {
            self.buffer.extend_from_slice(b"\n");
        }

        for row in 0..batch.num_rows() {
            if self.pretty {
                self.buffer.extend_from_slice(b"  ");
            }
            serde_json::to_writer(&mut self.buffer, &batch_to_json_object(batch, row))
                .map_err(Error::Json)?;
            if self.pretty {
                self.buffer.extend_from_slice(b"\n");
            }
            if row + 1 < batch.num_rows() {
                self.buffer.extend_from_slice(b",");
                if self.pretty {
                    self.buffer.push(b'\n');
                }
            }
            if self.buffer.len() >= 1024 * 1024 {
                self.flush_buffer().await?;
            }
        }
        Ok(())
    }

    async fn finish(&mut self) -> Result<()> {
        self.ensure_file().await?;
        if !self.wrote_first {
            // Empty stream: emit an empty array.
            self.buffer.extend_from_slice(b"[]");
        } else if self.pretty {
            self.buffer.extend_from_slice(b"\n]");
        } else {
            self.buffer.extend_from_slice(b"]");
        }
        self.flush_buffer().await?;
        if let Some(file) = self.file.as_mut() {
            file.flush().await?;
            file.sync_all().await?;
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Columnar (.tptcol)
// ---------------------------------------------------------------------------

pub struct ColumnarSink {
    path: String,
    file: Option<tokio::fs::File>,
    buffer: Vec<u8>,
    use_zstd: bool,
    bytes_out: u64,
    batches: u64,
}

impl ColumnarSink {
    pub fn open(path: impl Into<String>, use_zstd: bool) -> Self {
        ColumnarSink {
            path: path.into(),
            file: None,
            buffer: Vec::with_capacity(1024 * 1024),
            use_zstd,
            bytes_out: 0,
            batches: 0,
        }
    }

    pub fn bytes_out(&self) -> u64 {
        self.bytes_out
    }

    async fn ensure_file(&mut self) -> Result<()> {
        if self.file.is_none() {
            self.file = Some(tokio::fs::File::create(&self.path).await?);
        }
        Ok(())
    }

    async fn flush_buffer(&mut self) -> Result<()> {
        if self.buffer.is_empty() {
            return Ok(());
        }
        let file = self.file.as_mut().expect("file set");
        file.write_all(&self.buffer).await?;
        self.bytes_out += self.buffer.len() as u64;
        self.buffer.clear();
        Ok(())
    }
}

impl std::fmt::Debug for ColumnarSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ColumnarSink")
            .field("path", &self.path)
            .field("use_zstd", &self.use_zstd)
            .field("bytes_out", &self.bytes_out)
            .field("batches", &self.batches)
            .finish()
    }
}

#[async_trait::async_trait]
impl Sink for ColumnarSink {
    async fn write_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        self.ensure_file().await?;
        let bytes = tpt_stream_columnar::format::encode_batch(batch, self.use_zstd)
            .map_err(|e| Error::Other(e.to_string()))?;
        self.buffer.extend_from_slice(&bytes);
        if self.buffer.len() >= 1024 * 1024 {
            self.flush_buffer().await?;
        }
        self.batches += 1;
        Ok(())
    }

    async fn finish(&mut self) -> Result<()> {
        self.ensure_file().await?;
        self.flush_buffer().await?;
        if let Some(file) = self.file.as_mut() {
            file.flush().await?;
            file.sync_all().await?;
        }
        Ok(())
    }
}
