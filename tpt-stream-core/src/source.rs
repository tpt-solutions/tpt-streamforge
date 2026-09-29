use crate::column::Column;
use crate::error::{Error, Result};
use crate::table::RecordBatch;
use crate::value::{DataType, Value};
#[cfg(feature = "async")]
use crate::DEFAULT_CHUNK_ROWS;
#[cfg(feature = "async")]
use rayon::prelude::*;
#[cfg(feature = "async")]
use std::io::{BufRead, Write};
#[cfg(feature = "async")]
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tpt_csv::columnar::{ColumnarChunk, ColumnarReader, RaggedRowPolicy};

#[cfg(feature = "async")]
type BatchResult = std::result::Result<RecordBatch, Error>;

/// Channel a reader thread pushes batches through (used by blocking sources:
/// CSV/JSON files, SQLite, PostgreSQL, cloud object storage).
#[cfg(feature = "async")]
pub type BatchTx = UnboundedSender<BatchResult>;

/// How a source reacts to rows it cannot parse (field-count mismatches in
/// CSV, undecodable JSONL lines). Type errors inside a well-formed row are
/// always fatal.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum ErrorPolicy {
    /// Abort with a line-numbered parse error (default).
    #[default]
    Strict,
    /// Drop malformed rows and continue.
    Skip,
    /// Drop malformed rows and append each raw row to `path` (CSV rows as
    /// CSV, JSONL lines verbatim) so nothing is lost silently.
    Quarantine(String),
}

/// Open a (possibly gzip-compressed) file as a buffered reader. With the
/// `gzip` feature, names ending in `.gz` are transparently decompressed.
#[cfg(feature = "async")]
pub(crate) fn open_file_buffered(path: &str) -> std::io::Result<Box<dyn BufRead + Send>> {
    let file = std::fs::File::open(path)?;
    #[cfg(feature = "gzip")]
    if path.ends_with(".gz") {
        use flate2::read::MultiGzDecoder;
        return Ok(Box::new(std::io::BufReader::with_capacity(
            64 * 1024,
            MultiGzDecoder::new(file),
        )));
    }
    #[cfg(not(feature = "gzip"))]
    let _ = path;
    Ok(Box::new(std::io::BufReader::with_capacity(64 * 1024, file)))
}

#[cfg(feature = "async")]
#[async_trait::async_trait]
pub trait Source: Send {
    async fn next_batch(&mut self) -> Result<Option<RecordBatch>>;
}

/// Streams batches from a dedicated reader thread into an async channel, so file
/// I/O + parsing never block the async runtime. Memory stays bounded by the chunk
/// size (the channel holds at most a couple in-flight batches).
/// Other blocking sources (SQLite) reuse this plumbing.
#[cfg(feature = "async")]
pub(crate) struct StreamingReader {
    pub(crate) rx: UnboundedReceiver<BatchResult>,
    pub(crate) done: bool,
}

#[cfg(feature = "async")]
impl StreamingReader {
    pub(crate) fn spawn<F>(f: F) -> (Self, std::thread::JoinHandle<()>)
    where
        F: FnOnce(&UnboundedSender<BatchResult>) + Send + 'static,
    {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<BatchResult>();
        let handle = std::thread::spawn(move || f(&tx));
        (StreamingReader { rx, done: false }, handle)
    }

    pub(crate) async fn next(&mut self) -> Option<BatchResult> {
        self.rx.recv().await
    }
}

// ---------------------------------------------------------------------------
// Shared source plumbing
// ---------------------------------------------------------------------------

/// Implements `Source` for a reader type. All reader types expose:
/// - `reader: StreamingReader`
/// - `_handle: Option<JoinHandle<()>>` (kept alive so batches keep flowing)
/// - `pending: Option<BatchResult>` (never used; kept for symmetry, see below)
#[cfg(feature = "async")]
macro_rules! impl_source {
    ($type:ty) => {
        #[async_trait::async_trait]
        impl Source for $type {
            async fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
                if self.reader.done {
                    return Ok(None);
                }
                if let Some(result) = self.pending.take() {
                    self.reader.done = true;
                    return match result {
                        Ok(b) => Ok(Some(b)),
                        Err(e) => Err(e),
                    };
                }
                match self.reader.next().await {
                    Some(Ok(batch)) => Ok(Some(batch)),
                    Some(Err(e)) => {
                        self.reader.done = true;
                        Err(e)
                    }
                    None => {
                        self.reader.done = true;
                        Ok(None)
                    }
                }
            }
        }
    };
}

// ---------------------------------------------------------------------------
// CSV
// ---------------------------------------------------------------------------

/// Streams a CSV file (`.gz`-compressed files are transparently decompressed
/// with the `gzip` feature). The reader thread spawns on the first
/// `next_batch`, so the `with_*` builders can be chained after `open`.
#[cfg(feature = "async")]
pub struct CsvSource {
    reader: Option<StreamingReader>,
    pending: Option<BatchResult>,
    path: String,
    chunk_rows: usize,
    policy: ErrorPolicy,
}

#[cfg(feature = "async")]
impl CsvSource {
    pub fn open(path: impl Into<String>) -> Self {
        CsvSource::open_with_chunk_size(path, DEFAULT_CHUNK_ROWS)
    }

    pub fn open_with_chunk_size(path: impl Into<String>, chunk_rows: usize) -> Self {
        CsvSource {
            reader: None,
            pending: None,
            path: path.into(),
            chunk_rows,
            policy: ErrorPolicy::default(),
        }
    }

    pub fn with_chunk_size(mut self, rows: usize) -> Self {
        self.chunk_rows = rows;
        self
    }

    /// Set how malformed rows (field-count mismatches) are handled.
    pub fn with_error_policy(mut self, policy: ErrorPolicy) -> Self {
        self.policy = policy;
        self
    }

    fn spawn_reader(&mut self) -> &mut StreamingReader {
        if self.reader.is_none() {
            let path = self.path.clone();
            let chunk_rows = self.chunk_rows;
            let policy = self.policy.clone();
            let (reader, handle) =
                StreamingReader::spawn(move |tx| csv_read_loop(tx, &path, chunk_rows, &policy));
            let _ = handle;
            self.reader = Some(reader);
        }
        self.reader.as_mut().expect("just spawned")
    }
}

#[cfg(feature = "async")]
fn csv_read_loop(
    tx: &UnboundedSender<BatchResult>,
    path: &str,
    chunk_rows: usize,
    policy: &ErrorPolicy,
) {
    match open_file_buffered(path) {
        Ok(reader) => csv_read_stream(tx, reader, chunk_rows, policy),
        Err(e) => {
            let _ = tx.send(Err(Error::Io(e)));
        }
    }
}

/// Stream-parse CSV from any reader (file, cloud object body, HTTP body)
/// into the channel, `chunk_rows` rows per batch. Ragged rows (fewer or
/// more fields than the header) are fatal under [`ErrorPolicy::Strict`],
/// dropped under `Skip`, and captured to a file under `Quarantine`.
#[cfg(feature = "async")]
pub(crate) fn csv_read_stream(
    tx: &UnboundedSender<BatchResult>,
    reader: Box<dyn BufRead + Send>,
    chunk_rows: usize,
    policy: &ErrorPolicy,
) {
    let mut columnar = match ColumnarReader::from_reader(reader) {
        Ok(cr) => cr,
        Err(e) => {
            let _ = tx.send(Err(Error::Csv(e)));
            return;
        }
    };
    let headers: Vec<String> = columnar.headers().iter().map(|s| s.to_string()).collect();
    if headers.is_empty() {
        let _ = tx.send(Err(Error::Schema("CSV file has no header row".into())));
        return;
    }
    let mut quarantine = QuarantineWriter::csv(&headers, policy);
    let mut schema: Option<Vec<DataType>> = None;
    let mut chunk = ColumnarChunk::new(columnar.num_columns(), chunk_rows);

    loop {
        let got = {
            let mut quarantine_fn =
                |fields: &[&str]| quarantine.write_record(fields.iter().copied());
            let mut ragged = match policy {
                ErrorPolicy::Strict => RaggedRowPolicy::Strict,
                ErrorPolicy::Skip => RaggedRowPolicy::Skip,
                ErrorPolicy::Quarantine(_) => RaggedRowPolicy::Quarantine(&mut quarantine_fn),
            };
            match columnar.read_chunk_into(chunk_rows, &mut ragged, &mut chunk) {
                Ok(g) => g,
                Err(e) => {
                    let _ = tx.send(Err(Error::Csv(e)));
                    return;
                }
            }
        };
        if !got {
            break;
        }
        let batch = match build_csv_batch_from_chunk(&headers, &mut schema, &chunk) {
            Ok(b) => b,
            Err(e) => {
                let _ = tx.send(Err(e));
                return;
            }
        };
        if tx.send(Ok(batch)).is_err() {
            return;
        }
    }
}

/// Destination for malformed rows under [`ErrorPolicy::Quarantine`].
#[cfg(feature = "async")]
enum QuarantineWriter {
    None,
    /// Raw row written as CSV (header written on creation).
    Csv(Box<tpt_csv::Writer<std::fs::File>>),
    /// Raw line for text formats (JSONL).
    Lines(Box<std::fs::File>),
}

#[cfg(feature = "async")]
impl QuarantineWriter {
    fn csv(headers: &[String], policy: &ErrorPolicy) -> Self {
        let ErrorPolicy::Quarantine(path) = policy else {
            return QuarantineWriter::None;
        };
        match std::fs::File::create(path) {
            Ok(file) => {
                // Malformed rows may have any field count: flexible(true).
                let mut writer = tpt_csv::WriterBuilder::new()
                    .flexible(true)
                    .from_writer(file);
                let _ = writer.write_record(headers);
                QuarantineWriter::Csv(Box::new(writer))
            }
            // Quarantine targets are validated when the first malformed row
            // arrives; a bad path surfaces there as an I/O error.
            Err(_) => QuarantineWriter::None,
        }
    }

    fn write_record<'a, I>(&mut self, fields: I) -> std::io::Result<()>
    where
        I: IntoIterator<Item = &'a str>,
    {
        match self {
            QuarantineWriter::Csv(writer) => writer
                .write_record(fields)
                .map_err(|e| std::io::Error::other(e.to_string())),
            _ => Ok(()),
        }
    }

    fn write_line(&mut self, line: &str) -> std::io::Result<()> {
        match self {
            QuarantineWriter::Lines(file) => {
                file.write_all(line.as_bytes())?;
                file.write_all(b"\n")
            }
            _ => Ok(()),
        }
    }
}

#[cfg(feature = "async")]
impl std::fmt::Debug for CsvSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CsvSource")
            .field("chunk_rows", &self.chunk_rows)
            .finish()
    }
}

// ---------------------------------------------------------------------------
// Shared CSV/JSON helpers
// ---------------------------------------------------------------------------

/// Read an entire CSV file into batches (used to load join build relations).
#[cfg(feature = "async")]
pub(crate) fn read_csv_batches(path: &str, chunk_rows: usize) -> Result<Vec<RecordBatch>> {
    // Whole-buffer inputs are split at record boundaries and parsed in
    // parallel; the bounded-memory streaming `CsvSource` stays sequential.
    if let Some(buf) = slurp_parallelizable(path) {
        return csv_bytes_to_batches(&buf, chunk_rows);
    }
    let reader = open_file_buffered(path).map_err(Error::Io)?;
    csv_reader_to_batches(reader, chunk_rows)
}

/// Read `path` fully into memory when it is worth parsing in parallel:
/// uncompressed, at least [`PARALLEL_CSV_MIN_BYTES`], and not gzip-compressed
/// (a compressed file must stay on the streaming decompression path).
///
/// `None` means "use the sequential streaming path" — either because the file
/// is small, because it is compressed, or because it cannot be stat'ed. The
/// caller then reports any I/O error from the normal open path, so a missing
/// file still surfaces as `Error::Io` with its real cause.
#[cfg(feature = "async")]
fn slurp_parallelizable(path: &str) -> Option<Vec<u8>> {
    if path.ends_with(".gz") {
        return None;
    }
    let len = std::fs::metadata(path).ok()?.len();
    if len < PARALLEL_CSV_MIN_BYTES as u64 {
        return None;
    }
    std::fs::read(path).ok()
}

/// Whole-buffer CSV inputs at least this large are parsed across the rayon
/// pool. Below it, the thread hand-off costs more than the scan it parallelizes.
#[cfg(feature = "async")]
pub const PARALLEL_CSV_MIN_BYTES: usize = 1 << 20;

/// Parse in-memory CSV text into `RecordBatch`es of `chunk_rows` rows each.
/// Exposed for language wrappers that do not have a filesystem (WASM) and for
/// the FFI tests.
///
/// Inputs of at least [`PARALLEL_CSV_MIN_BYTES`] are split at record boundaries
/// and parsed concurrently, which yields exactly the same batches as the
/// sequential path.
pub fn csv_to_batches(text: &str, chunk_rows: usize) -> Result<Vec<RecordBatch>> {
    #[cfg(feature = "async")]
    if text.len() >= PARALLEL_CSV_MIN_BYTES {
        return csv_bytes_to_batches(text.as_bytes(), chunk_rows);
    }
    csv_reader_to_batches(text.as_bytes(), chunk_rows)
}

/// Parse in-memory CSV text on the calling thread, never splitting the input.
///
/// This is what [`csv_to_batches`] does for inputs below
/// [`PARALLEL_CSV_MIN_BYTES`], exposed so benchmarks can compare the two paths
/// on identical input.
pub fn csv_to_batches_sequential(text: &str, chunk_rows: usize) -> Result<Vec<RecordBatch>> {
    csv_reader_to_batches(text.as_bytes(), chunk_rows)
}

/// Parse a whole in-memory CSV buffer, splitting it at record boundaries and
/// parsing the slices across the rayon pool.
///
/// The header is parsed once, sequentially. Each slice is then handed to
/// [`ColumnarReader::from_slice`] with the header's column count, so slices
/// need no header of their own. Batches are built in slice order, which keeps
/// type inference identical to the sequential path (the first non-empty batch
/// fixes the schema) and makes an error in a later slice surface only after
/// earlier slices have been processed — the same first-failure behavior a
/// sequential parse has.
#[cfg(feature = "async")]
fn csv_bytes_to_batches(buf: &[u8], chunk_rows: usize) -> Result<Vec<RecordBatch>> {
    let header_reader = ColumnarReader::from_reader(buf)?;
    let headers: Vec<String> = header_reader.headers().iter().map(str::to_string).collect();
    if headers.is_empty() {
        return Err(Error::Schema("CSV file has no header row".into()));
    }
    // The header occupies record 1, so data record `n` is document record
    // `n + 1`.
    let Some(data_start) = tpt_csv::first_record_end(buf) else {
        return Ok(Vec::new());
    };
    let boundaries = tpt_csv::find_chunk_boundaries(&buf[data_start..], chunk_rows);
    if boundaries.is_empty() {
        // A single slice: nothing to gain from the parallel path.
        return csv_reader_to_batches(buf, chunk_rows);
    }
    let num_columns = header_reader.num_columns();

    // Each entry is (slice bytes, document record number of its first record).
    let mut ranges: Vec<(&[u8], u64)> = Vec::with_capacity(boundaries.len() + 1);
    let mut start = data_start;
    let mut next_record = 2u64;
    for boundary in &boundaries {
        let end = data_start + boundary.offset;
        ranges.push((&buf[start..end], next_record));
        start = end;
        next_record = boundary.record + 1;
    }
    ranges.push((&buf[start..], next_record));

    let chunks: Vec<Result<ColumnarChunk>> = ranges
        .par_iter()
        .map(|(slice, first_record)| {
            let mut reader = ColumnarReader::from_slice(*slice, num_columns, *first_record);
            let mut policy = RaggedRowPolicy::Strict;
            let mut chunk = ColumnarChunk::new(num_columns, chunk_rows);
            reader.read_chunk_into(chunk_rows, &mut policy, &mut chunk)?;
            Ok(chunk)
        })
        .collect();

    // Build batches in order so schema inference sees the first rows first.
    let mut schema: Option<Vec<DataType>> = None;
    let mut batches = Vec::with_capacity(chunks.len());
    for chunk in chunks {
        batches.push(build_csv_batch_from_chunk(&headers, &mut schema, &chunk?)?);
    }
    Ok(batches)
}

fn csv_reader_to_batches<R: std::io::Read>(
    reader: R,
    chunk_rows: usize,
) -> Result<Vec<RecordBatch>> {
    let mut columnar = ColumnarReader::from_reader(reader)?;
    let headers: Vec<String> = columnar.headers().iter().map(|s| s.to_string()).collect();
    if headers.is_empty() {
        return Err(Error::Schema("CSV file has no header row".into()));
    }
    let mut schema: Option<Vec<DataType>> = None;
    let mut chunk = ColumnarChunk::new(columnar.num_columns(), chunk_rows);
    let mut batches = Vec::new();
    let mut policy = RaggedRowPolicy::Strict;
    loop {
        let got = columnar.read_chunk_into(chunk_rows, &mut policy, &mut chunk)?;
        if !got {
            break;
        }
        batches.push(build_csv_batch_from_chunk(&headers, &mut schema, &chunk)?);
    }
    Ok(batches)
}

pub(crate) fn fits(data_type: DataType, cell: &str) -> bool {
    parse_cell(cell, data_type).is_ok()
}

pub(crate) fn escalate(current: DataType, cell: &str) -> DataType {
    const ORDER: [DataType; 7] = [
        DataType::Bool,
        DataType::Int32,
        DataType::Int64,
        DataType::Float64,
        DataType::Date,
        DataType::Timestamp,
        DataType::Utf8,
    ];
    let start = ORDER.iter().position(|d| *d == current);
    for candidate in ORDER.iter().skip(start.map_or(0, |i| i + 1)) {
        if parse_cell(cell, *candidate).is_ok() {
            return *candidate;
        }
    }
    DataType::Utf8
}

pub(crate) fn parse_cell(cell: &str, data_type: DataType) -> std::result::Result<Value, ()> {
    use tpt_stream_columnar::value::{parse_date, parse_timestamp};
    match data_type {
        DataType::Int32 => cell.parse::<i32>().map(Value::Int32).map_err(|_| ()),
        DataType::Int64 => cell.parse::<i64>().map(Value::Int64).map_err(|_| ()),
        DataType::Float32 => cell.parse::<f32>().map(Value::Float32).map_err(|_| ()),
        DataType::Float64 => cell.parse::<f64>().map(Value::Float64).map_err(|_| ()),
        DataType::Bool => match cell.to_ascii_lowercase().as_str() {
            "true" => Ok(Value::Bool(true)),
            "false" => Ok(Value::Bool(false)),
            _ => Err(()),
        },
        DataType::Date => parse_date(cell).map(Value::Date).ok_or(()),
        DataType::Timestamp => parse_timestamp(cell).map(Value::Timestamp).ok_or(()),
        DataType::Utf8 => Ok(Value::Utf8(cell.to_string())),
    }
}

pub(crate) fn infer_types_columnar(chunk: &ColumnarChunk) -> Vec<DataType> {
    let mut types = vec![DataType::Bool; chunk.num_columns];
    for (col, current) in types.iter_mut().enumerate() {
        for cell in chunk.column_bytes(col) {
            let cell = cell.trim();
            if cell.is_empty() {
                continue;
            }
            if fits(*current, cell) {
                continue;
            }
            *current = escalate(*current, cell);
        }
    }
    types
}

pub(crate) fn build_csv_batch_from_chunk(
    headers: &[String],
    schema: &mut Option<Vec<DataType>>,
    chunk: &ColumnarChunk,
) -> Result<RecordBatch> {
    let resolved = match schema {
        Some(s) => s.clone(),
        None => {
            let inferred = infer_types_columnar(chunk);
            *schema = Some(inferred.clone());
            inferred
        }
    };
    // Columns are independent: build them in parallel for throughput on hosts
    // with rayon (async feature), or sequentially on reduced targets (wasm).
    #[cfg(feature = "async")]
    let columns = (0..chunk.num_columns)
        .into_par_iter()
        .map(|i| {
            build_csv_column_from_iter(
                headers[i].clone(),
                resolved[i],
                chunk.row_count,
                chunk.column_bytes(i),
            )
        })
        .collect::<Result<Vec<_>>>()?;
    #[cfg(not(feature = "async"))]
    let columns = (0..chunk.num_columns)
        .map(|i| {
            build_csv_column_from_iter(
                headers[i].clone(),
                resolved[i],
                chunk.row_count,
                chunk.column_bytes(i),
            )
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(RecordBatch::new(columns))
}

fn build_csv_column_from_iter<'a>(
    name: String,
    data_type: DataType,
    row_count: usize,
    fields: impl Iterator<Item = &'a str>,
) -> Result<Column> {
    let mut col = Column::new(name, data_type, row_count);
    for (col_index, cell) in fields.enumerate() {
        let cell = cell.trim();
        if cell.is_empty() {
            col.push(Value::Null);
        } else {
            col.push(parse_cell(cell, data_type).map_err(|_| {
                Error::Schema(format!(
                    "cannot parse {cell:?} as {data_type} in column {col_index}"
                ))
            })?);
        }
    }
    Ok(col)
}

// ---------------------------------------------------------------------------
// JSONL
// ---------------------------------------------------------------------------

/// Streams newline-delimited JSON (`.gz` supported with the `gzip`
/// feature). The reader thread spawns on the first `next_batch`, so the
/// `with_*` builders can be chained after `open`.
#[cfg(feature = "async")]
pub struct JsonlSource {
    reader: Option<StreamingReader>,
    pending: Option<BatchResult>,
    path: String,
    chunk_rows: usize,
    policy: ErrorPolicy,
}

#[cfg(feature = "async")]
impl JsonlSource {
    pub fn open(path: impl Into<String>) -> Self {
        JsonlSource::open_with_chunk_size(path, DEFAULT_CHUNK_ROWS)
    }

    pub fn open_with_chunk_size(path: impl Into<String>, chunk_rows: usize) -> Self {
        JsonlSource {
            reader: None,
            pending: None,
            path: path.into(),
            chunk_rows,
            policy: ErrorPolicy::default(),
        }
    }

    pub fn with_chunk_size(mut self, rows: usize) -> Self {
        self.chunk_rows = rows;
        self
    }

    /// Set how undecodable lines are handled.
    pub fn with_error_policy(mut self, policy: ErrorPolicy) -> Self {
        self.policy = policy;
        self
    }

    fn spawn_reader(&mut self) -> &mut StreamingReader {
        if self.reader.is_none() {
            let path = self.path.clone();
            let chunk_rows = self.chunk_rows;
            let policy = self.policy.clone();
            let (reader, handle) =
                StreamingReader::spawn(move |tx| jsonl_read_loop(tx, &path, chunk_rows, &policy));
            let _ = handle;
            self.reader = Some(reader);
        }
        self.reader.as_mut().expect("just spawned")
    }
}

#[cfg(feature = "async")]
fn jsonl_read_loop(
    tx: &UnboundedSender<BatchResult>,
    path: &str,
    chunk_rows: usize,
    policy: &ErrorPolicy,
) {
    match open_file_buffered(path) {
        Ok(reader) => jsonl_read_stream(tx, reader, chunk_rows, policy),
        Err(e) => {
            let _ = tx.send(Err(Error::Io(e)));
        }
    }
}

/// Stream-parse newline-delimited JSON from any reader into the channel.
#[cfg(feature = "async")]
pub(crate) fn jsonl_read_stream(
    tx: &UnboundedSender<BatchResult>,
    mut reader: Box<dyn BufRead + Send>,
    chunk_rows: usize,
    policy: &ErrorPolicy,
) {
    let mut objects: Vec<serde_json::Value> = Vec::with_capacity(chunk_rows);
    let mut schema: Option<Vec<(String, DataType)>> = None;
    let mut line = String::new();
    let mut quarantine = match policy {
        ErrorPolicy::Quarantine(path) => match std::fs::File::create(path) {
            Ok(f) => QuarantineWriter::Lines(Box::new(f)),
            Err(e) => {
                let _ = tx.send(Err(Error::Io(e)));
                return;
            }
        },
        _ => QuarantineWriter::None,
    };

    loop {
        match reader.read_line(&mut line) {
            Ok(0) => break,
            Ok(_) => {
                let trimmed = line.trim().to_string();
                line.clear();
                if trimmed.is_empty() {
                    continue;
                }
                match serde_json::from_str::<serde_json::Value>(&trimmed) {
                    Ok(v) => objects.push(v),
                    Err(e) => match policy {
                        ErrorPolicy::Strict => {
                            let _ = tx.send(Err(Error::Json(e)));
                            return;
                        }
                        ErrorPolicy::Skip => continue,
                        ErrorPolicy::Quarantine(_) => {
                            if let Err(io) = quarantine.write_line(&trimmed) {
                                let _ = tx.send(Err(Error::Io(io)));
                                return;
                            }
                            continue;
                        }
                    },
                }
                if objects.len() >= chunk_rows {
                    match build_json_batch(&objects, &mut schema) {
                        Ok(b) => {
                            if tx.send(Ok(b)).is_err() {
                                return;
                            }
                        }
                        Err(e) => {
                            let _ = tx.send(Err(e));
                            return;
                        }
                    }
                    objects.clear();
                }
            }
            Err(e) => {
                let _ = tx.send(Err(Error::Io(e)));
                return;
            }
        }
    }

    if !objects.is_empty() {
        match build_json_batch(&objects, &mut schema) {
            Ok(b) => {
                let _ = tx.send(Ok(b));
            }
            Err(e) => {
                let _ = tx.send(Err(e));
            }
        }
    }
}

#[cfg(feature = "async")]
impl std::fmt::Debug for JsonlSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JsonlSource")
            .field("chunk_rows", &self.chunk_rows)
            .finish()
    }
}

#[cfg(feature = "async")]
fn build_json_batch(
    objects: &[serde_json::Value],
    schema: &mut Option<Vec<(String, DataType)>>,
) -> Result<RecordBatch> {
    let resolved = match schema {
        Some(s) => s.clone(),
        None => {
            let inferred = infer_json_schema(objects)?;
            *schema = Some(inferred.clone());
            inferred
        }
    };
    let columns = resolved
        .iter()
        .map(|(name, data_type)| {
            let mut col = Column::new(name.clone(), *data_type, objects.len());
            for obj in objects {
                let value = match obj.get(name) {
                    Some(j) => json_to_value(j, *data_type)
                        .map_err(|e| Error::Schema(format!("column {name}: {e}")))?,
                    None => Value::Null,
                };
                col.push(value);
            }
            Ok(col)
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(RecordBatch::new(columns))
}

#[cfg(feature = "async")]
fn json_to_value(json: &serde_json::Value, data_type: DataType) -> Result<Value> {
    match json {
        serde_json::Value::Null => Ok(Value::Null),
        serde_json::Value::Bool(b) => Ok(Value::Bool(*b)),
        serde_json::Value::Number(n) => match data_type {
            DataType::Int32 => n
                .as_i64()
                .and_then(|v| i32::try_from(v).ok())
                .map(Value::Int32)
                .ok_or_else(|| Error::Schema(format!("cannot parse number {n} as int32"))),
            DataType::Int64 => n
                .as_i64()
                .map(Value::Int64)
                .ok_or_else(|| Error::Schema(format!("cannot parse number {n} as int64"))),
            DataType::Float32 => n
                .as_f64()
                .map(|f| Value::Float32(f as f32))
                .ok_or_else(|| Error::Schema(format!("cannot parse number {n} as float32"))),
            DataType::Float64 => n
                .as_f64()
                .map(Value::Float64)
                .ok_or_else(|| Error::Schema(format!("cannot parse number {n} as float64"))),
            DataType::Utf8 => Ok(Value::Utf8(json.to_string())),
            DataType::Bool => n
                .as_i64()
                .map(|v| v != 0)
                .map(Value::Bool)
                .ok_or_else(|| Error::Schema(format!("cannot parse number {n} as bool"))),
            DataType::Date => n
                .as_i64()
                .map(|v| Value::Date(v as i32))
                .ok_or_else(|| Error::Schema(format!("cannot parse number {n} as date"))),
            DataType::Timestamp => n
                .as_i64()
                .map(Value::Timestamp)
                .ok_or_else(|| Error::Schema(format!("cannot parse number {n} as timestamp"))),
        },
        serde_json::Value::String(s) => match data_type {
            DataType::Date => tpt_stream_columnar::value::parse_date(s)
                .map(Value::Date)
                .ok_or_else(|| Error::Schema(format!("cannot parse {s:?} as date"))),
            DataType::Timestamp => tpt_stream_columnar::value::parse_timestamp(s)
                .map(Value::Timestamp)
                .ok_or_else(|| Error::Schema(format!("cannot parse {s:?} as timestamp"))),
            _ => Ok(Value::Utf8(s.clone())),
        },
        serde_json::Value::Array(_) | serde_json::Value::Object(_) => {
            Ok(Value::Utf8(json.to_string()))
        }
    }
}

#[cfg(feature = "async")]
fn infer_json_schema(objects: &[serde_json::Value]) -> Result<Vec<(String, DataType)>> {
    let mut names: Vec<String> = Vec::new();
    let mut types: Vec<Option<DataType>> = Vec::new();
    let mut seen: std::collections::HashMap<String, usize> = std::collections::HashMap::new();

    for obj in objects {
        let serde_json::Value::Object(map) = obj else {
            return Err(Error::Schema("JSONL rows must be objects".into()));
        };
        for (key, value) in map {
            let idx = match seen.get(key) {
                Some(&i) => i,
                None => {
                    let i = seen.len();
                    seen.insert(key.clone(), i);
                    names.push(key.clone());
                    types.push(None);
                    i
                }
            };
            if value.is_null() {
                continue;
            }
            let dt = json_type_hint(value);
            types[idx] = Some(match types[idx] {
                Some(current) => widen(current, dt),
                None => dt,
            });
        }
    }

    Ok(names
        .into_iter()
        .zip(types.into_iter().map(|t| t.unwrap_or(DataType::Utf8)))
        .collect())
}

#[cfg(feature = "async")]
fn json_type_hint(json: &serde_json::Value) -> DataType {
    match json {
        serde_json::Value::Bool(_) => DataType::Bool,
        serde_json::Value::Number(n) => {
            if n.is_i64() {
                let v = n.as_i64().expect("checked");
                if v >= i32::MIN as i64 && v <= i32::MAX as i64 {
                    DataType::Int32
                } else {
                    DataType::Int64
                }
            } else if n.is_f64() {
                DataType::Float64
            } else {
                DataType::Int64
            }
        }
        serde_json::Value::String(_) => DataType::Utf8,
        _ => DataType::Utf8,
    }
}

#[cfg(feature = "async")]
fn widen(current: DataType, seen: DataType) -> DataType {
    use DataType::*;
    if current == seen {
        return current;
    }
    match (current, seen) {
        (Utf8, _) | (_, Utf8) => Utf8,
        (Int32, Int64) | (Int64, Int32) => Int64,
        (Float32, Float64) | (Float64, Float32) => Float64,
        (Int32, Float32)
        | (Float32, Int32)
        | (Int32, Float64)
        | (Float64, Int32)
        | (Int64, Float32)
        | (Float32, Int64)
        | (Int64, Float64)
        | (Float64, Int64) => Float64,
        (Bool, Int32)
        | (Int32, Bool)
        | (Bool, Int64)
        | (Int64, Bool)
        | (Bool, Float32)
        | (Float32, Bool)
        | (Bool, Float64)
        | (Float64, Bool) => Utf8,
        _ => current,
    }
}

// ---------------------------------------------------------------------------
// JSON array
// ---------------------------------------------------------------------------

#[cfg(feature = "async")]
pub struct JsonArraySource {
    reader: StreamingReader,
    pending: Option<BatchResult>,
    chunk_rows: usize,
}

#[cfg(feature = "async")]
impl JsonArraySource {
    pub fn open(path: impl Into<String>) -> Self {
        JsonArraySource::open_with_chunk_size(path, DEFAULT_CHUNK_ROWS)
    }

    pub fn open_with_chunk_size(path: impl Into<String>, chunk_rows: usize) -> Self {
        let path = path.into();
        let (reader, handle) =
            StreamingReader::spawn(move |tx| json_array_read_loop(tx, &path, chunk_rows));
        let _ = handle;
        JsonArraySource {
            reader,
            pending: None,
            chunk_rows,
        }
    }

    pub fn with_chunk_size(mut self, rows: usize) -> Self {
        self.chunk_rows = rows;
        self
    }
}

#[cfg(feature = "async")]
fn json_array_read_loop(tx: &UnboundedSender<BatchResult>, path: &str, chunk_rows: usize) {
    match open_file_buffered(path) {
        Ok(reader) => json_array_read_stream(tx, reader, chunk_rows),
        Err(e) => {
            let _ = tx.send(Err(Error::Io(e)));
        }
    }
}

/// Stream-parse a top-level JSON array from any reader into the channel.
#[cfg(feature = "async")]
pub(crate) fn json_array_read_stream(
    tx: &UnboundedSender<BatchResult>,
    reader: Box<dyn BufRead + Send>,
    chunk_rows: usize,
) {
    let mut scanner = JsonArrayScanner::new(reader);
    let mut objects: Vec<serde_json::Value> = Vec::with_capacity(chunk_rows);
    let mut schema: Option<Vec<(String, DataType)>> = None;

    loop {
        let value = match scanner.next_value() {
            Ok(Some(v)) => v,
            Ok(None) => break,
            Err(e) => {
                let _ = tx.send(Err(Error::Json(e)));
                return;
            }
        };
        objects.push(value);
        if objects.len() >= chunk_rows {
            match build_json_batch(&objects, &mut schema) {
                Ok(b) => {
                    if tx.send(Ok(b)).is_err() {
                        return;
                    }
                }
                Err(e) => {
                    let _ = tx.send(Err(e));
                    return;
                }
            }
            objects.clear();
        }
    }

    if !objects.is_empty() {
        match build_json_batch(&objects, &mut schema) {
            Ok(b) => {
                let _ = tx.send(Ok(b));
            }
            Err(e) => {
                let _ = tx.send(Err(e));
            }
        }
    }
}

/// Streaming scanner that reads a top-level JSON array one element at a time from
/// a `BufRead`, without loading the whole array into memory. Tracks string/escape
/// state and structural depth to find element boundaries.
#[cfg(feature = "async")]
struct JsonArrayScanner<R: BufRead> {
    reader: R,
    open: bool,
    closed: bool,
}

#[cfg(feature = "async")]
impl<R: BufRead> JsonArrayScanner<R> {
    fn new(reader: R) -> Self {
        JsonArrayScanner {
            reader,
            open: false,
            closed: false,
        }
    }

    fn next_value(&mut self) -> std::result::Result<Option<serde_json::Value>, serde_json::Error> {
        if self.closed {
            return Ok(None);
        }

        let mut buf: Vec<u8> = Vec::new();
        let mut depth: i32 = 0;
        let mut in_string = false;
        let mut just_escaped = false;
        let mut started = false;
        let mut one = [0u8; 1];

        loop {
            let n = match self.reader.read(&mut one) {
                Ok(n) => n,
                Err(e) => return Err(serde_json::Error::io(e)),
            };
            if n == 0 {
                break;
            }
            let b = one[0];
            let c = b as char;

            if !started {
                if c.is_whitespace() {
                    continue;
                }
                if c == '[' && !self.open {
                    self.open = true;
                    continue;
                }
                if c == ',' && self.open {
                    continue;
                }
                if c == ']' && self.open && depth == 0 {
                    self.closed = true;
                    return Ok(None);
                }
                started = true;
                if c == '{' || c == '[' {
                    depth = 1;
                }
                buf.push(b);
                continue;
            }

            // Inside the current element.
            if in_string {
                buf.push(b);
                if just_escaped {
                    just_escaped = false;
                } else if c == '\\' {
                    just_escaped = true;
                } else if c == '"' {
                    in_string = false;
                }
                continue;
            }

            match c {
                '"' => {
                    in_string = true;
                    buf.push(b);
                }
                '{' | '[' => {
                    depth += 1;
                    buf.push(b);
                }
                '}' | ']' => {
                    if depth == 0 && c == ']' {
                        // The array's own closing bracket: the element is complete.
                        self.closed = true;
                        return parse_array_element(&buf);
                    }
                    buf.push(b);
                    depth -= 1;
                }
                _ => {
                    if depth == 0 && (c == ',' || c == ']' || c.is_whitespace()) {
                        if c == ']' {
                            self.closed = true;
                        }
                        return parse_array_element(&buf);
                    }
                    buf.push(b);
                }
            }
        }

        // EOF: either nothing to parse or the element ended at the stream end.
        if self.open && depth == 0 && started {
            parse_array_element(&buf)
        } else {
            Ok(None)
        }
    }
}

#[cfg(feature = "async")]
fn parse_array_element(
    buf: &[u8],
) -> std::result::Result<Option<serde_json::Value>, serde_json::Error> {
    if buf.is_empty() {
        return Ok(None);
    }
    serde_json::from_slice::<serde_json::Value>(buf).map(Some)
}

#[cfg(feature = "async")]
impl std::fmt::Debug for JsonArraySource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JsonArraySource")
            .field("chunk_rows", &self.chunk_rows)
            .finish()
    }
}

#[cfg(feature = "async")]
#[async_trait::async_trait]
impl Source for CsvSource {
    async fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        if self.reader.is_none() {
            self.spawn_reader();
        }
        let reader = self.reader.as_mut().expect("spawned above");
        if reader.done {
            return Ok(None);
        }
        if let Some(result) = self.pending.take() {
            reader.done = true;
            return match result {
                Ok(b) => Ok(Some(b)),
                Err(e) => Err(e),
            };
        }
        match reader.next().await {
            Some(Ok(batch)) => Ok(Some(batch)),
            Some(Err(e)) => {
                reader.done = true;
                Err(e)
            }
            None => {
                reader.done = true;
                Ok(None)
            }
        }
    }
}

#[cfg(feature = "async")]
#[async_trait::async_trait]
impl Source for JsonlSource {
    async fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        if self.reader.is_none() {
            self.spawn_reader();
        }
        let reader = self.reader.as_mut().expect("spawned above");
        if reader.done {
            return Ok(None);
        }
        if let Some(result) = self.pending.take() {
            reader.done = true;
            return match result {
                Ok(b) => Ok(Some(b)),
                Err(e) => Err(e),
            };
        }
        match reader.next().await {
            Some(Ok(batch)) => Ok(Some(batch)),
            Some(Err(e)) => {
                reader.done = true;
                Err(e)
            }
            None => {
                reader.done = true;
                Ok(None)
            }
        }
    }
}
#[cfg(feature = "async")]
impl_source!(JsonArraySource);

// ---------------------------------------------------------------------------
// Columnar (.tptcol) source
// ---------------------------------------------------------------------------

#[cfg(feature = "async")]
pub struct ColumnarSource {
    reader: StreamingReader,
    pending: Option<BatchResult>,
}

#[cfg(feature = "async")]
impl ColumnarSource {
    pub fn open(path: impl Into<String>) -> Self {
        let path = path.into();
        let (reader, handle) = StreamingReader::spawn(move |tx| columnar_read_loop(tx, &path));
        let _ = handle;
        ColumnarSource {
            reader,
            pending: None,
        }
    }
}

#[cfg(feature = "async")]
fn columnar_read_loop(tx: &UnboundedSender<BatchResult>, path: &str) {
    match std::fs::File::open(path) {
        Ok(file) => columnar_read_stream(tx, Box::new(file)),
        Err(e) => {
            let _ = tx.send(Err(Error::Io(e)));
        }
    }
}

/// Stream-parse a `.tptcol` byte stream from any reader into the channel.
#[cfg(feature = "async")]
pub(crate) fn columnar_read_stream(
    tx: &UnboundedSender<BatchResult>,
    reader: Box<dyn std::io::Read + Send>,
) {
    let mut reader = tpt_stream_columnar::format::ChunkedReader::new(reader);
    loop {
        match reader.next_batch() {
            Ok(Some(batch)) => {
                if tx.send(Ok(batch)).is_err() {
                    return;
                }
            }
            Ok(None) => return,
            Err(e) => {
                let _ = tx.send(Err(Error::Other(e.to_string())));
                return;
            }
        }
    }
}

#[cfg(feature = "async")]
impl std::fmt::Debug for ColumnarSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ColumnarSource").finish()
    }
}

#[cfg(feature = "async")]
impl_source!(ColumnarSource);

#[cfg(test)]
mod tests {
    use super::*;

    fn chunk_from_csv(csv: &str) -> ColumnarChunk {
        let mut cr = ColumnarReader::from_reader(csv.as_bytes()).unwrap();
        let nc = cr.num_columns();
        let mut chunk = ColumnarChunk::new(nc, 64);
        let mut policy = RaggedRowPolicy::Strict;
        cr.read_chunk_into(64, &mut policy, &mut chunk).unwrap();
        chunk
    }

    #[test]
    fn parse_cell_works() {
        assert_eq!(parse_cell("42", DataType::Int32), Ok(Value::Int32(42)));
        assert_eq!(
            parse_cell("3.5", DataType::Float64),
            Ok(Value::Float64(3.5))
        );
        assert_eq!(parse_cell("true", DataType::Bool), Ok(Value::Bool(true)));
        assert_eq!(
            parse_cell("abc", DataType::Utf8),
            Ok(Value::Utf8("abc".into()))
        );
    }

    #[test]
    fn infer_types_basic() {
        let chunk = chunk_from_csv("a,b,c\n1,true,x\n2,false,y\n3,true,z\n");
        let types = infer_types_columnar(&chunk);
        assert_eq!(types[0], DataType::Int32);
        assert_eq!(types[1], DataType::Bool);
        assert_eq!(types[2], DataType::Utf8);
    }

    #[test]
    fn infer_types_escalates() {
        let chunk = chunk_from_csv("n\n1\n3000000000\n3.5\n");
        let types = infer_types_columnar(&chunk);
        assert_eq!(types[0], DataType::Float64);
    }

    #[test]
    fn csv_batch_from_chunk() {
        let chunk = chunk_from_csv("id,flag\n1,true\n2,false\n");
        let headers = vec!["id".to_string(), "flag".to_string()];
        let batch = build_csv_batch_from_chunk(&headers, &mut None, &chunk).unwrap();
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.cell(1, "id"), Some(Value::Int32(2)));
        assert_eq!(batch.cell(0, "flag"), Some(Value::Bool(true)));
    }

    #[test]
    #[cfg(feature = "async")]
    fn json_type_hint_numbers() {
        assert_eq!(json_type_hint(&serde_json::json!(5)), DataType::Int32);
        assert_eq!(
            json_type_hint(&serde_json::json!(5000000000i64)),
            DataType::Int64
        );
        assert_eq!(json_type_hint(&serde_json::json!(5.5)), DataType::Float64);
    }
}

/// Serialize `batches` to CSV text (header + rows, posix newlines). Nulls are
/// emitted as empty fields. Exposed for language wrappers without a filesystem
/// (WASM) and the FFI tests.
pub fn batches_to_csv(batches: &[RecordBatch]) -> String {
    let mut buffer: Vec<u8> = Vec::new();
    {
        let mut writer = tpt_csv::WriterBuilder::new().from_writer(&mut buffer);
        let mut record = tpt_csv::StringRecord::new();
        for (bi, batch) in batches.iter().enumerate() {
            if bi == 0 {
                for name in batch.column_names() {
                    record.push_field(name);
                }
                let _ = writer.write_record(&record);
                record.clear();
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
                let _ = writer.write_record(&record);
            }
        }
    }
    String::from_utf8(buffer).unwrap_or_default()
}

#[cfg(test)]
mod in_memory_csv_tests {
    use super::*;

    #[test]
    fn csv_to_batches_roundtrip() {
        let text = "id,name\n1,alice\n2,bob\n";
        let batches = csv_to_batches(text, 2).unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches_to_csv(&batches), text);
    }

    #[test]
    fn csv_to_batches_chunks() {
        let mut text = String::from("id,x\n");
        for i in 0..5 {
            text.push_str(&format!("{i},{i}\n"));
        }
        let batches = csv_to_batches(&text, 2).unwrap();
        assert_eq!(batches.len(), 3); // 2 + 2 + 1
        assert_eq!(batches_to_csv(&batches), text);
    }

    /// Append filler rows of `columns` fields until `text` is at least
    /// [`PARALLEL_CSV_MIN_BYTES`], so the parallel path is really exercised
    /// regardless of how wide the caller's own rows turn out to be.
    #[cfg(feature = "async")]
    fn pad_to_parallel_size(mut text: String, columns: usize) -> String {
        let mut i = 0usize;
        while text.len() < PARALLEL_CSV_MIN_BYTES {
            let row = (0..columns)
                .map(|c| {
                    if c == 1 {
                        format!("\"filler {i}\"")
                    } else {
                        i.to_string()
                    }
                })
                .collect::<Vec<_>>()
                .join(",");
            text.push_str(&row);
            text.push('\n');
            i += 1;
        }
        text
    }

    /// Build a CSV whose rows mix the quoting styles that make naive newline
    /// splitting wrong: embedded newlines, escaped quotes, and commas inside
    /// quoted fields. Rows are normalized on write (CRLF becomes LF), so
    /// compare against the sequential result rather than the raw input.
    #[cfg(feature = "async")]
    fn messy_csv(rows: usize) -> String {
        let mut text = String::from("id,label,value\n");
        for i in 0..rows {
            match i % 3 {
                0 => text.push_str(&format!("{i},plain,{}\n", i * 7)),
                1 => text.push_str(&format!("{i},\"has, comma\",{}\n", i * 7)),
                _ => text.push_str(&format!("{i},\"two\nlines\",{}\n", i * 7)),
            }
        }
        text
    }

    #[test]
    #[cfg(feature = "async")]
    fn parallel_csv_matches_sequential() {
        // Quoted newlines would corrupt the data if the boundary scan ignored
        // quote state, so this doubles as a check that slicing is lossless.
        let text = pad_to_parallel_size(messy_csv(3), 3);
        assert!(text.len() >= PARALLEL_CSV_MIN_BYTES);

        let parallel = csv_to_batches(&text, 4096).unwrap();
        let sequential = csv_to_batches_sequential(&text, 4096).unwrap();

        assert!(parallel.len() > 1, "expected multiple slices");
        assert_eq!(parallel.len(), sequential.len());
        assert_eq!(batches_to_csv(&parallel), batches_to_csv(&sequential));
        assert_eq!(
            parallel.iter().map(|b| b.num_rows()).sum::<usize>(),
            sequential.iter().map(|b| b.num_rows()).sum::<usize>()
        );
    }

    #[test]
    #[cfg(feature = "async")]
    fn parallel_csv_slicing_is_record_aligned_at_every_chunk_size() {
        let text = pad_to_parallel_size(messy_csv(3), 3);
        let expected = batches_to_csv(&csv_to_batches_sequential(&text, 4096).unwrap());
        // Sizes that do and do not divide the row count evenly.
        for chunk_rows in [7usize, 999, 4096, 100_000] {
            let batches = csv_to_batches(&text, chunk_rows).unwrap();
            assert_eq!(
                batches_to_csv(&batches),
                expected,
                "chunk_rows={chunk_rows}"
            );
        }
    }

    #[test]
    #[cfg(feature = "async")]
    fn parallel_csv_ragged_row_reports_whole_document_position() {
        // A short row in a later slice must report its position in the whole
        // document, not relative to its own slice.
        let mut text = pad_to_parallel_size(String::from("id,label\n"), 2);
        let rows = text.lines().count() - 1;
        text.push_str("short\n");
        // The header is record 1 and the `rows` data rows are records 2..=rows+1,
        // so the short row is document record rows+2 — the position a
        // sequential parse would report.
        let expected_line = rows as u64 + 2;

        let err = csv_to_batches(&text, 1000).unwrap_err();
        match err {
            Error::Csv(tpt_csv::Error::Ragged {
                line,
                expected,
                got,
            }) => {
                assert_eq!((line, expected, got), (expected_line, 2, 1));
            }
            other => panic!("expected a ragged-row error, got {other:?}"),
        }
    }

    #[test]
    #[cfg(feature = "async")]
    fn parallel_csv_last_slice_ending_at_buffer_end_adds_no_empty_batch() {
        // The final slice ends precisely at the buffer end; no empty trailing
        // batch may appear.
        let text = pad_to_parallel_size(String::from("id,label\n"), 2);
        let rows = text.lines().count() - 1;
        assert!(rows > 1000, "fixture must span several slices");
        let chunk_rows = (rows / 1000) * 1000;
        let batches = csv_to_batches(&text, chunk_rows).unwrap();
        // The remainder becomes a final partial batch, never an empty one.
        assert!(batches.iter().all(|b| b.num_rows() > 0));
        assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), rows);
    }

    #[test]
    #[cfg(feature = "async")]
    fn parallel_csv_without_trailing_newline() {
        // The final record has no line ending; it must be included in the last
        // slice rather than dropped.
        let mut text = pad_to_parallel_size(String::from("id,label\n"), 2);
        let rows = text.lines().count() - 1;
        text.push_str("99999,last");

        let batches = csv_to_batches(&text, 1000).unwrap();
        assert_eq!(
            batches.iter().map(|b| b.num_rows()).sum::<usize>(),
            rows + 1
        );
        let out = batches_to_csv(&batches);
        assert!(out.ends_with("99999,last\n"), "last row missing");
    }

    #[test]
    #[cfg(feature = "async")]
    fn parallel_csv_preserves_embedded_newlines_in_values() {
        // The strongest check that boundaries respect quotes: every quoted
        // value containing a newline must survive intact and in order.
        let mut text = pad_to_parallel_size(String::from("id,label\n"), 2);
        for i in 0..2000 {
            text.push_str(&format!("{i},\"line one\nline two {i}\"\n"));
        }
        let out = batches_to_csv(&csv_to_batches(&text, 256).unwrap());
        for i in 0..2000 {
            assert!(
                out.contains(&format!("\"line one\nline two {i}\"")),
                "value {i} was corrupted across a slice boundary"
            );
        }
    }

    #[test]
    #[cfg(feature = "async")]
    fn parallel_csv_single_huge_record_yields_no_batches() {
        // Above the threshold, but the only record is the header: the boundary
        // scan must not invent data rows.
        let text = format!("\"{}\",b\n", "x".repeat(PARALLEL_CSV_MIN_BYTES));
        assert!(csv_to_batches(&text, 1024).unwrap().is_empty());
    }

    #[test]
    #[cfg(feature = "async")]
    fn parallel_csv_ragged_row_in_first_slice_is_reported() {
        let mut text = String::from("id,label\n1\n");
        text.push_str(&pad_to_parallel_size(String::new(), 2));
        let err = csv_to_batches(&text, 1000).unwrap_err();
        match err {
            Error::Csv(tpt_csv::Error::Ragged {
                line,
                expected,
                got,
            }) => {
                assert_eq!((line, expected, got), (2, 2, 1));
            }
            other => panic!("expected a ragged-row error, got {other:?}"),
        }
    }

    #[test]
    #[cfg(feature = "async")]
    fn parallel_csv_type_inference_matches_sequential() {
        // A late non-numeric value must still widen the schema exactly as the
        // sequential path does, proving batches are built in slice order.
        let mut text = pad_to_parallel_size(String::from("id,value\n"), 2);
        let rows = text.lines().count() - 1;
        text.push_str("99999,not-a-number\n");

        let parallel = csv_to_batches(&text, 4096).unwrap();
        let sequential = csv_to_batches_sequential(&text, 4096).unwrap();
        assert_eq!(batches_to_csv(&parallel), batches_to_csv(&sequential));
        assert_eq!(parallel.len(), sequential.len());
        // Both paths widen `value` to text.
        for batches in [&parallel, &sequential] {
            assert_eq!(
                batches[0].column("value").unwrap().data_type(),
                DataType::Utf8
            );
        }
        assert_eq!(
            rows + 1,
            parallel.iter().map(|b| b.num_rows()).sum::<usize>()
        );
    }
}
