//! SQLite source and sink (`rusqlite`, feature `sqlite`).
//!
//! The sink creates the target table from the first batch's schema
//! (`CREATE TABLE IF NOT EXISTS`) and inserts rows in one transaction per
//! batch. The source streams a SELECT query in chunks from a dedicated
//! reader thread, so query execution never blocks the async runtime.

use crate::column::Column;
use crate::error::{Error, Result};
use crate::source::StreamingReader;
use crate::table::RecordBatch;
use crate::value::{DataType, Value};

type BatchResult = std::result::Result<RecordBatch, Error>;

fn sql_type_name(data_type: DataType) -> &'static str {
    match data_type {
        DataType::Int32 | DataType::Int64 | DataType::Bool => "INTEGER",
        DataType::Float32 | DataType::Float64 => "REAL",
        // SQLite has no native date type: ISO text with DATE affinity.
        DataType::Date | DataType::Timestamp | DataType::Utf8 => "TEXT",
    }
}

fn value_to_sql(value: &Value) -> rusqlite::types::Value {
    use rusqlite::types::Value as Sql;
    match value {
        Value::Null => Sql::Null,
        Value::Bool(b) => Sql::Integer(*b as i64),
        Value::Int32(v) => Sql::Integer(*v as i64),
        Value::Int64(v) => Sql::Integer(*v),
        Value::Float32(v) => Sql::Real(*v as f64),
        Value::Float64(v) => Sql::Real(*v),
        // ISO text survives round trips and stays sortable.
        Value::Date(_) | Value::Timestamp(_) => Sql::Text(value.to_string()),
        Value::Utf8(s) => Sql::Text(s.clone()),
    }
}

fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

// ---------------------------------------------------------------------------
// Sink
// ---------------------------------------------------------------------------

/// Writes batches into a SQLite database table. Connects lazily on the first
/// batch; the table is created from that batch's schema unless it exists.
pub struct SqliteSink {
    path: String,
    table: String,
    drop_existing: bool,
    conn: Option<rusqlite::Connection>,
    rows_written: u64,
}

impl SqliteSink {
    pub fn open(path: impl Into<String>, table: impl Into<String>) -> Self {
        SqliteSink {
            path: path.into(),
            table: table.into(),
            drop_existing: false,
            conn: None,
            rows_written: 0,
        }
    }

    /// Drop the table before the first batch is written (fresh overwrite).
    pub fn overwrite(mut self) -> Self {
        self.drop_existing = true;
        self
    }

    pub fn rows_written(&self) -> u64 {
        self.rows_written
    }

    fn connect(&mut self, batch: &RecordBatch) -> Result<()> {
        let conn = rusqlite::Connection::open(&self.path)
            .map_err(|e| Error::Database(format!("sqlite open {}: {e}", self.path)))?;
        if self.drop_existing {
            conn.execute_batch(&format!(
                "DROP TABLE IF EXISTS {};",
                quote_ident(&self.table)
            ))
            .map_err(|e| Error::Database(format!("sqlite drop table: {e}")))?;
        }
        let mut ddl = format!("CREATE TABLE IF NOT EXISTS {} (", quote_ident(&self.table));
        for (i, column) in batch.columns().iter().enumerate() {
            if i > 0 {
                ddl.push_str(", ");
            }
            ddl.push_str(&format!(
                "{} {}",
                quote_ident(column.name()),
                sql_type_name(column.data_type())
            ));
        }
        ddl.push(')');
        conn.execute_batch(&ddl)
            .map_err(|e| Error::Database(format!("sqlite create table: {e}")))?;
        self.conn = Some(conn);
        Ok(())
    }
}

impl std::fmt::Debug for SqliteSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SqliteSink")
            .field("path", &self.path)
            .field("table", &self.table)
            .field("rows_written", &self.rows_written)
            .finish()
    }
}

#[async_trait::async_trait]
impl crate::sink::Sink for SqliteSink {
    async fn write_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        if batch.num_rows() == 0 {
            return Ok(());
        }
        if self.conn.is_none() {
            self.connect(batch)?;
        }
        let conn = self.conn.as_mut().expect("connected");
        let names: Vec<String> = batch.column_names().into_iter().map(quote_ident).collect();
        let placeholders: Vec<String> = (1..=names.len()).map(|i| format!("?{i}")).collect();
        let insert_sql = format!(
            "INSERT INTO {} ({}) VALUES ({})",
            quote_ident(&self.table),
            names.join(", "),
            placeholders.join(", ")
        );

        let tx = conn
            .transaction()
            .map_err(|e| Error::Database(format!("sqlite begin: {e}")))?;
        {
            let mut stmt = tx
                .prepare(&insert_sql)
                .map_err(|e| Error::Database(format!("sqlite prepare: {e}")))?;
            for row in 0..batch.num_rows() {
                let params: Vec<rusqlite::types::Value> = batch
                    .columns()
                    .iter()
                    .map(|c| value_to_sql(&c.get(row).unwrap_or(Value::Null)))
                    .collect();
                stmt.execute(rusqlite::params_from_iter(params.iter()))
                    .map_err(|e| Error::Database(format!("sqlite insert: {e}")))?;
            }
        }
        tx.commit()
            .map_err(|e| Error::Database(format!("sqlite commit: {e}")))?;
        self.rows_written += batch.num_rows() as u64;
        Ok(())
    }

    async fn finish(&mut self) -> Result<()> {
        if let Some(conn) = &self.conn {
            conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
                .map_err(|e| Error::Database(format!("sqlite checkpoint: {e}")))?;
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Source
// ---------------------------------------------------------------------------

/// Streams the result of a SELECT query as batches, reading from a dedicated
/// thread. Column types come from SQLite declared types (BOOLEAN → Bool,
/// INTEGER → Int64, REAL → Float64, TEXT → Utf8); undeclared expression
/// columns infer from the first non-null value.
pub struct SqliteSource {
    reader: StreamingReader,
    chunk_rows: usize,
}

impl SqliteSource {
    pub fn open(path: impl Into<String>, query: impl Into<String>) -> Self {
        SqliteSource::open_with_chunk_size(path, query, crate::DEFAULT_CHUNK_ROWS)
    }

    pub fn open_with_chunk_size(
        path: impl Into<String>,
        query: impl Into<String>,
        chunk_rows: usize,
    ) -> Self {
        let path = path.into();
        let query = query.into();
        let (reader, handle) =
            StreamingReader::spawn(move |tx| sqlite_read_loop(tx, &path, &query, chunk_rows));
        let _ = handle;
        SqliteSource { reader, chunk_rows }
    }

    pub fn with_chunk_size(mut self, rows: usize) -> Self {
        self.chunk_rows = rows;
        self
    }
}

impl std::fmt::Debug for SqliteSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SqliteSource")
            .field("chunk_rows", &self.chunk_rows)
            .finish()
    }
}

fn decl_type_to_data_type(decl: Option<&str>) -> Option<DataType> {
    let decl = decl?.to_ascii_uppercase();
    if decl.contains("BOOL") || decl.contains("BIT") {
        Some(DataType::Bool)
    } else if decl.contains("INT") {
        Some(DataType::Int64)
    } else if decl.contains("REAL") || decl.contains("FLOA") || decl.contains("DOUB") {
        Some(DataType::Float64)
    } else if decl.contains("CHAR") || decl.contains("TEXT") || decl.contains("CLOB") {
        Some(DataType::Utf8)
    } else if decl.contains("TIMESTAMP") || decl.contains("DATETIME") {
        Some(DataType::Timestamp)
    } else if decl.contains("DATE") {
        Some(DataType::Date)
    } else {
        None
    }
}

fn infer_from_value(value: &Value) -> DataType {
    match value {
        Value::Date(_) => DataType::Date,
        Value::Timestamp(_) => DataType::Timestamp,
        Value::Bool(_) => DataType::Bool,
        Value::Int32(_) | Value::Int64(_) => DataType::Int64,
        Value::Float32(_) | Value::Float64(_) => DataType::Float64,
        Value::Utf8(_) => DataType::Utf8,
        Value::Null => DataType::Utf8,
    }
}

/// Coerce a raw SQLite cell to the column's declared type (the column buffers
/// are homogeneous, so mismatched cells are converted, never dropped).
fn sqlite_value(vref: rusqlite::types::ValueRef<'_>, data_type: DataType) -> Value {
    use rusqlite::types::ValueRef as V;
    match (vref, data_type) {
        (V::Null, _) => Value::Null,
        (V::Integer(i), DataType::Bool) => Value::Bool(i != 0),
        (V::Integer(i), DataType::Int32) => Value::Int32(i as i32),
        (V::Integer(i), DataType::Int64) => Value::Int64(i),
        (V::Integer(i), DataType::Float32) => Value::Float32(i as f32),
        (V::Integer(i), DataType::Float64) => Value::Float64(i as f64),
        (V::Integer(i), DataType::Utf8) => Value::Utf8(i.to_string()),
        (V::Real(f), DataType::Float32) => Value::Float32(f as f32),
        (V::Real(f), DataType::Float64) => Value::Float64(f),
        (V::Real(f), DataType::Int32) => Value::Int32(f as i32),
        (V::Real(f), DataType::Int64) => Value::Int64(f as i64),
        (V::Real(f), DataType::Bool) => Value::Bool(f != 0.0),
        (V::Real(f), DataType::Utf8) => Value::Utf8(f.to_string()),
        // SQLite stores dates as ISO text (DATE affinity columns read back
        // as Text); parse into the declared type when it's date-like.
        (V::Text(s), DataType::Date) => {
            let text = String::from_utf8_lossy(s);
            tpt_stream_columnar::value::parse_date(text.trim())
                .map_or_else(|| Value::Utf8(text.into_owned()), Value::Date)
        }
        (V::Text(s), DataType::Timestamp) => {
            let text = String::from_utf8_lossy(s);
            tpt_stream_columnar::value::parse_timestamp(text.trim())
                .map_or_else(|| Value::Utf8(text.into_owned()), Value::Timestamp)
        }
        (V::Integer(i), DataType::Date) => Value::Date(i as i32),
        (V::Integer(i), DataType::Timestamp) => Value::Timestamp(i),
        (V::Real(f), DataType::Date) => Value::Date(f as i32),
        (V::Real(f), DataType::Timestamp) => Value::Timestamp(f as i64),
        (V::Text(s), _) => Value::Utf8(String::from_utf8_lossy(s).into_owned()),
        (V::Blob(b), _) => Value::Utf8(String::from_utf8_lossy(b).into_owned()),
    }
}

fn sqlite_read_loop(
    tx: &tokio::sync::mpsc::UnboundedSender<BatchResult>,
    path: &str,
    query: &str,
    chunk_rows: usize,
) {
    let conn = match rusqlite::Connection::open_with_flags(
        path,
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
    ) {
        Ok(c) => c,
        Err(e) => {
            let _ = tx.send(Err(Error::Database(format!("sqlite open {path}: {e}"))));
            return;
        }
    };
    let mut stmt = match conn.prepare(query) {
        Ok(s) => s,
        Err(e) => {
            let _ = tx.send(Err(Error::Database(format!("sqlite prepare: {e}"))));
            return;
        }
    };
    let column_count = stmt.column_count();
    if column_count == 0 {
        let _ = tx.send(Err(Error::Schema("query returned no columns".into())));
        return;
    }
    let names: Vec<String> = stmt.column_names().into_iter().map(String::from).collect();
    let declared: Vec<Option<DataType>> = stmt
        .columns()
        .iter()
        .map(|c| decl_type_to_data_type(c.decl_type()))
        .collect();

    let mut rows: Vec<Vec<Value>> = Vec::with_capacity(chunk_rows);
    let mut schema: Option<Vec<DataType>> = None;
    let mut rows_result = match stmt.query([]) {
        Ok(r) => r,
        Err(e) => {
            let _ = tx.send(Err(Error::Database(format!("sqlite query: {e}"))));
            return;
        }
    };
    loop {
        let row = match rows_result.next() {
            Ok(Some(row)) => row,
            Ok(None) => break,
            Err(e) => {
                let _ = tx.send(Err(Error::Database(format!("sqlite read row: {e}"))));
                return;
            }
        };
        let mut values = Vec::with_capacity(column_count);
        for (i, declared_type) in declared.iter().enumerate() {
            let vref = match row.get_ref(i) {
                Ok(v) => v,
                Err(e) => {
                    let _ = tx.send(Err(Error::Database(format!("sqlite read row: {e}"))));
                    return;
                }
            };
            // Coerce with the declared type when known; otherwise defer the
            // decision to schema inference by peeking the raw value.
            let value = match *declared_type {
                Some(dt) => sqlite_value(vref, dt),
                None => match vref {
                    rusqlite::types::ValueRef::Null => Value::Null,
                    rusqlite::types::ValueRef::Integer(i) => Value::Int64(i),
                    rusqlite::types::ValueRef::Real(f) => Value::Float64(f),
                    rusqlite::types::ValueRef::Text(s) => {
                        Value::Utf8(String::from_utf8_lossy(s).into_owned())
                    }
                    rusqlite::types::ValueRef::Blob(b) => {
                        Value::Utf8(String::from_utf8_lossy(b).into_owned())
                    }
                },
            };
            values.push(value);
        }
        rows.push(values);
        if rows.len() >= chunk_rows {
            match build_sqlite_batch(&names, &mut schema, &rows) {
                Ok(batch) => {
                    if tx.send(Ok(batch)).is_err() {
                        return;
                    }
                }
                Err(e) => {
                    let _ = tx.send(Err(e));
                    return;
                }
            }
            rows.clear();
        }
    }
    if !rows.is_empty() {
        match build_sqlite_batch(&names, &mut schema, &rows) {
            Ok(batch) => {
                let _ = tx.send(Ok(batch));
            }
            Err(e) => {
                let _ = tx.send(Err(e));
            }
        }
    }
}

fn build_sqlite_batch(
    names: &[String],
    schema: &mut Option<Vec<DataType>>,
    rows: &[Vec<Value>],
) -> Result<RecordBatch> {
    let resolved = match schema {
        Some(s) => s.clone(),
        None => {
            let inferred: Vec<DataType> = names
                .iter()
                .enumerate()
                .map(|(i, _)| {
                    rows.iter()
                        .find_map(|row| match &row[i] {
                            Value::Null => None,
                            v => Some(infer_from_value(v)),
                        })
                        .unwrap_or(DataType::Utf8)
                })
                .collect();
            *schema = Some(inferred.clone());
            inferred
        }
    };
    let columns = names
        .iter()
        .zip(&resolved)
        .enumerate()
        .map(|(i, (name, data_type))| {
            let mut col = Column::new(name.clone(), *data_type, rows.len());
            for row in rows {
                col.push(coerce_to(row[i].clone(), *data_type));
            }
            Ok(col)
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(RecordBatch::new(columns))
}

fn coerce_to(value: Value, data_type: DataType) -> Value {
    match (value, data_type) {
        (Value::Null, _) => Value::Null,
        (Value::Bool(b), DataType::Int32) => Value::Int32(b as i32),
        (Value::Bool(b), DataType::Int64) => Value::Int64(b as i64),
        (Value::Bool(b), DataType::Float32) => Value::Float32(if b { 1.0 } else { 0.0 }),
        (Value::Bool(b), DataType::Float64) => Value::Float64(if b { 1.0 } else { 0.0 }),
        (Value::Bool(b), DataType::Utf8) => Value::Utf8(b.to_string()),
        (Value::Int32(v), DataType::Int64) => Value::Int64(v as i64),
        (Value::Int32(v), DataType::Float32) => Value::Float32(v as f32),
        (Value::Int32(v), DataType::Float64) => Value::Float64(v as f64),
        (Value::Int32(v), DataType::Utf8) => Value::Utf8(v.to_string()),
        (Value::Int64(v), DataType::Int32) => Value::Int32(v as i32),
        (Value::Int64(v), DataType::Float32) => Value::Float32(v as f32),
        (Value::Int64(v), DataType::Float64) => Value::Float64(v as f64),
        (Value::Int64(v), DataType::Utf8) => Value::Utf8(v.to_string()),
        (Value::Float32(v), DataType::Float64) => Value::Float64(v as f64),
        (Value::Float32(v), DataType::Int32) => Value::Int32(v as i32),
        (Value::Float32(v), DataType::Int64) => Value::Int64(v as i64),
        (Value::Float32(v), DataType::Utf8) => Value::Utf8(v.to_string()),
        (Value::Float64(v), DataType::Float32) => Value::Float32(v as f32),
        (Value::Float64(v), DataType::Int32) => Value::Int32(v as i32),
        (Value::Float64(v), DataType::Int64) => Value::Int64(v as i64),
        (Value::Float64(v), DataType::Utf8) => Value::Utf8(v.to_string()),
        (Value::Utf8(v), DataType::Utf8) => Value::Utf8(v),
        (value, data_type) => {
            // Parse stringified cells back into typed columns where possible.
            if let Value::Utf8(s) = &value {
                match data_type {
                    DataType::Bool => {
                        return match s.to_ascii_lowercase().as_str() {
                            "true" | "1" => Value::Bool(true),
                            _ => Value::Bool(false),
                        };
                    }
                    DataType::Int32 => {
                        if let Ok(v) = s.parse() {
                            return Value::Int32(v);
                        }
                    }
                    DataType::Int64 => {
                        if let Ok(v) = s.parse() {
                            return Value::Int64(v);
                        }
                    }
                    DataType::Float32 => {
                        if let Ok(v) = s.parse() {
                            return Value::Float32(v);
                        }
                    }
                    DataType::Float64 => {
                        if let Ok(v) = s.parse() {
                            return Value::Float64(v);
                        }
                    }
                    DataType::Date => {
                        if let Some(days) = tpt_stream_columnar::value::parse_date(s) {
                            return Value::Date(days);
                        }
                    }
                    DataType::Timestamp => {
                        if let Some(micros) = tpt_stream_columnar::value::parse_timestamp(s) {
                            return Value::Timestamp(micros);
                        }
                    }
                    DataType::Utf8 => {}
                }
            }
            value
        }
    }
}

#[async_trait::async_trait]
impl crate::source::Source for SqliteSource {
    async fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        if self.reader.done {
            return Ok(None);
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
