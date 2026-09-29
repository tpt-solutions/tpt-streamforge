//! PostgreSQL source and sink (`tokio-postgres`, feature `postgres`).
//!
//! The sink creates the target table from the first batch's schema
//! (`CREATE TABLE IF NOT EXISTS`) and bulk-loads every batch with the
//! `COPY ... FROM STDIN` protocol (one COPY per batch, text format), which
//! avoids per-row INSERT round trips. The source streams a SELECT query in
//! chunks from a dedicated reader thread, so query execution never blocks
//! the async runtime.

use crate::column::Column;
use crate::error::{Error, Result};
use crate::source::StreamingReader;
use crate::table::RecordBatch;
use crate::value::{DataType, Value};
use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use tpt_stream_columnar::value::{date_to_string, timestamp_to_string};

type BatchResult = std::result::Result<RecordBatch, Error>;

fn sql_type_name(data_type: DataType) -> &'static str {
    match data_type {
        DataType::Int32 => "INTEGER",
        DataType::Int64 => "BIGINT",
        DataType::Float32 => "REAL",
        DataType::Float64 => "DOUBLE PRECISION",
        DataType::Bool => "BOOLEAN",
        DataType::Utf8 => "TEXT",
        DataType::Date => "DATE",
        DataType::Timestamp => "TIMESTAMP",
    }
}

fn float_literal(v: f64) -> String {
    if v.is_infinite() {
        if v > 0.0 { "Infinity" } else { "-Infinity" }.to_string()
    } else if v.is_nan() {
        "NaN".to_string()
    } else {
        v.to_string()
    }
}

/// Encode one value for `COPY ... FROM STDIN` in text format: `NULL` is the
/// literal `\N`, and the three significant characters (backslash, tab,
/// newline, carriage return) are backslash-escaped.
fn copy_field(value: &Value) -> String {
    match value {
        Value::Null => "\\N".to_string(),
        Value::Bool(b) => if *b { "t" } else { "f" }.to_string(),
        Value::Int32(v) => v.to_string(),
        Value::Int64(v) => v.to_string(),
        Value::Float32(v) => float_literal(*v as f64),
        Value::Float64(v) => float_literal(*v),
        Value::Date(v) => date_to_string(*v),
        Value::Timestamp(v) => timestamp_to_string(*v),
        Value::Utf8(s) => s
            .replace('\\', "\\\\")
            .replace('\t', "\\t")
            .replace('\n', "\\n")
            .replace('\r', "\\r"),
    }
}

fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// Postgres's binary epoch is 2000-01-01; our `Value::Date`/`Value::Timestamp`
/// count from the Unix epoch (1970-01-01). 10,957 is the day count between them.
const PG_EPOCH_DAYS: i32 = 10_957;
const PG_EPOCH_MICROS: i64 = 946_684_800_000_000;

/// Decodes a binary-format `DATE` column into days since the Unix epoch.
/// `tokio_postgres` has no built-in `FromSql` for `DATE`/`TIMESTAMP` without
/// pulling in `chrono`, so this reads the raw big-endian integer directly.
struct PgDate(i32);

impl<'a> tokio_postgres::types::FromSql<'a> for PgDate {
    fn from_sql(
        _ty: &tokio_postgres::types::Type,
        raw: &'a [u8],
    ) -> std::result::Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        let days_since_pg_epoch = i32::from_be_bytes(raw.try_into()?);
        Ok(PgDate(days_since_pg_epoch + PG_EPOCH_DAYS))
    }

    fn accepts(ty: &tokio_postgres::types::Type) -> bool {
        matches!(*ty, tokio_postgres::types::Type::DATE)
    }
}

struct PgTimestamp(i64);

impl<'a> tokio_postgres::types::FromSql<'a> for PgTimestamp {
    fn from_sql(
        _ty: &tokio_postgres::types::Type,
        raw: &'a [u8],
    ) -> std::result::Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        let micros_since_pg_epoch = i64::from_be_bytes(raw.try_into()?);
        Ok(PgTimestamp(micros_since_pg_epoch + PG_EPOCH_MICROS))
    }

    fn accepts(ty: &tokio_postgres::types::Type) -> bool {
        matches!(*ty, tokio_postgres::types::Type::TIMESTAMP)
    }
}

fn pg_type_to_data_type(pg_type: &tokio_postgres::types::Type) -> DataType {
    match *pg_type {
        tokio_postgres::types::Type::BOOL => DataType::Bool,
        tokio_postgres::types::Type::DATE => DataType::Date,
        tokio_postgres::types::Type::TIMESTAMP => DataType::Timestamp,
        tokio_postgres::types::Type::INT2 | tokio_postgres::types::Type::INT4 => DataType::Int32,
        tokio_postgres::types::Type::INT8 => DataType::Int64,
        tokio_postgres::types::Type::FLOAT4 => DataType::Float32,
        tokio_postgres::types::Type::FLOAT8 => DataType::Float64,
        _ => DataType::Utf8,
    }
}

fn coerce_to(value: Value, data_type: DataType) -> Value {
    match (value, data_type) {
        (Value::Null, _) => Value::Null,
        // Dates pass through when the target matches; string sources parse.
        (Value::Date(days), DataType::Date) => Value::Date(days),
        (Value::Timestamp(micros), DataType::Timestamp) => Value::Timestamp(micros),
        (Value::Utf8(s), DataType::Date) => match tpt_stream_columnar::value::parse_date(&s) {
            Some(days) => Value::Date(days),
            None => Value::Utf8(s),
        },
        (Value::Utf8(s), DataType::Timestamp) => {
            match tpt_stream_columnar::value::parse_timestamp(&s) {
                Some(micros) => Value::Timestamp(micros),
                None => Value::Utf8(s),
            }
        }
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
            if let Value::Utf8(s) = &value {
                match data_type {
                    DataType::Bool => {
                        return match s.to_ascii_lowercase().as_str() {
                            "true" | "1" => Value::Bool(true),
                            _ => Value::Bool(false),
                        };
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
                    DataType::Utf8 => {}
                }
            }
            value
        }
    }
}

// ---------------------------------------------------------------------------
// Sink
// ---------------------------------------------------------------------------

/// The sink's default retry policy: one immediate reconnect, with no delay.
///
/// This preserves the sink's long-standing behavior exactly (a single
/// transparent retry after a dropped connection) while making it configurable
/// via [`PostgresSink::with_retry`]. A zero base delay keeps the happy path
/// latency unchanged; callers facing real flakiness raise `attempts` and
/// `base_delay`.
pub const DEFAULT_PG_RETRY: crate::httpclient::RetryPolicy = crate::httpclient::RetryPolicy {
    attempts: 2,
    base_delay: std::time::Duration::ZERO,
    max_delay: std::time::Duration::ZERO,
};

/// Whether a failed `COPY` is worth retrying.
///
/// Classified from the server's SQLSTATE, which is the stable, documented way
/// to tell a transient condition from a permanent one. The classes that mean
/// "the server is busy or the connection broke" retry; everything else (syntax
/// error, unknown column, type mismatch, constraint violation, and every other
/// permanent fault) is surfaced immediately, because retrying it can only
/// produce the same failure.
///
/// `08` is `connection_exception`, `40` is `transaction_rollback`, `53` is
/// `insufficient_resources`, `57` is `operator_intervention` (includes
/// `query_canceled` and `admin_shutdown`), and `58` is `system_error` (includes
/// the `08` family some servers report through it). `40001` is
/// `serialization_failure` and `40P01` is `deadlock_detected` — both transient
/// under concurrency.
fn is_transient_postgres_error(detail: &str) -> bool {
    // A connection-level failure never got a SQLSTATE at all; the driver
    // reports it as a closed/broken connection.
    if detail.contains("connection") || detail.contains("Connection") {
        return true;
    }
    // `db error: <CODE>: <message>` is how tokio-postgres renders a DbError.
    let Some(code) = detail
        .split("db error: ")
        .nth(1)
        .and_then(|rest| rest.split(':').next())
        .map(str::trim)
    else {
        return false;
    };
    matches!(code, "40001" | "40P01")
        || code.starts_with("08")
        || code.starts_with("40")
        || code.starts_with("53")
        || code.starts_with("57")
        || code.starts_with("58")
}

pub struct PostgresSink {
    conn_string: String,
    table: String,
    drop_existing: bool,
    client: Option<tokio_postgres::Client>,
    driver: Option<tokio::task::JoinHandle<()>>,
    rows_written: u64,
    retry: crate::httpclient::RetryPolicy,
}

impl PostgresSink {
    pub fn new(conn_string: impl Into<String>, table: impl Into<String>) -> Self {
        PostgresSink {
            conn_string: conn_string.into(),
            table: table.into(),
            drop_existing: false,
            client: None,
            driver: None,
            rows_written: 0,
            retry: DEFAULT_PG_RETRY,
        }
    }

    pub fn overwrite(mut self) -> Self {
        self.drop_existing = true;
        self
    }

    /// Retry a failed `COPY` after reconnecting, per `policy`.
    ///
    /// Defaults to [`DEFAULT_PG_RETRY`] — one immediate reconnect, which is
    /// the long-standing behavior: pipelines routinely outlive an idle TCP hop
    /// (NAT gateway, connection pooler in front of Postgres), and one
    /// transparent retry covers it without adding latency to the happy path.
    /// Raise `attempts` and give `base_delay` a value for real network
    /// flakiness; pass [`crate::httpclient::RetryPolicy::none`] to disable
    /// retrying entirely.
    #[must_use]
    pub fn with_retry(mut self, policy: crate::httpclient::RetryPolicy) -> Self {
        self.retry = policy;
        self
    }

    pub fn rows_written(&self) -> u64 {
        self.rows_written
    }

    /// Drop the current client and stop its driver task, so the next
    /// [`connect`](Self::connect) starts from a clean socket.
    fn reset_connection(&mut self) {
        self.client = None;
        if let Some(driver) = self.driver.take() {
            driver.abort();
        }
    }

    async fn connect(&mut self, batch: &RecordBatch) -> Result<()> {
        let (client, conn) = tokio_postgres::connect(&self.conn_string, tokio_postgres::NoTls)
            .await
            .map_err(|e| Error::Database(format!("postgres connect: {e}")))?;
        self.driver = Some(tokio::spawn(async move {
            // Drive the connection; its result is reported through request failures.
            let _ = conn.await;
        }));

        if self.drop_existing {
            client
                .batch_execute(&format!(
                    "DROP TABLE IF EXISTS {};",
                    quote_ident(&self.table)
                ))
                .await
                .map_err(|e| Error::Database(format!("postgres drop table: {e}")))?;
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
        client
            .batch_execute(&ddl)
            .await
            .map_err(|e| Error::Database(format!("postgres create table: {e}")))?;
        self.client = Some(client);
        Ok(())
    }
}

impl std::fmt::Debug for PostgresSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PostgresSink")
            .field("conn_string", &self.conn_string)
            .field("table", &self.table)
            .field("rows_written", &self.rows_written)
            .finish()
    }
}

#[async_trait::async_trait]
impl crate::sink::Sink for PostgresSink {
    async fn write_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        if batch.num_rows() == 0 {
            return Ok(());
        }
        let policy = self.retry;
        let attempts = policy.attempts.max(1);
        let mut attempt = 1;
        let mut first_failure: Option<String> = None;

        loop {
            // (Re)connect on the first attempt, and after any failure that
            // leaves us without a usable client.
            if self.client.is_none() {
                self.connect(batch).await?;
            }

            match self.copy_batch(batch).await {
                Ok(()) => {
                    self.rows_written += batch.num_rows() as u64;
                    return Ok(());
                }
                Err(e) => {
                    // A permanent failure (bad column, type mismatch,
                    // constraint violation) will fail identically on every
                    // attempt, so retrying just burns time and connections.
                    let detail = e.to_string();
                    let retryable = is_transient_postgres_error(&detail);
                    if attempt >= attempts || !retryable {
                        return Err(match first_failure {
                            Some(first) => Error::Database(format!(
                                "postgres copy: {detail}; first failure: {first}"
                            )),
                            None => e,
                        });
                    }
                    first_failure.get_or_insert(detail);

                    let delay = policy.backoff(attempt);
                    crate::trace_event!(
                        "tpt_stream_core::postgres",
                        crate::telemetry::Level::INFO,
                        table = self.table.as_str(),
                        attempt,
                        delay_ms = delay.as_millis() as u64,
                        "retrying postgres COPY after failure"
                    );
                    if !delay.is_zero() {
                        tokio::time::sleep(delay).await;
                    }
                    // Force a fresh connection: a COPY that failed because the
                    // socket died cannot succeed on the same dead client.
                    self.reset_connection();
                    attempt += 1;
                }
            }
        }
    }

    async fn finish(&mut self) -> Result<()> {
        // Nothing to flush: each batch was committed by its own COPY.
        // Shut down the connection driver task.
        self.client = None;
        if let Some(driver) = self.driver.take() {
            driver.abort();
        }
        Ok(())
    }
}

impl PostgresSink {
    /// Bulk-load one batch with a single `COPY ... FROM STDIN` statement.
    async fn copy_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        let client = self.client.as_mut().expect("connected");
        let names: Vec<String> = batch.column_names().into_iter().map(quote_ident).collect();
        let copy_sql = format!(
            "COPY {} ({}) FROM STDIN WITH (FORMAT text)",
            quote_ident(&self.table),
            names.join(", ")
        );
        // Encode the whole batch into one text-format payload, then stream it
        // through a single COPY statement (one round trip per batch).
        let mut payload = String::new();
        for row in 0..batch.num_rows() {
            for (i, column) in batch.columns().iter().enumerate() {
                if i > 0 {
                    payload.push('\t');
                }
                payload.push_str(&copy_field(&column.get(row).unwrap_or(Value::Null)));
            }
            payload.push('\n');
        }
        let copy_sink = client
            .copy_in(&copy_sql)
            .await
            .map_err(|e| Error::Database(format!("postgres copy in: {e}")))?;
        tokio::pin!(copy_sink);
        copy_sink
            .as_mut()
            .send(Bytes::from(payload))
            .await
            .map_err(|e| Error::Database(format!("postgres copy send: {e}")))?;
        copy_sink
            .finish()
            .await
            .map_err(|e| Error::Database(format!("postgres copy finish: {e}")))?;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Source
// ---------------------------------------------------------------------------

pub struct PostgresSource {
    reader: StreamingReader,
    chunk_rows: usize,
}

impl PostgresSource {
    pub fn open(conn_string: impl Into<String>, query: impl Into<String>) -> Self {
        PostgresSource::open_with_chunk_size(conn_string, query, crate::DEFAULT_CHUNK_ROWS)
    }

    pub fn open_with_chunk_size(
        conn_string: impl Into<String>,
        query: impl Into<String>,
        chunk_rows: usize,
    ) -> Self {
        let conn_string = conn_string.into();
        let query = query.into();
        let (reader, handle) = StreamingReader::spawn(move |tx| {
            postgres_read_loop(tx, &conn_string, &query, chunk_rows)
        });
        let _ = handle;
        PostgresSource { reader, chunk_rows }
    }

    pub fn with_chunk_size(mut self, rows: usize) -> Self {
        self.chunk_rows = rows;
        self
    }
}

impl std::fmt::Debug for PostgresSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PostgresSource")
            .field("chunk_rows", &self.chunk_rows)
            .finish()
    }
}

fn postgres_read_loop(
    tx: &tokio::sync::mpsc::UnboundedSender<BatchResult>,
    conn_string: &str,
    query: &str,
    chunk_rows: usize,
) {
    let runtime = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(r) => r,
        Err(e) => {
            let _ = tx.send(Err(Error::Database(format!("postgres runtime: {e}"))));
            return;
        }
    };
    let (client, conn) =
        match runtime.block_on(tokio_postgres::connect(conn_string, tokio_postgres::NoTls)) {
            Ok(v) => v,
            Err(e) => {
                let _ = tx.send(Err(Error::Database(format!("postgres connect: {e}"))));
                return;
            }
        };
    // Spawn the connection driver and leak it; the runtime will clean up on drop.
    runtime.spawn(conn);

    let stmt = match runtime.block_on(client.prepare(query)) {
        Ok(s) => s,
        Err(e) => {
            let _ = tx.send(Err(Error::Database(format!("postgres prepare: {e}"))));
            return;
        }
    };
    let column_count = stmt.columns().len();
    if column_count == 0 {
        let _ = tx.send(Err(Error::Schema("query returned no columns".into())));
        return;
    }
    let names: Vec<String> = stmt
        .columns()
        .iter()
        .map(|c| c.name().to_string())
        .collect();
    let declared: Vec<DataType> = stmt
        .columns()
        .iter()
        .map(|c| pg_type_to_data_type(c.type_()))
        .collect();

    let params: [&(dyn tokio_postgres::types::ToSql + Sync); 0] = [];
    let rows_iter = match runtime.block_on(client.query_raw(&stmt, params)) {
        Ok(s) => s,
        Err(e) => {
            let _ = tx.send(Err(Error::Database(format!("postgres query: {e}"))));
            return;
        }
    };

    let mut rows: Vec<Vec<Value>> = Vec::with_capacity(chunk_rows);
    // query_raw returns a RowStream (a Stream, not an Iterator, and !Unpin);
    // pin it and poll with block_on since we're marshalling from a
    // synchronous reader thread.
    tokio::pin!(rows_iter);
    while let Some(row_result) = runtime.block_on(rows_iter.as_mut().next()) {
        let row = match row_result {
            Ok(r) => r,
            Err(e) => {
                let _ = tx.send(Err(Error::Database(format!("postgres read row: {e}"))));
                return;
            }
        };
        let mut values = Vec::with_capacity(column_count);
        for (i, declared_type) in declared.iter().enumerate() {
            values.push(read_cell(&row, i, declared_type));
        }
        rows.push(values);
        if rows.len() >= chunk_rows {
            match build_pg_batch(&names, &declared, &rows) {
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
        match build_pg_batch(&names, &declared, &rows) {
            Ok(batch) => {
                let _ = tx.send(Ok(batch));
            }
            Err(e) => {
                let _ = tx.send(Err(e));
            }
        }
    }
}

/// Extract one cell, trusting the declared column type first and falling
/// back to a string cast for types that don't map to a primitive (dates,
/// numerics, JSON, ... are read as text).
fn read_cell(row: &tokio_postgres::Row, i: usize, data_type: &DataType) -> Value {
    macro_rules! typed {
        ($ty:ty, $variant:ident) => {
            if let Ok(v) = row.try_get::<_, Option<$ty>>(i) {
                return v.map(Value::$variant).unwrap_or(Value::Null);
            }
        };
    }
    match data_type {
        DataType::Bool => typed!(bool, Bool),
        DataType::Int32 => typed!(i32, Int32),
        DataType::Int64 => typed!(i64, Int64),
        DataType::Float32 => typed!(f32, Float32),
        DataType::Float64 => typed!(f64, Float64),
        DataType::Date => {
            if let Ok(v) = row.try_get::<_, Option<PgDate>>(i) {
                return v.map(|d| Value::Date(d.0)).unwrap_or(Value::Null);
            }
        }
        DataType::Timestamp => {
            if let Ok(v) = row.try_get::<_, Option<PgTimestamp>>(i) {
                return v.map(|t| Value::Timestamp(t.0)).unwrap_or(Value::Null);
            }
        }
        DataType::Utf8 => {}
    }
    if let Ok(v) = row.try_get::<_, Option<String>>(i) {
        return v.map(Value::Utf8).unwrap_or(Value::Null);
    }
    Value::Null
}

fn build_pg_batch(
    names: &[String],
    declared: &[DataType],
    rows: &[Vec<Value>],
) -> Result<RecordBatch> {
    // `declared` comes from the query's column metadata (see `pg_type_to_data_type`),
    // which is authoritative; `read_cell` already produced values in that type, so
    // this just carries the schema through without re-inferring it from the values
    // (re-inferring previously widened e.g. INT4 to Int64 via `infer_from_value`).
    let columns = names
        .iter()
        .zip(declared)
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

#[async_trait::async_trait]
impl crate::source::Source for PostgresSource {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_policy_is_one_immediate_reconnect() {
        // Matches the sink's historical behavior: exactly one retry, no delay.
        assert_eq!(DEFAULT_PG_RETRY.attempts, 2);
        assert_eq!(DEFAULT_PG_RETRY.base_delay, std::time::Duration::ZERO);
        assert_eq!(DEFAULT_PG_RETRY.backoff(1), std::time::Duration::ZERO);
        assert_eq!(DEFAULT_PG_RETRY.backoff(5), std::time::Duration::ZERO);
    }

    #[test]
    fn new_sink_uses_the_default_policy() {
        let sink = PostgresSink::new("postgres://localhost/x", "t");
        assert_eq!(sink.retry, DEFAULT_PG_RETRY);
    }

    #[test]
    fn with_retry_overrides_the_policy() {
        let policy = crate::httpclient::RetryPolicy::new(
            5,
            std::time::Duration::from_millis(200),
            std::time::Duration::from_secs(2),
        );
        let sink = PostgresSink::new("postgres://localhost/x", "t").with_retry(policy);
        assert_eq!(sink.retry.attempts, 5);
        assert_eq!(sink.retry.base_delay, std::time::Duration::from_millis(200));
    }

    #[test]
    fn with_retry_none_disables_retrying() {
        let sink = PostgresSink::new("postgres://localhost/x", "t")
            .with_retry(crate::httpclient::RetryPolicy::none());
        assert_eq!(sink.retry.attempts, 1, "a single attempt means no retry");
    }

    #[test]
    fn connection_failures_are_transient() {
        for detail in [
            "db error: connection closed",
            "connection reset by peer",
            "Connection closed",
            "terminating connection due to administrator command",
        ] {
            assert!(
                is_transient_postgres_error(detail),
                "should retry: {detail:?}"
            );
        }
    }

    #[test]
    fn transient_sqlstates_are_retried() {
        // 08 connection_exception, 40001 serialization_failure,
        // 40P01 deadlock_detected, 53000/53300 insufficient resources,
        // 57014 query_canceled, 58000 system error.
        for code in [
            "08006", "08003", "08001", "40001", "40P01", "53000", "53300", "57014", "58030",
        ] {
            let detail = format!("db error: {code}: something happened");
            assert!(
                is_transient_postgres_error(&detail),
                "{code} should be transient"
            );
        }
    }

    #[test]
    fn permanent_sqlstates_are_not_retried() {
        // 42xxx syntax/access rule, 22xxx data exception, 23xxx integrity
        // constraint, 42703 undefined column, 23505 duplicate key.
        for code in [
            "42601", "42501", "42P01", "22003", "23505", "23502", "42703", "22P02",
        ] {
            let detail = format!("db error: {code}: permanent problem");
            assert!(
                !is_transient_postgres_error(&detail),
                "{code} must not be retried"
            );
        }
    }

    #[test]
    fn unparseable_errors_default_to_permanent() {
        // When we cannot tell, do not retry: a wrong retry silently duplicates
        // work, while a missing one just surfaces the error sooner.
        for detail in ["", "something went wrong", "db error: no code here"] {
            assert!(!is_transient_postgres_error(detail), "{detail:?}");
        }
    }

    #[test]
    fn retry_backoff_is_capped_for_a_configured_policy() {
        let policy = crate::httpclient::RetryPolicy::new(
            10,
            std::time::Duration::from_millis(50),
            std::time::Duration::from_millis(400),
        );
        for attempt in 1..=10 {
            assert!(
                policy.backoff(attempt) <= std::time::Duration::from_millis(400),
                "attempt {attempt} exceeded the cap"
            );
        }
    }
}
