use crate::table::RecordBatch;

// Re-export for back-compat with `crate::pipeline::{Error, Result}` imports.
pub use crate::error::{Error, PipelineStats, Result};

/// Split `batch` into two halves at `mid` rows, returning `(left, right)`.
///
/// Used by the dead-letter queue to narrow down which row a stage choked on.
/// Cheap relative to re-running the stage, and each half keeps the full schema
/// so the stage sees exactly the shape it would have seen for the whole batch.
fn split_batch(batch: &RecordBatch, mid: usize) -> (RecordBatch, RecordBatch) {
    let mut left = batch.clone();
    left.truncate(mid);
    let mut right = batch.clone();
    let total = batch.num_rows();
    // Drop the first `mid` rows by keeping the tail. `retain_rows` takes a
    // per-row keep mask, which avoids needing a slice API on `Column`.
    let keep: Vec<bool> = (0..total).map(|i| i >= mid).collect();
    for column in right.columns_mut() {
        column.retain_rows(&keep);
    }
    right.recompute_row_count();
    (left, right)
}

#[async_trait::async_trait]
pub trait PipelineStage: Send {
    fn name(&self) -> &'static str {
        std::any::type_name::<Self>()
            .rsplit("::")
            .next()
            .unwrap_or("?")
    }

    async fn process(&mut self, batch: RecordBatch) -> Result<Vec<RecordBatch>>;

    /// Called once after the source is exhausted. Stages that buffer state
    /// across chunks (aggregation, external sort, hash join) release their
    /// tail here. Default: nothing to flush.
    async fn finish(&mut self) -> Result<Vec<RecordBatch>> {
        Ok(Vec::new())
    }
}

pub struct Pipeline {
    source: Option<Box<dyn crate::source::Source>>,
    stages: Vec<Box<dyn PipelineStage>>,
    sink: Option<Box<dyn crate::sink::Sink>>,
    chunk_rows: usize,
    pending_error: Option<String>,
    telemetry: Option<crate::telemetry::ProgressHook>,
    stage_metrics: Vec<crate::telemetry::StageMetrics>,
    error_policy: crate::source::ErrorPolicy,
    dead_letter: Option<DeadLetterQueue>,
    executed: bool,
}

impl Default for Pipeline {
    fn default() -> Self {
        Self::new()
    }
}

/// Writer for rows a *stage* rejected, so a run can finish with the bad rows
/// preserved instead of aborting.
///
/// Distinct from `ErrorPolicy::Quarantine`, which captures malformed *input*
/// rows at the source. This one captures rows that parsed fine but failed once
/// a stage touched them (e.g. a `map` closure that returns the wrong width for
/// one row). Both can be active at once.
struct DeadLetterQueue {
    writer: tpt_csv::Writer<std::fs::File>,
    stage: String,
    rows: u64,
    header_written: bool,
}

impl DeadLetterQueue {
    /// Create the file. The header row is written lazily by
    /// [`DeadLetterQueue::write`], once the failing row's schema is known.
    fn open(path: &str, stage: &str) -> Result<Self> {
        let file = std::fs::File::create(path)
            .map_err(|e| Error::Io(std::io::Error::new(e.kind(), format!("{path}: {e}"))))?;
        let writer = tpt_csv::WriterBuilder::new()
            .flexible(true)
            .from_writer(file);
        Ok(DeadLetterQueue {
            writer,
            stage: stage.to_string(),
            rows: 0,
            header_written: false,
        })
    }

    fn write(&mut self, batch: &RecordBatch, error: &str) -> Result<()> {
        // The row schema is only known once a bad row actually shows up, so
        // the header is written lazily, on the first capture. A run that
        // captures nothing therefore leaves a zero-byte file, which is easier
        // to interpret than a header for a schema nobody ever saw.
        if !self.header_written {
            let mut header: Vec<String> =
                vec!["_dead_letter_stage".to_string(), "_error".to_string()];
            header.extend(batch.column_names().iter().map(|c| c.to_string()));
            self.writer
                .write_record(header.iter().map(String::as_str))
                .map_err(|e| Error::Other(format!("dead-letter header: {e}")))?;
            self.header_written = true;
        }
        for i in 0..batch.num_rows() {
            let mut fields: Vec<String> = Vec::with_capacity(batch.num_columns() + 2);
            fields.push(self.stage.clone());
            fields.push(error.to_string());
            for c in 0..batch.num_columns() {
                fields.push(
                    batch
                        .column_by_index(c)
                        .and_then(|col| col.get(i))
                        .map(|v| v.to_string())
                        .unwrap_or_default(),
                );
            }
            self.writer
                .write_record(fields.iter().map(String::as_str))
                .map_err(|e| Error::Other(format!("dead-letter write: {e}")))?;
            self.rows += 1;
        }
        crate::trace_event!(
            "tpt_stream_core::dead_letter",
            crate::telemetry::Level::INFO,
            stage = self.stage.as_str(),
            rows = batch.num_rows(),
            error,
            "captured rows rejected by stage"
        );
        Ok(())
    }
}

impl Pipeline {
    pub fn new() -> Self {
        Pipeline {
            source: None,
            stages: Vec::new(),
            sink: None,
            chunk_rows: crate::DEFAULT_CHUNK_ROWS,
            pending_error: None,
            telemetry: None,
            stage_metrics: Vec::new(),
            error_policy: crate::source::ErrorPolicy::default(),
            dead_letter: None,
            executed: false,
        }
    }

    /// Set how sources built after this call handle malformed input rows
    /// (CSV field-count mismatches, undecodable JSONL lines): abort
    /// (`Strict`, default), drop them (`Skip`), or drop and capture them
    /// (`Quarantine(path)`).
    pub fn on_error(&mut self, policy: crate::source::ErrorPolicy) -> &mut Self {
        self.error_policy = policy;
        self
    }

    /// Capture rows that a *stage* rejects to `path` instead of aborting the
    /// run, so the pipeline finishes and nothing is lost silently.
    ///
    /// When a stage fails on a batch, the offending row is isolated by
    /// re-running the stage over progressively smaller halves, then written to
    /// `path` as CSV with two leading columns (`_dead_letter_stage`,
    /// `_error`) followed by the row's own fields. Good rows continue through
    /// the pipeline untouched.
    ///
    /// This is the stage-level counterpart to `ErrorPolicy::Quarantine`, which
    /// captures malformed *input* rows at the source; the two are independent
    /// and can both be enabled.
    ///
    /// Cost: a batch with no bad rows costs one extra stage call. Stages that
    /// carry state across batches (aggregation, sort, join, dedup) are
    /// inherently not row-independent, so a narrowing retry re-runs the stage
    /// over a prefix of the same input — see the caveat in the module docs.
    pub fn dead_letter(&mut self, path: impl Into<String>) -> Result<&mut Self> {
        self.dead_letter = Some(DeadLetterQueue::open(&path.into(), "stage")?);
        Ok(self)
    }

    /// Rows captured by the dead-letter queue so far (0 if none is attached).
    pub fn dead_letter_rows(&self) -> u64 {
        self.dead_letter.as_ref().map_or(0, |q| q.rows)
    }

    /// Human-readable stage plan, e.g. `csv source -> filter -> csv sink`.
    pub fn explain(&self) -> String {
        let mut parts: Vec<String> = Vec::new();
        parts.push("source".to_string());
        for stage in &self.stages {
            parts.push(stage.name().to_string());
        }
        if self.sink.is_some() {
            parts.push("sink".to_string());
        }
        parts.join(" -> ")
    }

    /// Read the first `rows` output rows through the attached stages without
    /// running the whole pipeline. Consumes the source (the pipeline must be
    /// rebuilt afterwards, like after `execute`). Sinks are not touched.
    ///
    /// Buffering stages (external sort, dedup, hash join, group-by) emit
    /// nothing until the source is exhausted, so once the source runs dry the
    /// stage tails are drained in pipeline order — the same finalization
    /// [`Pipeline::execute`] performs — until `rows` output rows are available.
    pub async fn preview(&mut self, rows: usize) -> Result<Vec<RecordBatch>> {
        if let Some(err) = self.pending_error.take() {
            return Err(Error::Schema(err));
        }
        if self.executed {
            return Err(Error::Config(
                "pipeline already executed; sources are one-shot — rebuild it".into(),
            ));
        }
        let mut source = self
            .source
            .take()
            .ok_or_else(|| Error::Config("pipeline has no source".into()))?;
        let mut collected: Vec<RecordBatch> = Vec::new();
        let mut count = 0usize;
        // `true` once the source signalled EOF (as opposed to `rows` already
        // being satisfied by streaming stages), which is when buffered stage
        // tails become available.
        let mut exhausted = false;
        while count < rows {
            let Some(batch) = source.next_batch().await? else {
                exhausted = true;
                break;
            };
            let mut current = vec![batch];
            for stage in self.stages.iter_mut() {
                let mut next = Vec::new();
                for b in current {
                    next.extend(stage.process(b).await?);
                }
                current = next;
            }
            for mut b in current {
                if count >= rows {
                    break;
                }
                let take = rows - count;
                if b.num_rows() > take {
                    b.truncate(take);
                }
                count += b.num_rows();
                collected.push(b);
            }
        }

        if exhausted && count < rows {
            #[allow(clippy::needless_range_loop)]
            for i in 0..self.stages.len() {
                if count >= rows {
                    break;
                }
                let mut current = self.stages[i].finish().await?;
                for stage in self.stages.iter_mut().skip(i + 1) {
                    let mut next = Vec::new();
                    for b in current {
                        next.extend(stage.process(b).await?);
                    }
                    current = next;
                }
                for mut b in current {
                    if count >= rows {
                        break;
                    }
                    let take = rows - count;
                    if b.num_rows() > take {
                        b.truncate(take);
                    }
                    count += b.num_rows();
                    collected.push(b);
                }
            }
        }
        self.executed = true;
        Ok(collected)
    }

    /// Attach a telemetry hook invoked on every source batch, stage output,
    /// sink write, and at completion (see [`crate::telemetry::TelemetryEvent`]).
    pub fn on_progress(&mut self, hook: crate::telemetry::ProgressHook) -> &mut Self {
        self.telemetry = Some(hook);
        self
    }

    /// Cumulative per-stage metrics from the last `execute()` run
    /// (empty before execution; one entry per stage, in pipeline order).
    pub fn stage_stats(&self) -> &[crate::telemetry::StageMetrics] {
        &self.stage_metrics
    }

    pub fn with_chunk_size(&mut self, rows: usize) -> &mut Self {
        self.chunk_rows = rows;
        self
    }

    pub fn source(&mut self, source: impl crate::source::Source + 'static) -> &mut Self {
        self.source = Some(Box::new(source));
        self
    }

    pub fn read_csv(&mut self, path: impl Into<String>) -> &mut Self {
        self.source(
            crate::source::CsvSource::open_with_chunk_size(path, self.chunk_rows)
                .with_error_policy(self.error_policy.clone()),
        );
        self
    }

    pub fn read_jsonl(&mut self, path: impl Into<String>) -> &mut Self {
        self.source(
            crate::source::JsonlSource::open_with_chunk_size(path, self.chunk_rows)
                .with_error_policy(self.error_policy.clone()),
        );
        self
    }

    pub fn read_json(&mut self, path: impl Into<String>) -> &mut Self {
        self.source(crate::source::JsonArraySource::open_with_chunk_size(
            path,
            self.chunk_rows,
        ));
        self
    }

    pub fn read_columnar(&mut self, path: impl Into<String>) -> &mut Self {
        self.source(crate::source::ColumnarSource::open(path));
        self
    }

    pub fn stage(&mut self, stage: impl PipelineStage + 'static) -> &mut Self {
        self.stages.push(Box::new(stage));
        self
    }

    pub fn filter<F>(&mut self, predicate: F) -> &mut Self
    where
        F: FnMut(crate::row::Row) -> bool + Send + 'static,
    {
        self.stage(crate::transform::Filter::new(predicate))
    }

    pub fn map<F>(&mut self, columns: &[&str], mapper: F) -> &mut Self
    where
        F: FnMut(crate::row::Row) -> Vec<crate::value::Value> + Send + 'static,
    {
        let cols: Vec<String> = columns.iter().map(|s| s.to_string()).collect();
        self.stage(crate::transform::Map::new(cols, mapper))
    }

    pub fn select(&mut self, columns: &[&str]) -> &mut Self {
        let cols: Vec<String> = columns.iter().map(|s| s.to_string()).collect();
        self.stage(crate::transform::Select::keep(cols))
    }

    /// Filter rows with a row-expression string (see `tpt_stream_core::expr`).
    /// Includes the row when the expression evaluates to boolean true.
    /// Invalid expressions surface as a schema error at `execute()`.
    pub fn filter_expr(&mut self, expr: &str) -> &mut Self {
        match crate::expr::parse(expr) {
            Ok(parsed) => {
                self.stage(crate::transform::Filter::new(move |row| {
                    parsed.matches(&row)
                }));
            }
            Err(err) => {
                self.pending_error = Some(format!("filter expression: {err}"));
            }
        }
        self
    }

    /// Map a list of `(output_column, expression)` pairs, evaluated per row.
    /// Invalid expressions surface as a schema error at `execute()`.
    pub fn map_expr(&mut self, columns: &[(&str, &str)]) -> &mut Self {
        let parsed: Vec<(String, crate::expr::Expr)> = columns
            .iter()
            .map(|(name, expr)| match crate::expr::parse(expr) {
                Ok(e) => (name.to_string(), e),
                Err(err) => {
                    self.pending_error = Some(format!("map expression: {err}"));
                    (name.to_string(), crate::expr::Expr::Null)
                }
            })
            .collect();
        let output: Vec<String> = parsed.iter().map(|(n, _)| n.clone()).collect();
        self.stage(crate::transform::Map::new(output, move |row| {
            parsed.iter().map(|(_, expr)| expr.eval(&row)).collect()
        }))
    }

    /// One-shot aggregate: `group_by` columns then aggregate specs.
    pub fn aggregate(&mut self, group_by: &[&str], specs: &[crate::agg::AggSpec]) -> &mut Self {
        let keys: Vec<String> = group_by.iter().map(|s| s.to_string()).collect();
        self.stage(crate::agg::GroupByAgg::new(keys, specs.to_vec()))
    }

    /// Sort all rows by the given columns (ascending). Buffers in memory up to
    /// the spill threshold, then externally merges sorted runs.
    pub fn sort_by(&mut self, columns: &[&str]) -> &mut Self {
        let cols: Vec<String> = columns.iter().map(|s| s.to_string()).collect();
        let desc = vec![false; cols.len()];
        self.stage(crate::sort::Sort::new(cols, desc))
    }

    pub fn sort_by_desc(&mut self, columns: &[&str]) -> &mut Self {
        let cols: Vec<String> = columns.iter().map(|s| s.to_string()).collect();
        let desc = vec![true; cols.len()];
        self.stage(crate::sort::Sort::new(cols, desc))
    }

    /// Attach data-quality checks (see `tpt_stream_core::expect::Check`).
    /// A violation aborts `execute()` with `Error::DataQuality`.
    pub fn expect_checks(&mut self, checks: Vec<crate::expect::Check>) -> &mut Self {
        self.stage(crate::expect::Expect::new(checks))
    }

    /// Pass through only the first `n` rows (pipeline `LIMIT`).
    pub fn limit(&mut self, n: usize) -> &mut Self {
        self.stage(crate::transform::Limit::new(n))
    }

    /// Drop rows whose identity columns (or whole row if `columns` is empty)
    /// have already been seen in this pipeline.
    pub fn dedup(&mut self, columns: &[&str]) -> &mut Self {
        let cols: Vec<String> = columns.iter().map(|s| s.to_string()).collect();
        self.stage(crate::dedup::Deduplicate::new(cols))
    }

    /// Hash join against an in-memory right relation built from `right_batches`.
    /// The join output is: left columns followed by right columns (colliding
    /// right column names get a `_r` suffix). Build side uses `right_keys`,
    /// probe side uses `left_keys`; null keys never match.
    pub fn join(
        &mut self,
        right_batches: Vec<RecordBatch>,
        left_keys: &[&str],
        right_keys: &[&str],
        join_type: crate::join::JoinType,
    ) -> Result<&mut Self> {
        let stage = self.build_join(right_batches, left_keys, right_keys, join_type)?;
        Ok(self.stage(stage))
    }

    /// Like [`Pipeline::join`](Self::join) but loads the right relation from a
    /// CSV file (same type inference as the CSV source).
    pub fn join_csv(
        &mut self,
        right_path: &str,
        left_keys: &[&str],
        right_keys: &[&str],
        join_type: crate::join::JoinType,
    ) -> Result<&mut Self> {
        let batches = self.load_csv_relation(right_path)?;
        self.join(batches, left_keys, right_keys, join_type)
    }

    fn build_join(
        &self,
        right_batches: Vec<RecordBatch>,
        left_keys: &[&str],
        right_keys: &[&str],
        join_type: crate::join::JoinType,
    ) -> Result<crate::join::HashJoin> {
        let first = right_batches
            .first()
            .ok_or_else(|| Error::Schema("join build side is empty".into()))?;
        let schema = first.schema();
        for key in right_keys {
            if !schema.iter().any(|(n, _)| n == key) {
                return Err(Error::Schema(format!(
                    "join build key column '{key}' not found (build has {})",
                    first.column_names().join(", ")
                )));
            }
        }
        let mut stage = crate::join::HashJoin::new(
            left_keys.iter().map(|s| s.to_string()).collect(),
            right_keys.iter().map(|s| s.to_string()).collect(),
            schema,
            join_type,
        );
        for batch in &right_batches {
            for ri in 0..batch.num_rows() {
                let row: Vec<crate::value::Value> = batch
                    .columns()
                    .iter()
                    .map(|c| c.get(ri).unwrap_or(crate::value::Value::Null))
                    .collect();
                stage.add_build_row(row)?;
            }
        }
        Ok(stage)
    }

    fn load_csv_relation(&self, path: &str) -> Result<Vec<RecordBatch>> {
        crate::source::read_csv_batches(path, crate::DEFAULT_CHUNK_ROWS)
    }

    pub fn sink(&mut self, sink: impl crate::sink::Sink + 'static) -> &mut Self {
        self.sink = Some(Box::new(sink));
        self
    }

    pub fn write_csv(&mut self, path: impl Into<String>) -> &mut Self {
        self.sink(crate::sink::CsvSink::open(path))
    }

    pub fn write_jsonl(&mut self, path: impl Into<String>) -> &mut Self {
        self.sink(crate::sink::JsonlSink::open(path))
    }

    pub fn write_json(&mut self, path: impl Into<String>, pretty: bool) -> &mut Self {
        self.sink(crate::sink::JsonArraySink::open(path, pretty))
    }

    pub fn write_columnar(&mut self, path: impl Into<String>, use_zstd: bool) -> &mut Self {
        self.sink(crate::sink::ColumnarSink::open(path, use_zstd))
    }

    /// Stream the result of a SQLite SELECT query (feature `sqlite`).
    #[cfg(feature = "sqlite")]
    pub fn read_sqlite(&mut self, path: impl Into<String>, query: impl Into<String>) -> &mut Self {
        self.source(crate::sqlite::SqliteSource::open(path, query));
        self
    }

    /// Write batches into a SQLite table, created from the first batch's
    /// schema if missing (feature `sqlite`).
    #[cfg(feature = "sqlite")]
    pub fn write_sqlite(&mut self, path: impl Into<String>, table: impl Into<String>) -> &mut Self {
        self.sink(crate::sqlite::SqliteSink::open(path, table));
        self
    }

    /// Stream the result of a PostgreSQL SELECT query (feature `postgres`).
    #[cfg(feature = "postgres")]
    pub fn read_postgres(
        &mut self,
        conn_string: impl Into<String>,
        query: impl Into<String>,
    ) -> &mut Self {
        self.source(crate::postgres::PostgresSource::open(conn_string, query));
        self
    }

    /// Write batches into a PostgreSQL table, created from the first batch's
    /// schema if missing (feature `postgres`).
    #[cfg(feature = "postgres")]
    pub fn write_postgres(
        &mut self,
        conn_string: impl Into<String>,
        table: impl Into<String>,
    ) -> &mut Self {
        self.sink(crate::postgres::PostgresSink::new(conn_string, table));
        self
    }

    /// Like [`Pipeline::write_postgres`] but with a pre-built sink, so a caller
    /// can set options the shorthand has no argument for (e.g.
    /// [`crate::postgres::PostgresSink::with_retry`] or `overwrite()`).
    #[cfg(feature = "postgres")]
    pub fn write_postgres_sink(&mut self, sink: crate::postgres::PostgresSink) -> &mut Self {
        self.sink(sink);
        self
    }

    /// Stream an S3 (or S3-compatible) object as the source. The data format
    /// comes from the key extension: `.csv` (default), `.jsonl`/`.ndjson`,
    /// `.json`, `.tptcol` (feature `s3`).
    #[cfg(feature = "s3")]
    pub fn read_s3(
        &mut self,
        bucket_url: &str,
        key: &str,
        credentials: &crate::s3::CloudCredentials,
    ) -> Result<&mut Self> {
        let store = crate::s3::S3Store::new(bucket_url, credentials)?;
        self.read_s3_store(store, key);
        Ok(self)
    }

    /// Like [`Pipeline::read_s3`] but with a pre-built store, so a caller can
    /// apply settings (e.g. [`crate::s3::S3Store::with_retry`]) the shorthand
    /// has no argument for.
    #[cfg(feature = "s3")]
    pub fn read_s3_store(&mut self, store: crate::s3::S3Store, key: &str) -> &mut Self {
        self.source(crate::s3::S3Source::open(store, key));
        self
    }

    /// Upload the pipeline output to an S3 (or S3-compatible) object; small
    /// payloads are a single `PUT`, large ones use multipart upload (feature
    /// `s3`).
    #[cfg(feature = "s3")]
    pub fn write_s3(
        &mut self,
        bucket_url: &str,
        key: &str,
        credentials: &crate::s3::CloudCredentials,
    ) -> Result<&mut Self> {
        let store = crate::s3::S3Store::new(bucket_url, credentials)?;
        self.write_s3_store(store, key);
        Ok(self)
    }

    /// Like [`Pipeline::write_s3`] but with a pre-built store (see
    /// [`Pipeline::read_s3_store`]).
    #[cfg(feature = "s3")]
    pub fn write_s3_store(&mut self, store: crate::s3::S3Store, key: &str) -> &mut Self {
        self.sink(crate::s3::S3Sink::new(store, key));
        self
    }

    /// Stream a Google Cloud Storage object via the S3-compatible XML API
    /// (HMAC credentials) (feature `gcs`).
    #[cfg(feature = "gcs")]
    pub fn read_gcs(
        &mut self,
        bucket: &str,
        key: &str,
        credentials: &crate::s3::CloudCredentials,
    ) -> Result<&mut Self> {
        let store = crate::gcs::GcsStore::new(bucket, credentials)?;
        self.read_gcs_store(store, key);
        Ok(self)
    }

    /// Like [`Pipeline::read_gcs`] but with a pre-built store.
    #[cfg(feature = "gcs")]
    pub fn read_gcs_store(&mut self, store: crate::gcs::GcsStore, key: &str) -> &mut Self {
        self.source(crate::gcs::GcsSource::open(store, key));
        self
    }

    /// Upload the pipeline output to a Google Cloud Storage object (feature
    /// `gcs`).
    #[cfg(feature = "gcs")]
    pub fn write_gcs(
        &mut self,
        bucket: &str,
        key: &str,
        credentials: &crate::s3::CloudCredentials,
    ) -> Result<&mut Self> {
        let store = crate::gcs::GcsStore::new(bucket, credentials)?;
        self.write_gcs_store(store, key);
        Ok(self)
    }

    /// Like [`Pipeline::write_gcs`] but with a pre-built store.
    #[cfg(feature = "gcs")]
    pub fn write_gcs_store(&mut self, store: crate::gcs::GcsStore, key: &str) -> &mut Self {
        self.sink(crate::gcs::GcsSink::new(store, key));
        self
    }

    /// Stream a plain HTTP(S) URL as the source; the data format comes from
    /// the URL extension (feature `http`).
    #[cfg(feature = "http")]
    pub fn read_http(&mut self, url: impl Into<String>) -> &mut Self {
        self.source(crate::http::HttpSource::open_with_chunk_size(
            url,
            self.chunk_rows,
        ));
        self
    }

    /// Stream an Azure blob as the source (feature `azure`).
    #[cfg(feature = "azure")]
    pub fn read_azure_blob(
        &mut self,
        store: crate::azure::AzureBlobStore,
        key: impl Into<String>,
    ) -> &mut Self {
        self.source(crate::azure::AzureBlobSource::open(store, key));
        self
    }

    /// Upload the pipeline output to an Azure block blob (feature `azure`).
    #[cfg(feature = "azure")]
    pub fn write_azure_blob(
        &mut self,
        store: crate::azure::AzureBlobStore,
        key: impl Into<String>,
    ) -> &mut Self {
        self.sink(crate::azure::AzureBlobSink::new(store, key));
        self
    }

    pub fn num_stages(&self) -> usize {
        self.stages.len()
    }

    /// A parse error from the last `filter_expr`/`map_expr` call, if any.
    /// `execute()` surfaces it as an `Error::Schema`; this accessor lets
    /// language wrappers fail fast at construction time.
    pub fn pending_error(&self) -> Option<&str> {
        self.pending_error.as_deref()
    }

    /// Run the pipeline and return the resulting rows in memory instead of
    /// writing them to a sink (any attached sink is ignored).
    pub async fn collect(&mut self) -> Result<Vec<RecordBatch>> {
        self.run(Some(Box::new(MemorySink::default())))
            .await
            .map(|(_, batches)| batches)
    }

    pub async fn execute(&mut self) -> Result<PipelineStats> {
        self.run(None).await.map(|(stats, _)| stats)
    }

    /// Shared runner behind [`Pipeline::execute`] and [`Pipeline::collect`]:
    /// `sink_override` replaces the attached sink (or the absence of one).
    /// Returns the output batches when an in-memory sink was used.
    async fn run(
        &mut self,
        sink_override: Option<Box<dyn crate::sink::Sink>>,
    ) -> Result<(PipelineStats, Vec<RecordBatch>)> {
        if let Some(err) = self.pending_error.take() {
            return Err(Error::Schema(err));
        }
        if self.executed {
            return Err(Error::Config(
                "pipeline already executed; sources are one-shot — rebuild it".into(),
            ));
        }
        let mut source = self
            .source
            .take()
            .ok_or_else(|| Error::Config("pipeline has no source".into()))?;
        let overrode_sink = sink_override.is_some();
        let mut sink = Some(match sink_override {
            Some(sink) => sink,
            None => match self.sink.take() {
                Some(sink) => sink,
                None => Box::new(NullSink) as Box<dyn crate::sink::Sink>,
            },
        });
        let mut collected: Vec<RecordBatch> = Vec::new();
        let telemetry = self.telemetry.take();
        let mut stage_metrics = vec![crate::telemetry::StageMetrics::default(); self.stages.len()];
        let emit = |event: &crate::telemetry::TelemetryEvent| {
            // `tracing` and the progress hook are independent, so a subscriber
            // can use either or both.
            crate::telemetry::emit_tracing(event);
            if let Some(hook) = &telemetry {
                hook(event);
            }
        };
        let mut stats = PipelineStats::default();
        let mut sink_rows: u64 = 0;
        let started = std::time::Instant::now();

        loop {
            let batch = match source.next_batch().await? {
                Some(b) => b,
                None => break,
            };
            stats.rows += batch.num_rows() as u64;
            stats.batches += 1;
            emit(&crate::telemetry::TelemetryEvent::SourceBatch {
                rows: batch.num_rows() as u64,
                total_rows: stats.rows,
                batches: stats.batches,
            });

            let mut current = vec![batch];
            for (i, stage) in self.stages.iter_mut().enumerate() {
                let rows_in: u64 = current.iter().map(|b| b.num_rows() as u64).sum();
                let start = std::time::Instant::now();
                let mut next = Vec::new();
                for b in current {
                    // With no dead-letter queue a stage error aborts, exactly
                    // as before. With one, the bad row(s) are isolated,
                    // recorded, and the good rows still flow on.
                    match stage.process(b.clone()).await {
                        Ok(out) => next.extend(out),
                        Err(err) => {
                            let Some(dlq) = self.dead_letter.as_mut() else {
                                return Err(err);
                            };
                            dlq.stage = stage.name().to_string();
                            let recovered =
                                isolate_and_capture(stage.as_mut(), b, dlq, &err).await?;
                            next.extend(recovered);
                        }
                    }
                }
                let elapsed = start.elapsed();
                let rows_out: u64 = next.iter().map(|b| b.num_rows() as u64).sum();
                let stage_name = stage.name().to_string();
                let metrics = &mut stage_metrics[i];
                metrics.name = stage_name.clone();
                metrics.rows_in += rows_in;
                metrics.rows_out += rows_out;
                metrics.batches += 1;
                metrics.elapsed += elapsed;
                emit(&crate::telemetry::TelemetryEvent::StageBatch {
                    stage: stage_name,
                    rows_in,
                    rows_out,
                });
                current = next;
            }

            if let Some(sink) = sink.as_mut() {
                let mut written: u64 = 0;
                for b in &current {
                    sink.write_batch(b).await?;
                    written += b.num_rows() as u64;
                }
                sink_rows += written;
                emit(&crate::telemetry::TelemetryEvent::SinkBatch {
                    rows: written,
                    total_rows: sink_rows,
                });
            }
            collected.extend(current);
        }

        // Finalization: stages may buffer across chunks and emit tails once.
        // Drain each stage's tail through the downstream stages, in order, so
        // downstream tail-emitting stages receive upstream tails before their
        // own finish is invoked. Tail rows count toward stage metrics.
        #[allow(clippy::needless_range_loop)]
        for i in 0..self.stages.len() {
            let start = std::time::Instant::now();
            let tail = self.stages[i].finish().await?;
            let rows_in: u64 = tail.iter().map(|b| b.num_rows() as u64).sum();
            let mut current = tail;
            for stage in self.stages.iter_mut().skip(i + 1) {
                let mut next = Vec::new();
                for b in current {
                    next.extend(stage.process(b).await?);
                }
                current = next;
            }
            let elapsed = start.elapsed();
            let tail_rows_out: u64 = current.iter().map(|b| b.num_rows() as u64).sum();
            // An empty tail contributed no work; don't log a phantom batch.
            if rows_in > 0 || tail_rows_out > 0 {
                let metrics = &mut stage_metrics[i];
                metrics.rows_in += rows_in;
                metrics.rows_out += tail_rows_out;
                metrics.batches += 1;
                metrics.elapsed += elapsed;
            }
            if let Some(sink) = sink.as_mut() {
                for b in &current {
                    sink.write_batch(b).await?;
                }
            }
            collected.extend(current);
        }

        if let Some(sink) = sink.as_mut() {
            sink.finish().await?;
            stats.bytes_out = sink.bytes_out();
        }

        self.executed = true;
        stats.elapsed = started.elapsed();
        emit(&crate::telemetry::TelemetryEvent::Done {
            rows: stats.rows,
            batches: stats.batches,
            elapsed: stats.elapsed,
        });

        self.source = Some(source);
        if !overrode_sink {
            // A sink override (collect) drops the MemorySink; a normal run
            // hands the user's sink back for byte counters on repeat calls.
            self.sink = sink;
        }
        self.telemetry = telemetry;
        self.stage_metrics = stage_metrics;

        Ok((stats, collected))
    }
}

/// Isolate the rows `stage` chokes on and capture them to `dlq`, returning the
/// stage's output for every row that succeeded.
///
/// `batch` has already failed, so at least one row in it is bad. The batch is
/// halved and each half re-run: a half that succeeds contributes its output, a
/// half that fails is halved again. Narrowing stops at single rows, which are
/// recorded and dropped. That is O(log n) stage calls to find one bad row and
/// O(k log n) for k bad rows — far cheaper than re-running the stage per row.
///
/// The original error is attached to the first bad row found; deeper rows
/// record the error from the narrowing step that actually isolated them.
///
/// Only correct for row-independent stages (`filter`, `map`, `select`,
/// `limit`). A stateful stage (aggregate, sort, join, dedup) can have already
/// absorbed the bad rows into its accumulator when it fails, so narrowing
/// re-runs it over a *subset* of what it originally saw. Those stages are
/// therefore rejected up front by the caller, which is why a stateful stage
/// with a dead-letter queue configured is a hard error rather than a silent
/// wrong answer.
async fn isolate_and_capture(
    stage: &mut dyn PipelineStage,
    batch: RecordBatch,
    dlq: &mut DeadLetterQueue,
    error: &Error,
) -> Result<Vec<RecordBatch>> {
    if stateful_stage(stage) {
        return Err(Error::Config(format!(
            "dead-letter queue is not supported for the stateful stage {:?}; \
             it carries state across batches, so isolating a bad row would \
             re-run it over a subset of its input. Disable the dead-letter \
             queue or move the failing work into a row-independent stage \
             (filter/map/select).",
            stage.name()
        )));
    }
    if batch.num_rows() == 0 {
        return Ok(Vec::new());
    }

    let mut good: Vec<RecordBatch> = Vec::new();
    // Queue (not stack) of batches still to examine, each known to fail, with
    // the 0-based index of its first row in the original batch. FIFO order
    // plus the index is what keeps recovered rows in their original order —
    // a LIFO stack would emit survivors out of sequence.
    let mut pending: std::collections::VecDeque<(RecordBatch, String, usize)> =
        std::collections::VecDeque::new();
    pending.push_back((batch, error.to_string(), 0));
    let total = pending.front().map_or(0, |(b, _, _)| b.num_rows());
    // `good` is indexed by first-row position so the caller's downstream
    // stages and sink see the batch's rows in the order they arrived.
    let mut slots: Vec<Option<Vec<RecordBatch>>> = vec![None; total];

    while let Some((candidate, why, origin)) = pending.pop_front() {
        if candidate.num_rows() == 1 {
            dlq.write(&candidate, &why)?;
            continue;
        }
        let mid = candidate.num_rows() / 2;
        let (left, right) = split_batch(&candidate, mid);
        for (offset, half) in [left, right].into_iter().enumerate() {
            if half.is_empty() {
                continue;
            }
            let at = origin + offset * mid;
            match stage.process(half.clone()).await {
                Ok(out) => {
                    let slot = slots[at].get_or_insert_with(Vec::new);
                    slot.extend(out);
                }
                Err(e) => pending.push_back((half, e.to_string(), at)),
            }
        }
    }

    good.extend(slots.into_iter().flatten().flatten());
    Ok(good)
}

/// Stages that carry state across batches, and so cannot be safely re-run over
/// a subset of their input. `expect` is here because it asserts over the whole
/// stream, not row by row. `limit` is *not* listed: it tracks a running count,
/// but narrowing only ever re-runs it on a prefix of the same rows, so its
/// count stays monotonic and correct.
fn stateful_stage(stage: &dyn PipelineStage) -> bool {
    matches!(
        stage.name(),
        "GroupByAgg" | "Sort" | "Deduplicate" | "HashJoin" | "expect"
    )
}

/// In-memory sink used by [`Pipeline::collect`].
#[derive(Default)]
struct MemorySink {
    batches: Vec<RecordBatch>,
}

#[async_trait::async_trait]
impl crate::sink::Sink for MemorySink {
    async fn write_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        self.batches.push(batch.clone());
        Ok(())
    }

    async fn finish(&mut self) -> Result<()> {
        Ok(())
    }
}

/// Discards writes; used when a pipeline has stages but no sink so the
/// runner can always hand a sink down.
struct NullSink;

#[async_trait::async_trait]
impl crate::sink::Sink for NullSink {
    async fn write_batch(&mut self, _batch: &RecordBatch) -> Result<()> {
        Ok(())
    }

    async fn finish(&mut self) -> Result<()> {
        Ok(())
    }
}

pub struct EmptySource;

#[async_trait::async_trait]
impl crate::source::Source for EmptySource {
    async fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::column::Column;
    use crate::value::{DataType, Value};

    fn batch_of(values: &[i64]) -> RecordBatch {
        let mut col = Column::new("n", DataType::Int64, values.len());
        for v in values {
            col.push(Value::Int64(*v));
        }
        RecordBatch::new(vec![col])
    }

    fn ints(batch: &RecordBatch) -> Vec<i64> {
        (0..batch.num_rows())
            .map(|i| match batch.cell(i, "n").unwrap() {
                Value::Int64(n) => n,
                _ => unreachable!("column is Int64"),
            })
            .collect()
    }

    /// A stage that rejects any row whose value is divisible by `bad` (0 =
    /// reject nothing), so a test can place bad rows precisely.
    struct RejectDivisible {
        bad: i64,
    }

    #[async_trait::async_trait]
    impl PipelineStage for RejectDivisible {
        fn name(&self) -> &'static str {
            "filter"
        }

        async fn process(&mut self, batch: RecordBatch) -> Result<Vec<RecordBatch>> {
            for i in 0..batch.num_rows() {
                let Value::Int64(n) = batch.cell(i, "n").unwrap() else {
                    unreachable!("column is Int64")
                };
                if self.bad != 0 && n % self.bad == 0 {
                    return Err(Error::Schema(format!(
                        "row {n} is divisible by {}",
                        self.bad
                    )));
                }
            }
            Ok(vec![batch])
        }
    }

    /// A one-batch source over `values`.
    struct OneBatch(Option<RecordBatch>);

    #[async_trait::async_trait]
    impl crate::source::Source for OneBatch {
        async fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
            Ok(self.0.take())
        }
    }

    /// Run `values` through a `RejectDivisible` stage with the dead-letter
    /// queue armed, returning `(rows that reached the output, rows captured)`.
    fn run_dead_letter(values: &[i64], bad: i64) -> (Vec<i64>, Vec<i64>) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("dlq.csv");
        let path_str = path.to_string_lossy().into_owned();

        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let passed = runtime.block_on(async {
            let mut p = Pipeline::new();
            p.source(OneBatch(Some(batch_of(values))));
            p.stage(RejectDivisible { bad });
            p.dead_letter(&path_str).unwrap();
            p.collect()
                .await
                .unwrap()
                .iter()
                .flat_map(ints)
                .collect::<Vec<i64>>()
        });

        let captured = std::fs::read_to_string(&path).unwrap();
        let mut bad_rows: Vec<i64> = captured
            .lines()
            .skip(1) // header
            .map(|line| line.rsplit(',').next().unwrap().parse::<i64>().unwrap())
            .collect();
        bad_rows.sort_unstable();
        (passed, bad_rows)
    }

    #[test]
    fn split_batch_halves_at_mid() {
        let b = batch_of(&[1, 2, 3, 4, 5]);
        let (left, right) = split_batch(&b, 2);
        assert_eq!(ints(&left), vec![1, 2]);
        assert_eq!(ints(&right), vec![3, 4, 5]);
    }

    #[test]
    fn split_batch_preserves_all_rows() {
        let b = batch_of(&[10, 20, 30, 40, 50, 60, 70]);
        for mid in 0..=b.num_rows() {
            let (left, right) = split_batch(&b, mid);
            assert_eq!(
                left.num_rows() + right.num_rows(),
                b.num_rows(),
                "mid={mid}"
            );
        }
    }

    /// The narrowing loop depends on halves being *contiguous, in order*
    /// slices of the original batch. This pins that contract, because a
    /// silently reordered split would make the dead-letter queue attribute
    /// the wrong row.
    #[test]
    fn split_batch_halves_are_ordered_and_contiguous() {
        let b = batch_of(&[1, 2, 3, 4, 5, 6, 7, 8]);
        let (left, right) = split_batch(&b, 4);
        assert_eq!(ints(&left), vec![1, 2, 3, 4]);
        assert_eq!(ints(&right), vec![5, 6, 7, 8]);

        let (left, right) = split_batch(&b, 3);
        assert_eq!(ints(&left), vec![1, 2, 3]);
        assert_eq!(ints(&right), vec![4, 5, 6, 7, 8]);
    }

    #[test]
    fn dead_letter_captures_exactly_the_bad_rows() {
        let values: Vec<i64> = (1..=16).collect();
        let (passed, captured) = run_dead_letter(&values, 3);
        let mut expected_bad: Vec<i64> = values.iter().copied().filter(|v| v % 3 == 0).collect();
        expected_bad.sort_unstable();
        let expected_good: Vec<i64> = values.iter().copied().filter(|v| v % 3 != 0).collect();
        assert_eq!(captured, expected_bad, "captured the wrong rows");
        // Also asserts order: survivors must come back in input sequence, not
        // in whatever order the narrowing happened to visit them.
        assert_eq!(passed, expected_good, "good rows must survive in order");
    }

    #[test]
    fn dead_letter_handles_a_single_bad_row() {
        let values = vec![1, 2, 3, 4, 5, 6, 7, 8];
        let (passed, captured) = run_dead_letter(&values, 4);
        assert_eq!(captured, vec![4, 8]);
        assert_eq!(passed, vec![1, 2, 3, 5, 6, 7]);
    }

    #[test]
    fn dead_letter_handles_no_bad_rows() {
        // No value here is a multiple of 3, so the stage never fails.
        let values: Vec<i64> = vec![1, 2, 4, 5, 7, 8, 10, 11];
        let (passed, captured) = run_dead_letter(&values, 3);
        assert_eq!(captured, Vec::<i64>::new(), "nothing should be captured");
        assert_eq!(passed, values);
    }

    #[test]
    fn dead_letter_handles_all_rows_bad() {
        let values = vec![2, 4, 6, 8];
        let (passed, captured) = run_dead_letter(&values, 2);
        assert_eq!(captured, vec![2, 4, 6, 8]);
        assert!(passed.is_empty(), "no row should reach the output");
    }

    #[test]
    fn dead_letter_captures_one_row_batch() {
        // Narrowing bottoms out immediately at a single-row batch.
        let (passed, captured) = run_dead_letter(&[7], 7);
        assert_eq!(captured, vec![7]);
        assert!(passed.is_empty());
    }

    #[test]
    fn dead_letter_records_stage_and_error_columns() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("dlq.csv");
        let path_str = path.to_string_lossy().into_owned();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            let mut p = Pipeline::new();
            p.source(OneBatch(Some(batch_of(&[3, 5]))));
            p.stage(RejectDivisible { bad: 3 });
            p.dead_letter(&path_str).unwrap();
            p.collect().await.unwrap();
        });
        let written = std::fs::read_to_string(&path).unwrap();
        let lines: Vec<&str> = written.lines().collect();
        assert_eq!(lines[0], "_dead_letter_stage,_error,n");
        assert!(lines[1].starts_with("filter,"), "row: {}", lines[1]);
        assert!(lines[1].ends_with(",3"), "row: {}", lines[1]);
        assert!(lines[1].contains("divisible by 3"), "row: {}", lines[1]);
    }

    #[test]
    fn dead_letter_writes_an_empty_file_when_nothing_fails() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("dlq.csv");
        let path_str = path.to_string_lossy().into_owned();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            let mut p = Pipeline::new();
            p.source(OneBatch(Some(batch_of(&[1, 2, 3]))));
            p.stage(RejectDivisible { bad: 0 }); // rejects nothing
            p.dead_letter(&path_str).unwrap();
            p.collect().await.unwrap();
        });
        // The header is written lazily, so a clean run leaves nothing behind.
        let written = std::fs::read_to_string(&path).unwrap();
        assert!(
            written.is_empty(),
            "a clean run must leave an empty file: {written:?}"
        );
    }

    #[test]
    fn stage_error_still_aborts_without_a_dead_letter_queue() {
        // The default path must be untouched: a failing stage fails the run.
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let result = runtime.block_on(async {
            let mut p = Pipeline::new();
            p.source(OneBatch(Some(batch_of(&[1, 3, 2]))));
            p.stage(RejectDivisible { bad: 3 });
            p.collect().await
        });
        assert!(result.is_err(), "stage error must abort the run");
        assert!(matches!(result.unwrap_err(), Error::Schema(_)));
    }

    #[test]
    fn dead_letter_rejects_stateful_stages() {
        // Narrowing a stateful stage would re-run it over a subset of its
        // input, so it must fail loudly rather than silently mis-aggregate.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("dlq.csv");
        let path_str = path.to_string_lossy().into_owned();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let result = runtime.block_on(async {
            let mut p = Pipeline::new();
            p.source(OneBatch(Some(batch_of(&[1, 2, 3]))));
            p.aggregate(&["n"], &[crate::AggSpec::sum("n")]);
            p.dead_letter(&path_str).unwrap();
            p.collect().await
        });
        // A healthy aggregate never errors, so the run succeeds; the guard only
        // fires when the stage actually fails.
        assert!(result.is_ok());

        // Now force a failure inside the stateful stage.
        let result = runtime.block_on(async {
            let mut p = Pipeline::new();
            p.source(OneBatch(Some(batch_of(&[1, 2, 3]))));
            p.aggregate(&["missing_column"], &[crate::AggSpec::sum("n")]);
            p.dead_letter(&path_str).unwrap();
            p.collect().await
        });
        let err = result.unwrap_err();
        match err {
            Error::Config(msg) => {
                assert!(msg.contains("stateful"), "unhelpful message: {msg}");
            }
            other => panic!("expected a Config error, got {other:?}"),
        }
    }
}
