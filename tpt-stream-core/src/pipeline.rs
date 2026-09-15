use crate::table::RecordBatch;

// Re-export for back-compat with `crate::pipeline::{Error, Result}` imports.
pub use crate::error::{Error, PipelineStats, Result};

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
}

impl Default for Pipeline {
    fn default() -> Self {
        Self::new()
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
        }
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
        self.source(crate::source::CsvSource::open_with_chunk_size(
            path,
            self.chunk_rows,
        ));
        self
    }

    pub fn read_jsonl(&mut self, path: impl Into<String>) -> &mut Self {
        self.source(crate::source::JsonlSource::open_with_chunk_size(
            path,
            self.chunk_rows,
        ));
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
        self.source(crate::s3::S3Source::open(store, key));
        Ok(self)
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
        self.sink(crate::s3::S3Sink::new(store, key));
        Ok(self)
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
        self.source(crate::gcs::GcsSource::open(store, key));
        Ok(self)
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
        self.sink(crate::gcs::GcsSink::new(store, key));
        Ok(self)
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

    pub async fn execute(&mut self) -> Result<PipelineStats> {
        if let Some(err) = self.pending_error.take() {
            return Err(Error::Schema(err));
        }
        let mut source = self
            .source
            .take()
            .ok_or_else(|| Error::Config("pipeline has no source".into()))?;
        let mut sink = self.sink.take();
        let telemetry = self.telemetry.take();
        let mut stage_metrics = vec![crate::telemetry::StageMetrics::default(); self.stages.len()];
        let emit = |event: &crate::telemetry::TelemetryEvent| {
            if let Some(hook) = &telemetry {
                hook(event);
            }
        };
        let mut stats = PipelineStats::default();
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
                    next.extend(stage.process(b).await?);
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
                for b in &current {
                    sink.write_batch(b).await?;
                }
                emit(&crate::telemetry::TelemetryEvent::SinkBatch {
                    rows: current.iter().map(|b| b.num_rows() as u64).sum(),
                    total_rows: stats.rows,
                });
            }
        }

        // Finalization: stages may buffer across chunks and emit tails once.
        // Drain each stage's tail through the downstream stages, in order, so
        // downstream tail-emitting stages receive upstream tails before their
        // own finish is invoked.
        for i in 0..self.stages.len() {
            let tail = self.stages[i].finish().await?;
            let mut current = tail;
            for stage in self.stages.iter_mut().skip(i + 1) {
                let mut next = Vec::new();
                for b in current {
                    next.extend(stage.process(b).await?);
                }
                current = next;
            }
            if let Some(sink) = sink.as_mut() {
                for b in &current {
                    sink.write_batch(b).await?;
                }
            }
        }

        if let Some(sink) = sink.as_mut() {
            sink.finish().await?;
            stats.bytes_out = sink.bytes_out();
        }

        stats.elapsed = started.elapsed();
        emit(&crate::telemetry::TelemetryEvent::Done {
            rows: stats.rows,
            batches: stats.batches,
            elapsed: stats.elapsed,
        });

        self.source = Some(source);
        self.sink = sink;
        self.telemetry = telemetry;
        self.stage_metrics = stage_metrics;

        Ok(stats)
    }
}

pub struct EmptySource;

#[async_trait::async_trait]
impl crate::source::Source for EmptySource {
    async fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        Ok(None)
    }
}
