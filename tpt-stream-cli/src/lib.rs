//! `tptforge`: YAML-driven ETL pipelines on the tpt-streamforge engine.
//!
//! A pipeline file lists a `source`, optional `stages`, and an optional
//! `sink`:
//!
//! ```yaml
//! source:
//!   csv: { path: in.csv }
//! stages:
//!   - filter: "amount > 0"
//!   - aggregate: { group_by: [region], aggs: { amount: sum, id: count_all } }
//!   - sort: { columns: [sum_amount], descending: true }
//! sink:
//!   csv: out.csv
//! ```
//!
//! Run it with `tptforge run pipeline.yaml`. `tptforge schema FILE` prints
//! the inferred column types; `tptforge preview FILE -n 10` prints the first
//! rows. Sources and sinks cover local files (CSV/JSONL/JSON/`.tptcol`,
//! `.gz`-compressed), SQLite, PostgreSQL, S3/GCS/Azure, and plain HTTP URLs.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Mutex;
use std::time::Duration;

use anyhow::{bail, Context, Result};
use clap::{Parser, Subcommand};
use indicatif::ProgressBar;
use serde::Deserialize;
use tpt_stream_core::join::JoinType;
use tpt_stream_core::source::ErrorPolicy;
use tpt_stream_core::value::DataType;
use tpt_stream_core::{Check, Pipeline, RecordBatch};

// ---------------------------------------------------------------------------
// Pipeline file schema
// ---------------------------------------------------------------------------

/// serde_yaml 0.9 needs `!tag` syntax for serde's externally tagged enums;
/// users should be able to write plain `csv: {...}` keys instead, so the
/// three spec enums below deserialize by hand: exactly one key selects the
/// variant, and the value deserializes into that variant's struct.
fn one_key(
    value: serde_yaml::Value,
    what: &str,
) -> std::result::Result<(String, serde_yaml::Value), String> {
    match value {
        serde_yaml::Value::Mapping(map) => {
            if map.len() != 1 {
                Err(format!(
                    "{what} must have exactly one key, found {}",
                    map.len()
                ))
            } else {
                let (k, v) = map.into_iter().next().expect("len checked");
                let key = k
                    .as_str()
                    .ok_or_else(|| format!("{what} key must be a string"))?
                    .to_string();
                Ok((key, v))
            }
        }
        other => Err(format!(
            "{what} must be a mapping with one key, got {other:?}"
        )),
    }
}

fn variant<T: serde::de::DeserializeOwned>(
    key: &str,
    value: serde_yaml::Value,
    what: &str,
) -> std::result::Result<T, String> {
    serde_yaml::from_value(value).map_err(|e| format!("invalid {what} {key:?}: {e}"))
}

macro_rules! keyed_enum {
    ($name:ident, $what:literal, { $($variant:ident => $key:literal),+ $(,)? }) => {
        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                use serde::de::Error as _;
                let (key, value) = one_key(serde_yaml::Value::deserialize(deserializer)?, $what)
                    .map_err(D::Error::custom)?;
                match key.as_str() {
                    $(
                        $key => variant(&key, value, $what)
                            .map($name::$variant)
                            .map_err(D::Error::custom),
                    )+
                    other => Err(D::Error::custom(format!(
                        "unknown {} {other:?} (expected one of: {})",
                        $what,
                        [$($key),+].join(", ")
                    ))),
                }
            }
        }
    };
}

/// The parsed `pipeline.yaml`.
#[derive(Debug, Deserialize)]
pub struct PipelineSpec {
    pub source: SourceSpec,
    #[serde(default)]
    pub stages: Vec<StageSpec>,
    #[serde(default)]
    pub error_policy: Option<String>,
    pub sink: Option<SinkSpec>,
}

#[derive(Debug)]
pub enum SourceSpec {
    Csv(FileSourceSpec),
    Jsonl(FileSourceSpec),
    Json(FileSourceSpec),
    Columnar(PathOnlySpec),
    Http(PathOnlySpec),
    Sqlite(SqliteQuerySpec),
    Postgres(ConnectionQuerySpec),
    S3(ObjectSpec),
    Gcs(BucketObjectSpec),
    Azure(AzureObjectSpec),
}

/// Accept either `csv: out.csv` (scalar path shorthand) or
/// `csv: { path: out.csv }`.
#[derive(Debug, Deserialize)]
#[serde(from = "PathOrSpec")]
pub struct PathOnlySpec {
    pub path: String,
    #[serde(default)]
    pub chunk_rows: Option<usize>,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum PathOrSpec {
    Path(String),
    // A separate struct (not PathOnlySpec) avoids infinite `from` recursion.
    Spec(SpecInner),
}

#[derive(Debug, Deserialize)]
struct SpecInner {
    path: String,
    #[serde(default)]
    chunk_rows: Option<usize>,
}

impl From<PathOrSpec> for PathOnlySpec {
    fn from(value: PathOrSpec) -> Self {
        match value {
            PathOrSpec::Path(path) => PathOnlySpec {
                path,
                chunk_rows: None,
            },
            PathOrSpec::Spec(spec) => PathOnlySpec {
                path: spec.path,
                chunk_rows: spec.chunk_rows,
            },
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct FileSourceSpec {
    pub path: String,
    #[serde(default)]
    pub chunk_rows: Option<usize>,
}

#[derive(Debug, Deserialize)]
pub struct SqliteQuerySpec {
    pub path: String,
    pub query: String,
    #[serde(default)]
    pub chunk_rows: Option<usize>,
}

#[derive(Debug, Deserialize)]
pub struct ConnectionQuerySpec {
    pub connection: String,
    pub query: String,
}

#[derive(Debug, Deserialize)]
pub struct ObjectSpec {
    pub bucket_url: String,
    pub key: String,
}

#[derive(Debug, Deserialize)]
pub struct BucketObjectSpec {
    pub bucket: String,
    pub key: String,
}

#[derive(Debug, Deserialize)]
pub struct AzureObjectSpec {
    pub account_url: String,
    pub container: String,
    pub key: String,
}

#[derive(Debug)]
pub enum StageSpec {
    Filter(String),
    Map(BTreeMap<String, String>),
    Select(Vec<String>),
    Aggregate(AggregateSpec),
    Sort(SortSpec),
    Dedup(Vec<String>),
    Join(JoinSpec),
    Expect(ExpectSpec),
}

#[derive(Debug, Deserialize)]
pub struct AggregateSpec {
    pub group_by: Vec<String>,
    /// `{column: fn}` where fn ∈ sum|avg|count|count_all|min|max. Use the
    /// key `"*"` with `count_all` for a row count.
    #[serde(default)]
    pub aggs: BTreeMap<String, String>,
}

#[derive(Debug, Deserialize)]
pub struct SortSpec {
    pub columns: Vec<String>,
    #[serde(default)]
    pub descending: bool,
}

#[derive(Debug, Deserialize)]
pub struct JoinSpec {
    pub right: String,
    pub left_keys: Vec<String>,
    pub right_keys: Vec<String>,
    #[serde(rename = "type", default)]
    pub join_type: String,
}

#[derive(Debug, Deserialize)]
pub struct ExpectSpec {
    #[serde(default)]
    pub rows_at_least: Option<u64>,
    #[serde(default)]
    pub rows_at_most: Option<u64>,
    #[serde(default)]
    pub no_nulls: Vec<String>,
    #[serde(default)]
    pub unique: Vec<String>,
}

#[derive(Debug)]
pub enum SinkSpec {
    Csv(PathOnlySpec),
    Jsonl(PathOnlySpec),
    Json(JsonSinkSpec),
    Columnar(ColumnarSinkSpec),
    Sqlite(SqliteSinkSpec),
    Postgres(ConnectionTableSpec),
    S3(ObjectSpec),
    Gcs(BucketObjectSpec),
    Azure(AzureObjectSpec),
}

#[derive(Debug, Deserialize)]
pub struct JsonSinkSpec {
    pub path: String,
    #[serde(default)]
    pub pretty: bool,
}

#[derive(Debug, Deserialize)]
pub struct ColumnarSinkSpec {
    pub path: String,
    #[serde(default)]
    pub use_zstd: bool,
}

#[derive(Debug, Deserialize)]
pub struct SqliteSinkSpec {
    pub path: String,
    pub table: String,
}

#[derive(Debug, Deserialize)]
pub struct ConnectionTableSpec {
    pub connection: String,
    pub table: String,
}

keyed_enum!(SourceSpec, "source", {
    Csv => "csv",
    Jsonl => "jsonl",
    Json => "json",
    Columnar => "columnar",
    Http => "http",
    Sqlite => "sqlite",
    Postgres => "postgres",
    S3 => "s3",
    Gcs => "gcs",
    Azure => "azure",
});

keyed_enum!(StageSpec, "stage", {
    Filter => "filter",
    Map => "map",
    Select => "select",
    Aggregate => "aggregate",
    Sort => "sort",
    Dedup => "dedup",
    Join => "join",
    Expect => "expect",
});

keyed_enum!(SinkSpec, "sink", {
    Csv => "csv",
    Jsonl => "jsonl",
    Json => "json",
    Columnar => "columnar",
    Sqlite => "sqlite",
    Postgres => "postgres",
    S3 => "s3",
    Gcs => "gcs",
    Azure => "azure",
});

// ---------------------------------------------------------------------------
// Spec -> Pipeline
// ---------------------------------------------------------------------------

/// Build a [`Pipeline`] from a spec. S3/GCS credentials come from the
/// `AWS_*` environment variables, Azure from `AZURE_*`.
pub fn build_pipeline(spec: &PipelineSpec) -> Result<Pipeline> {
    let mut pipeline = Pipeline::new();
    if let Some(policy) = &spec.error_policy {
        pipeline.on_error(parse_error_policy(policy)?);
    }
    apply_source(&mut pipeline, &spec.source)?;
    for stage in &spec.stages {
        apply_stage(&mut pipeline, stage)?;
    }
    if let Some(sink) = &spec.sink {
        apply_sink(&mut pipeline, sink)?;
    }
    Ok(pipeline)
}

fn parse_error_policy(text: &str) -> Result<ErrorPolicy> {
    if text == "strict" {
        Ok(ErrorPolicy::Strict)
    } else if text == "skip" {
        Ok(ErrorPolicy::Skip)
    } else if let Some(path) = text.strip_prefix("quarantine:") {
        Ok(ErrorPolicy::Quarantine(path.to_string()))
    } else {
        bail!("invalid error_policy {text:?} (expected 'strict', 'skip', or 'quarantine:<path>')")
    }
}

fn parse_join_type(text: &str) -> Result<JoinType> {
    match text {
        "inner" => Ok(JoinType::Inner),
        "left" => Ok(JoinType::Left),
        "right" => Ok(JoinType::Right),
        other => bail!("invalid join type {other:?} (expected inner|left|right)"),
    }
}

fn apply_source(pipeline: &mut Pipeline, source: &SourceSpec) -> Result<()> {
    match source {
        SourceSpec::Csv(s) => {
            if let Some(rows) = s.chunk_rows {
                pipeline.with_chunk_size(rows);
            }
            pipeline.read_csv(&s.path);
        }
        SourceSpec::Jsonl(s) => {
            if let Some(rows) = s.chunk_rows {
                pipeline.with_chunk_size(rows);
            }
            pipeline.read_jsonl(&s.path);
        }
        SourceSpec::Json(s) => {
            if let Some(rows) = s.chunk_rows {
                pipeline.with_chunk_size(rows);
            }
            pipeline.read_json(&s.path);
        }
        SourceSpec::Columnar(s) => {
            pipeline.read_columnar(&s.path);
        }
        SourceSpec::Http(s) => {
            pipeline.read_http(&s.path);
        }
        SourceSpec::Sqlite(s) => {
            if let Some(rows) = s.chunk_rows {
                pipeline.with_chunk_size(rows);
            }
            pipeline.read_sqlite(&s.path, &s.query);
        }
        SourceSpec::Postgres(s) => {
            pipeline.read_postgres(&s.connection, &s.query);
        }
        SourceSpec::S3(s) => {
            let creds = tpt_stream_core::CloudCredentials::from_env()?;
            pipeline.read_s3(&s.bucket_url, &s.key, &creds)?;
        }
        SourceSpec::Gcs(s) => {
            let creds = tpt_stream_core::CloudCredentials::from_env()?;
            pipeline.read_gcs(&s.bucket, &s.key, &creds)?;
        }
        SourceSpec::Azure(s) => {
            let creds = tpt_stream_core::AzureCredentials::from_env()?;
            let store = tpt_stream_core::AzureBlobStore::new(&s.account_url, &s.container, &creds)?;
            pipeline.read_azure_blob(store, &s.key);
        }
    }
    Ok(())
}

fn apply_stage(pipeline: &mut Pipeline, stage: &StageSpec) -> Result<()> {
    match stage {
        StageSpec::Filter(expr) => {
            pipeline.filter_expr(expr);
        }
        StageSpec::Map(mapping) => {
            let pairs: Vec<(&str, &str)> = mapping
                .iter()
                .map(|(k, v)| (k.as_str(), v.as_str()))
                .collect();
            pipeline.map_expr(&pairs);
        }
        StageSpec::Select(columns) => {
            let refs: Vec<&str> = columns.iter().map(|s| s.as_str()).collect();
            pipeline.select(&refs);
        }
        StageSpec::Aggregate(agg) => {
            let group_by: Vec<&str> = agg.group_by.iter().map(|s| s.as_str()).collect();
            let mut specs = Vec::new();
            for (column, function) in &agg.aggs {
                let column = if column == "*" { "" } else { column.as_str() };
                specs.push(parse_agg(column, function)?);
            }
            pipeline.aggregate(&group_by, &specs);
        }
        StageSpec::Sort(sort) => {
            let columns: Vec<&str> = sort.columns.iter().map(|s| s.as_str()).collect();
            if sort.descending {
                pipeline.sort_by_desc(&columns);
            } else {
                pipeline.sort_by(&columns);
            }
        }
        StageSpec::Dedup(columns) => {
            let refs: Vec<&str> = columns.iter().map(|s| s.as_str()).collect();
            pipeline.dedup(&refs);
        }
        StageSpec::Join(join) => {
            let left: Vec<&str> = join.left_keys.iter().map(|s| s.as_str()).collect();
            let right: Vec<&str> = join.right_keys.iter().map(|s| s.as_str()).collect();
            pipeline.join_csv(
                &join.right,
                &left,
                &right,
                parse_join_type(&join.join_type)?,
            )?;
        }
        StageSpec::Expect(expect) => {
            let mut checks = Vec::new();
            if let Some(n) = expect.rows_at_least {
                checks.push(Check::RowsAtLeast(n));
            }
            if let Some(n) = expect.rows_at_most {
                checks.push(Check::RowsAtMost(n));
            }
            for column in &expect.no_nulls {
                checks.push(Check::NoNulls(column.clone()));
            }
            for column in &expect.unique {
                checks.push(Check::Unique(column.clone()));
            }
            if checks.is_empty() {
                bail!("expect stage has no checks");
            }
            pipeline.expect_checks(checks);
        }
    }
    Ok(())
}

fn parse_agg(column: &str, function: &str) -> Result<tpt_stream_core::AggSpec> {
    use tpt_stream_core::AggSpec;
    // In AggSpec, count_all's argument is the *output* column name.
    if function == "count_all" {
        let output = if column.is_empty() {
            "count_all".to_string()
        } else {
            format!("count_{column}")
        };
        return Ok(AggSpec::count_all(output));
    }
    Ok(match function {
        "sum" => AggSpec::sum(column),
        "avg" => AggSpec::avg(column),
        "count" => AggSpec::count(column),
        "min" => AggSpec::min(column),
        "max" => AggSpec::max(column),
        other => {
            bail!("unknown aggregate function {other:?} (expected sum|avg|count|count_all|min|max)")
        }
    })
}

fn apply_sink(pipeline: &mut Pipeline, sink: &SinkSpec) -> Result<()> {
    match sink {
        SinkSpec::Csv(s) => {
            pipeline.write_csv(&s.path);
        }
        SinkSpec::Jsonl(s) => {
            pipeline.write_jsonl(&s.path);
        }
        SinkSpec::Json(s) => {
            pipeline.write_json(&s.path, s.pretty);
        }
        SinkSpec::Columnar(s) => {
            pipeline.write_columnar(&s.path, s.use_zstd);
        }
        SinkSpec::Sqlite(s) => {
            pipeline.write_sqlite(&s.path, &s.table);
        }
        SinkSpec::Postgres(s) => {
            pipeline.write_postgres(&s.connection, &s.table);
        }
        SinkSpec::S3(s) => {
            let creds = tpt_stream_core::CloudCredentials::from_env()?;
            pipeline.write_s3(&s.bucket_url, &s.key, &creds)?;
        }
        SinkSpec::Gcs(s) => {
            let creds = tpt_stream_core::CloudCredentials::from_env()?;
            pipeline.write_gcs(&s.bucket, &s.key, &creds)?;
        }
        SinkSpec::Azure(s) => {
            let creds = tpt_stream_core::AzureCredentials::from_env()?;
            let store = tpt_stream_core::AzureBlobStore::new(&s.account_url, &s.container, &creds)?;
            pipeline.write_azure_blob(store, &s.key);
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Commands
// ---------------------------------------------------------------------------

#[derive(Debug, Parser)]
#[command(
    name = "tptforge",
    version,
    about = "Streaming ETL pipelines from a YAML file, powered by tpt-streamforge"
)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    /// Run a pipeline defined in a YAML file.
    Run {
        /// Path to the pipeline YAML file.
        pipeline: PathBuf,
        /// Hide the progress bar.
        #[arg(long)]
        quiet: bool,
    },
    /// Print the inferred schema (column name + type) of a data file or URL.
    Schema {
        /// File path (csv/jsonl/json/tptcol, `.gz` supported) or http(s) URL.
        input: String,
        /// How many rows to scan before printing the schema.
        #[arg(long, default_value = "1000")]
        rows: usize,
    },
    /// Print the first rows of a data file or URL as CSV.
    Preview {
        /// File path (csv/jsonl/json/tptcol, `.gz` supported) or http(s) URL.
        input: String,
        /// Number of rows to show.
        #[arg(short = 'n', long, default_value = "10")]
        num: usize,
    },
}

/// Attach a spinner progress bar to the pipeline's telemetry.
fn attach_progress(pipeline: &mut Pipeline) {
    let bar = ProgressBar::new_spinner();
    bar.enable_steady_tick(Duration::from_millis(120));
    bar.set_style(
        indicatif::ProgressStyle::with_template("{spinner} {msg}")
            .unwrap_or_else(|_| indicatif::ProgressStyle::default_spinner()),
    );
    let bar_for_done = bar.clone();
    let bar = Mutex::new(bar);
    pipeline.on_progress(std::sync::Arc::new(move |event| {
        use tpt_stream_core::TelemetryEvent::*;
        let locked = bar.lock().unwrap();
        match event {
            SourceBatch { total_rows, .. } => {
                locked.set_message(format!("{total_rows} rows read"));
            }
            SinkBatch { total_rows, .. } => {
                locked.set_message(format!("{total_rows} rows written"));
            }
            Done { elapsed, .. } => {
                bar_for_done.finish_with_message(format!("done in {elapsed:.1?}"));
            }
            _ => {}
        }
    }));
}

/// Execute `tptforge run`.
pub async fn run_command(pipeline_path: &std::path::Path, quiet: bool) -> Result<String> {
    let text = std::fs::read_to_string(pipeline_path)
        .with_context(|| format!("reading pipeline file {}", pipeline_path.display()))?;
    let spec: PipelineSpec = serde_yaml::from_str(&text).context("parsing pipeline YAML")?;
    let mut pipeline = build_pipeline(&spec)?;
    if !quiet {
        attach_progress(&mut pipeline);
    }
    let stats = pipeline.execute().await?;
    Ok(format!(
        "{} rows in {} batch(es), {} bytes out, in {:.1?}",
        stats.rows, stats.batches, stats.bytes_out, stats.elapsed
    ))
}

/// Inspect a file/URL: read the first `rows` output rows through the
/// format's native reader.
async fn inspect(input: &str, rows: usize) -> Result<Vec<RecordBatch>> {
    let mut pipeline = Pipeline::new();
    let lower = input.to_ascii_lowercase();
    if lower.starts_with("http://") || lower.starts_with("https://") {
        pipeline.read_http(input);
    } else if lower.ends_with(".jsonl")
        || lower.ends_with(".ndjson")
        || lower.ends_with(".jsonl.gz")
    {
        pipeline.read_jsonl(input);
    } else if lower.ends_with(".json") {
        pipeline.read_json(input);
    } else if lower.ends_with(".tptcol") {
        pipeline.read_columnar(input);
    } else {
        pipeline.read_csv(input);
    }
    let batches = pipeline
        .preview(rows)
        .await
        .with_context(|| format!("reading {input}"))?;
    Ok(batches)
}

/// Execute `tptforge schema`.
pub async fn schema_command(input: &str, rows: usize) -> Result<String> {
    let batches = inspect(input, rows).await?;
    let Some(first) = batches.first() else {
        bail!("{input} has no rows; schema unknown");
    };
    let mut out = String::new();
    for (name, data_type) in first.schema() {
        out.push_str(&format!("{name}\t{}\n", type_name(data_type)));
    }
    Ok(out)
}

fn type_name(data_type: DataType) -> &'static str {
    match data_type {
        DataType::Int32 => "int32",
        DataType::Int64 => "int64",
        DataType::Float32 => "float32",
        DataType::Float64 => "float64",
        DataType::Bool => "bool",
        DataType::Utf8 => "string",
    }
}

/// Execute `tptforge preview`.
pub async fn preview_command(input: &str, num: usize) -> Result<String> {
    let batches = inspect(input, num).await?;
    Ok(tpt_stream_core::source::batches_to_csv(&batches))
}

/// Parse a YAML pipeline (exposed for tests and embedders).
pub fn parse_pipeline_yaml(text: &str) -> Result<PipelineSpec> {
    serde_yaml::from_str(text).context("parsing pipeline YAML")
}
