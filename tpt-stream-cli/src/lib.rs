//! `tptforge`: TOML-driven ETL pipelines on the tpt-streamforge engine.
//!
//! A pipeline file lists a `source`, optional `stages`, and an optional
//! `sink`:
//!
//! ```toml
//! [source.csv]
//! path = "in.csv"
//!
//! [[stages]]
//! filter = "amount > 0"
//!
//! [[stages]]
//! [stages.aggregate]
//! group_by = ["region"]
//! [stages.aggregate.aggs]
//! amount = "sum"
//! id = "count_all"
//!
//! [[stages]]
//! [stages.sort]
//! columns = ["sum_amount"]
//! descending = true
//!
//! [sink]
//! csv = "out.csv"
//! ```
//!
//! Run it with `tptforge run pipeline.toml`. `tptforge schema FILE` prints
//! the inferred column types; `tptforge preview FILE -n 10` prints the first
//! rows. Sources and sinks cover local files (CSV/JSONL/JSON/`.tptcol`,
//! `.gz`-compressed), SQLite, PostgreSQL, S3/GCS/Azure, and plain HTTP URLs.

pub mod config;
pub mod manifest;
pub mod metrics;
pub mod schema_json;
pub mod sql;
pub mod tools;

pub use config::{parse_pipeline_toml, parse_pipeline_toml_with_env};

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

/// TOML has no `!tag` syntax for serde's externally tagged enums; users
/// should be able to write plain `csv = "in.csv"` / `[source.csv]` keys
/// instead, so the three spec enums below deserialize by hand: exactly one
/// key selects the variant, and the value deserializes into that variant's
/// struct (or, for the path shorthands, straight into a string).
fn one_key(value: toml::Value, what: &str) -> std::result::Result<(String, toml::Value), String> {
    match value {
        toml::Value::Table(map) => {
            if map.len() != 1 {
                Err(format!(
                    "{what} must have exactly one key, found {}",
                    map.len()
                ))
            } else {
                let (k, v) = map.into_iter().next().expect("len checked");
                Ok((k, v))
            }
        }
        other => Err(format!(
            "{what} must be a table with one key, got {other:?}"
        )),
    }
}

fn variant<T: serde::de::DeserializeOwned>(
    key: &str,
    value: toml::Value,
    what: &str,
) -> std::result::Result<T, String> {
    value
        .try_into()
        .map_err(|e| format!("invalid {what} {key:?}: {e}"))
}

macro_rules! keyed_enum {
    ($name:ident, $what:literal, { $($variant:ident => $key:literal),+ $(,)? }) => {
        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                use serde::de::Error as _;
                let (key, value) = one_key(toml::Value::deserialize(deserializer)?, $what)
                    .map_err(D::Error::custom)?;
                match key.as_str() {
                    $(
                        $key => variant(&key, value, $what)
                            .map($name::$variant)
                            .map_err(D::Error::custom),
                    )+
                    other => {
                        let keys = [$($key),+];
                        let hint = crate::config::did_you_mean(other, &keys)
                            .map(|s| format!(" -- did you mean `{s}`?"))
                            .unwrap_or_default();
                        Err(D::Error::custom(format!(
                            "unknown {} {other:?} (expected one of: {}){hint}",
                            $what,
                            keys.join(", ")
                        )))
                    }
                }
            }
        }
    };
}

/// The parsed `pipeline.toml`.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PipelineSpec {
    pub source: SourceSpec,
    #[serde(default)]
    pub stages: Vec<StageSpec>,
    #[serde(default)]
    pub error_policy: Option<String>,
    /// Path of a CSV file that receives rows a *stage* rejects (instead of
    /// aborting the run). Not supported with stateful stages
    /// (aggregate/sort/dedup/join/expect).
    #[serde(default)]
    pub dead_letter: Option<String>,
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

/// Accept either `csv = "out.csv"` (scalar path shorthand) or
/// `[csv]` with `path = "out.csv"`.
#[derive(Debug)]
pub struct PathOnlySpec {
    pub path: String,
    pub chunk_rows: Option<usize>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct SpecInner {
    path: String,
    #[serde(default)]
    chunk_rows: Option<usize>,
}

impl<'de> Deserialize<'de> for PathOnlySpec {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de::Error as _;
        // Hand-rolled instead of `#[serde(untagged)]` so a bad table reports
        // the real problem ("unknown field `pth`") rather than serde's
        // opaque "data did not match any variant".
        match toml::Value::deserialize(deserializer)? {
            toml::Value::String(path) => Ok(PathOnlySpec {
                path,
                chunk_rows: None,
            }),
            table @ toml::Value::Table(_) => {
                let inner: SpecInner = table.try_into().map_err(D::Error::custom)?;
                Ok(PathOnlySpec {
                    path: inner.path,
                    chunk_rows: inner.chunk_rows,
                })
            }
            other => Err(D::Error::custom(format!(
                "expected a path string or a table with `path`, got {}",
                other.type_str()
            ))),
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FileSourceSpec {
    pub path: String,
    #[serde(default)]
    pub chunk_rows: Option<usize>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SqliteQuerySpec {
    pub path: String,
    pub query: String,
    #[serde(default)]
    pub chunk_rows: Option<usize>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConnectionQuerySpec {
    pub connection: String,
    pub query: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ObjectSpec {
    pub bucket_url: String,
    pub key: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BucketObjectSpec {
    pub bucket: String,
    pub key: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
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
    /// Keep only the first N rows.
    Limit(u64),
    /// Deterministic keyed sample.
    Sample(SampleSpec),
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SampleSpec {
    /// Fraction of distinct keys to keep, in `[0, 1]`.
    pub fraction: f64,
    /// Key columns; rows sharing a key are kept or dropped together.
    pub key: Vec<String>,
    #[serde(default)]
    pub seed: u64,
}

/// Inclusive numeric bounds; either side may be omitted.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RangeSpec {
    #[serde(default)]
    pub min: Option<f64>,
    #[serde(default)]
    pub max: Option<f64>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AggregateSpec {
    pub group_by: Vec<String>,
    /// `{column: fn}` where fn ∈ sum|avg|count|count_all|min|max. Use the
    /// key `"*"` with `count_all` for a row count.
    #[serde(default)]
    pub aggs: BTreeMap<String, String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SortSpec {
    pub columns: Vec<String>,
    #[serde(default)]
    pub descending: bool,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JoinSpec {
    pub right: String,
    pub left_keys: Vec<String>,
    pub right_keys: Vec<String>,
    #[serde(rename = "type", default)]
    pub join_type: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExpectSpec {
    #[serde(default)]
    pub rows_at_least: Option<u64>,
    #[serde(default)]
    pub rows_at_most: Option<u64>,
    #[serde(default)]
    pub no_nulls: Vec<String>,
    #[serde(default)]
    pub unique: Vec<String>,
    /// `{column = {min = .., max = ..}}`, inclusive bounds.
    #[serde(default)]
    pub ranges: BTreeMap<String, RangeSpec>,
    /// `{column = [allowed values]}`.
    #[serde(default)]
    pub one_of: BTreeMap<String, Vec<toml::Value>>,
    /// `{column = "int32"}`; int32|int64|float32|float64|utf8|bool|date|timestamp.
    #[serde(default)]
    pub types: BTreeMap<String, String>,
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
#[serde(deny_unknown_fields)]
pub struct JsonSinkSpec {
    pub path: String,
    #[serde(default)]
    pub pretty: bool,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ColumnarSinkSpec {
    pub path: String,
    #[serde(default)]
    pub use_zstd: bool,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SqliteSinkSpec {
    pub path: String,
    pub table: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
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
    Limit => "limit",
    Sample => "sample",
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
    if let Some(path) = &spec.dead_letter {
        pipeline
            .dead_letter(path)
            .with_context(|| format!("opening dead-letter file {path:?}"))?;
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
        StageSpec::Limit(n) => {
            pipeline.limit(usize::try_from(*n).context("limit does not fit in usize")?);
        }
        StageSpec::Sample(sample) => {
            let keys: Vec<&str> = sample.key.iter().map(|s| s.as_str()).collect();
            pipeline.sample(sample.fraction, &keys, sample.seed);
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
            for (column, r) in &expect.ranges {
                checks.push(Check::range(column.clone(), r.min, r.max));
            }
            for (column, values) in &expect.one_of {
                let allowed = values
                    .iter()
                    .map(|v| toml_to_value(v, column))
                    .collect::<Result<Vec<_>>>()?;
                checks.push(Check::one_of(column.clone(), allowed));
            }
            for (column, name) in &expect.types {
                checks.push(Check::of_type(column.clone(), parse_data_type(name)?));
            }
            if checks.is_empty() {
                bail!("expect stage has no checks");
            }
            pipeline.expect_checks(checks);
        }
    }
    Ok(())
}

/// Convert a TOML scalar into an engine value for `one_of` checks.
fn toml_to_value(v: &toml::Value, column: &str) -> Result<tpt_stream_core::Value> {
    use tpt_stream_core::Value;
    Ok(match v {
        toml::Value::String(s) => Value::Utf8(s.clone()),
        toml::Value::Integer(i) => Value::Int64(*i),
        toml::Value::Float(f) => Value::Float64(*f),
        toml::Value::Boolean(b) => Value::Bool(*b),
        other => {
            bail!("expect one_of[{column}]: unsupported value {other} (use string, number or bool)")
        }
    })
}

fn parse_data_type(name: &str) -> Result<DataType> {
    Ok(match name.to_ascii_lowercase().as_str() {
        "int32" => DataType::Int32,
        "int64" | "int" => DataType::Int64,
        "float32" => DataType::Float32,
        "float64" | "float" => DataType::Float64,
        "utf8" | "str" | "string" => DataType::Utf8,
        "bool" | "boolean" => DataType::Bool,
        "date" => DataType::Date,
        "timestamp" => DataType::Timestamp,
        other => bail!(
            "unknown type {other:?} (expected int32|int64|float32|float64|utf8|bool|date|timestamp)"
        ),
    })
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
    about = "Streaming ETL pipelines from a TOML file, powered by tpt-streamforge"
)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    /// Run a pipeline defined in a TOML file.
    Run {
        /// Path to the pipeline TOML file.
        pipeline: PathBuf,
        /// Hide the progress bar.
        #[arg(long)]
        quiet: bool,
        /// Serve Prometheus metrics on this address while the run is in
        /// progress (e.g. `127.0.0.1:9464`; scrape `/metrics`).
        ///
        /// The endpoint is bound before the pipeline starts, so a port
        /// conflict fails the run immediately instead of silently.
        #[arg(long, value_name = "ADDR")]
        metrics: Option<String>,
        /// Value of the `pipeline` label on the exported metrics.
        #[arg(long, default_value = "tptforge")]
        metrics_name: String,
        /// Allow `--metrics` to bind a non-loopback address (e.g. `0.0.0.0`).
        ///
        /// Off by default: the endpoint is unauthenticated, so exposing it
        /// would publish the run's row counts to anyone who can reach it.
        #[arg(long)]
        metrics_allow_remote: bool,
        /// Write a provenance manifest (JSON: counts, per-stage stats, and
        /// SHA-256 of the spec and local inputs/outputs) to this file.
        #[arg(long, value_name = "FILE")]
        manifest: Option<PathBuf>,
        /// Parse, validate, and print the plan (checking that input files
        /// exist), but do not read data or write anything.
        #[arg(long)]
        dry_run: bool,
        /// Rerun whenever the pipeline file or a local input changes
        /// (polls modification times; Ctrl-C to stop).
        #[arg(long)]
        watch: bool,
    },
    /// Check a pipeline file without running it (syntax, options,
    /// expressions). Exits non-zero on errors.
    Validate {
        /// Path to the pipeline TOML file.
        pipeline: PathBuf,
        /// Do not fail on unset `${VAR}` references (substitute `<VAR>`).
        #[arg(long)]
        no_env: bool,
    },
    /// Print the numbered plan a pipeline file describes.
    Explain {
        /// Path to the pipeline TOML file.
        pipeline: PathBuf,
        /// Do not fail on unset `${VAR}` references (substitute `<VAR>`).
        #[arg(long)]
        no_env: bool,
    },
    /// Print the JSON Schema for pipeline files (editor autocomplete).
    SchemaJson,
    /// Print a shell completion script.
    Completions {
        /// Target shell.
        shell: clap_complete::Shell,
    },
    /// Print the man page (roff), or write one page per command to a directory.
    Man {
        /// Write `tptforge.1` and `tptforge-<command>.1` here instead of
        /// printing the main page.
        #[arg(long, value_name = "DIR")]
        out_dir: Option<PathBuf>,
    },
    /// Generate a commented starter pipeline for a data file.
    Init {
        /// Data file or URL to infer the schema from.
        input: String,
        /// Write the pipeline here instead of stdout.
        #[arg(long, value_name = "FILE")]
        out: Option<PathBuf>,
        /// Overwrite `--out` if it exists.
        #[arg(long)]
        force: bool,
        /// How many rows to sample.
        #[arg(long, default_value = "1000")]
        rows: usize,
    },
    /// Check the environment (and optionally a pipeline) for problems.
    Doctor {
        /// Also check this pipeline file (parse, inputs, credentials).
        pipeline: Option<PathBuf>,
    },
    /// Convert a data file between formats (by extension: csv, jsonl,
    /// ndjson, json, tptcol).
    Convert {
        /// Input file or URL.
        input: String,
        /// Output file; the extension picks the format.
        output: PathBuf,
        /// Error policy for malformed input rows: strict | skip |
        /// quarantine:<path>.
        #[arg(long, default_value = "strict")]
        on_error: String,
        /// zstd-compress `.tptcol` output.
        #[arg(long)]
        zstd: bool,
    },
    /// Compare two data files by key: rows only in A (`-`), only in B (`+`),
    /// and changed rows (both). Output is CSV with a leading `_diff` column.
    Diff {
        /// Left file or URL.
        a: String,
        /// Right file or URL.
        b: String,
        /// Key column(s); repeat or comma-separate. Must be unique per file.
        #[arg(long, required = true, value_delimiter = ',')]
        key: Vec<String>,
        /// Write the diff here instead of stdout.
        #[arg(long, value_name = "FILE")]
        out: Option<PathBuf>,
        /// Exit with status 1 when the files differ (like `git diff`).
        #[arg(long)]
        exit_code: bool,
    },
    /// Run a single-table SQL SELECT against a file or URL.
    ///
    /// Example: tptforge sql "SELECT region, SUM(amount) AS total FROM
    /// 'in.csv' WHERE amount > 0 GROUP BY region ORDER BY total DESC LIMIT 5"
    Sql {
        /// The SELECT query; FROM takes a file path (csv/jsonl/json/tptcol,
        /// `.gz` supported) or an http(s) URL.
        query: String,
        /// Write results to a file instead of stdout (CSV).
        #[arg(long)]
        out: Option<PathBuf>,
        /// Error policy for malformed input rows: strict | skip |
        /// quarantine:<path>.
        #[arg(long, default_value = "strict")]
        on_error: String,
    },
    /// Print the inferred schema (column name + type) of a data file or URL.
    Schema {
        /// File path (csv/jsonl/json/tptcol, `.gz` supported) or http(s) URL.
        input: String,
        /// How many rows to scan before printing the schema.
        #[arg(long, default_value = "1000")]
        rows: usize,
        /// Save the inferred schema to this JSON file.
        #[arg(long, value_name = "FILE", conflicts_with = "against")]
        save: Option<PathBuf>,
        /// Compare against a schema saved with `--save`; exits with status 2
        /// on drift (added/removed/retyped/reordered columns).
        #[arg(long, value_name = "FILE")]
        against: Option<PathBuf>,
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

/// Attach a spinner progress bar to the pipeline's telemetry, and optionally
/// feed the Prometheus collector from the same event stream.
///
/// `on_progress` holds exactly one hook, so both consumers have to be driven
/// from a single closure rather than registered separately.
fn attach_progress(
    pipeline: &mut Pipeline,
    collector: Option<std::sync::Arc<crate::metrics::Metrics>>,
) {
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
        if let Some(collector) = &collector {
            collector.on_event(event);
        }
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

/// Execute `tptforge sql`.
pub async fn sql_command(
    query: &str,
    out: Option<&std::path::Path>,
    on_error: &str,
) -> Result<String> {
    let mut pipeline = crate::sql::build_sql_pipeline(query)?;
    pipeline.on_error(match on_error {
        "skip" => ErrorPolicy::Skip,
        other if other.starts_with("quarantine:") => {
            ErrorPolicy::Quarantine(other[11..].to_string())
        }
        _ => ErrorPolicy::Strict,
    });
    match out {
        Some(path) => {
            pipeline.write_csv(path.to_string_lossy());
            pipeline.execute().await?;
            Ok(format!("wrote {}", path.display()))
        }
        None => {
            let batches = pipeline.collect().await?;
            Ok(tpt_stream_core::source::batches_to_csv(&batches))
        }
    }
}

/// Options for [`run_with`].
#[derive(Debug, Clone)]
pub struct RunOptions {
    pub quiet: bool,
    /// Serve Prometheus metrics on this address during the run.
    pub metrics: Option<String>,
    pub metrics_name: String,
    pub metrics_allow_remote: bool,
    /// Write a provenance manifest (JSON) here.
    pub manifest: Option<PathBuf>,
    /// Validate and print the plan without reading or writing data.
    pub dry_run: bool,
}

impl Default for RunOptions {
    fn default() -> Self {
        RunOptions {
            quiet: false,
            metrics: None,
            metrics_name: "tptforge".to_string(),
            metrics_allow_remote: false,
            manifest: None,
            dry_run: false,
        }
    }
}

/// Execute `tptforge run` with the metrics endpoint disabled.
///
/// Convenience wrapper over [`run_with`] for embedders and tests that only
/// need the plain run behavior.
pub async fn run_command_default(pipeline_path: &std::path::Path, quiet: bool) -> Result<String> {
    run_with(
        pipeline_path,
        &RunOptions {
            quiet,
            ..RunOptions::default()
        },
    )
    .await
}

/// Execute `tptforge run`.
pub async fn run_command(
    pipeline_path: &std::path::Path,
    quiet: bool,
    metrics_addr: Option<String>,
    metrics_name: String,
    metrics_allow_remote: bool,
) -> Result<String> {
    run_with(
        pipeline_path,
        &RunOptions {
            quiet,
            metrics: metrics_addr,
            metrics_name,
            metrics_allow_remote,
            ..RunOptions::default()
        },
    )
    .await
}

/// Read a pipeline file (rejecting the retired YAML extensions) and parse it,
/// expanding `${VAR}` from the process environment.
fn load_spec(pipeline_path: &std::path::Path) -> Result<(String, PipelineSpec)> {
    let text = std::fs::read_to_string(pipeline_path)
        .with_context(|| format!("reading pipeline file {}", pipeline_path.display()))?;
    if is_yaml_path(pipeline_path) {
        bail!(
            "{} looks like a YAML pipeline, but pipeline files are TOML now \
             (the YAML reader pulled in an Apache-2.0-only dependency); \
             see tpt-stream-cli/README.md for the TOML layout",
            pipeline_path.display()
        );
    }
    let spec: PipelineSpec = parse_pipeline_toml(&text)
        .with_context(|| format!("parsing pipeline TOML {}", pipeline_path.display()))?;
    Ok((text, spec))
}

/// Parse a pipeline file for `validate` / `explain`. With `no_env`, unset
/// `${VAR}` references become `<VAR>` instead of an error, so a file can be
/// checked on a machine that does not hold the secrets.
pub fn load_spec_for_inspection(
    pipeline_path: &std::path::Path,
    no_env: bool,
) -> Result<PipelineSpec> {
    if !no_env {
        return load_spec(pipeline_path).map(|(_, spec)| spec);
    }
    let text = std::fs::read_to_string(pipeline_path)
        .with_context(|| format!("reading pipeline file {}", pipeline_path.display()))?;
    if is_yaml_path(pipeline_path) {
        bail!(
            "{} looks like a YAML pipeline; pipeline files are TOML now",
            pipeline_path.display()
        );
    }
    parse_pipeline_toml_with_env(&text, &|k| {
        Some(std::env::var(k).unwrap_or_else(|_| format!("<{k}>")))
    })
    .with_context(|| format!("parsing pipeline TOML {}", pipeline_path.display()))
}

/// `tptforge validate`: the report to print, or an error listing every problem.
pub fn validate_command(pipeline_path: &std::path::Path, no_env: bool) -> Result<String> {
    let spec = load_spec_for_inspection(pipeline_path, no_env)?;
    let diag = config::check_spec(&spec, false);
    if !diag.is_ok() {
        bail!(
            "{} has {} problem(s):\n  - {}",
            pipeline_path.display(),
            diag.errors.len(),
            diag.errors.join("\n  - ")
        );
    }
    let mut out = String::new();
    for w in &diag.warnings {
        out.push_str(&format!("warning: {w}\n"));
    }
    out.push_str(&format!(
        "{}: OK ({} stage(s))\n",
        pipeline_path.display(),
        spec.stages.len()
    ));
    Ok(out)
}

/// `tptforge explain`: the numbered plan.
pub fn explain_command(pipeline_path: &std::path::Path, no_env: bool) -> Result<String> {
    let spec = load_spec_for_inspection(pipeline_path, no_env)?;
    let mut out = config::explain_spec(&spec);
    for w in config::check_spec(&spec, false).warnings {
        out.push_str(&format!("warning: {w}\n"));
    }
    Ok(out)
}

/// `tptforge run`, with every option.
pub async fn run_with(pipeline_path: &std::path::Path, options: &RunOptions) -> Result<String> {
    let (text, spec) = load_spec(pipeline_path)?;

    if options.dry_run {
        let diag = config::check_spec(&spec, true);
        if !diag.is_ok() {
            bail!(
                "dry run found {} problem(s):\n  - {}",
                diag.errors.len(),
                diag.errors.join("\n  - ")
            );
        }
        let mut out = config::explain_spec(&spec);
        for w in &diag.warnings {
            out.push_str(&format!("warning: {w}\n"));
        }
        out.push_str("dry run: OK, nothing was read or written\n");
        return Ok(out);
    }

    let started_unix = manifest::now_unix();
    let inputs = if options.manifest.is_some() {
        manifest::hash_inputs(&spec)
    } else {
        Vec::new()
    };

    let mut pipeline = build_pipeline(&spec)?;

    // Serve metrics first: binding before the run means a port conflict is a
    // hard error the caller sees, not a silent no-op.
    let metrics = match options.metrics.clone() {
        Some(addr) => {
            let collector = std::sync::Arc::new(metrics::Metrics::new());
            let shutdown = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
            let name = options.metrics_name.clone();
            let bound = if options.metrics_allow_remote {
                metrics::serve_allow_remote(&addr, name, collector.clone(), shutdown.clone())
            } else {
                metrics::serve(&addr, name, collector.clone(), shutdown.clone())
            }
            .with_context(|| format!("binding metrics endpoint on {addr}"))?;
            eprintln!("metrics: http://{bound}/metrics");
            Some((collector, shutdown))
        }
        None => None,
    };

    // Telemetry feeds the progress bar and the metrics collector from one
    // hook, so `--quiet` and `--metrics` are independent rather than exclusive.
    let collector_for_hook = metrics.as_ref().map(|(c, _)| c.clone());
    if options.quiet {
        if let Some(collector) = collector_for_hook {
            pipeline.on_progress(std::sync::Arc::new(move |event| {
                collector.on_event(event);
            }));
        }
    } else {
        attach_progress(&mut pipeline, collector_for_hook);
    }

    let result = pipeline.execute().await;
    if let Some((collector, shutdown)) = &metrics {
        collector.add_dead_letter(pipeline.dead_letter_rows());
        // Report the final totals even on failure, so a scrape after the run
        // still shows what it managed to do.
        if let Ok(stats) = &result {
            collector.finish(stats.rows, stats.batches);
        } else {
            collector.finish(0, 0);
        }
        shutdown.store(true, std::sync::atomic::Ordering::Relaxed);
    }

    if let Some(path) = &options.manifest {
        let record = manifest::build_manifest(&manifest::ManifestInput {
            pipeline_path,
            spec_bytes: text.as_bytes(),
            started_unix,
            inputs,
            outputs: if result.is_ok() {
                manifest::hash_outputs(&spec)
            } else {
                Vec::new()
            },
            stats: result.as_ref().ok(),
            stages: pipeline.stage_stats(),
            dead_letter_rows: pipeline.dead_letter_rows(),
            error: result.as_ref().err().map(|e| e.to_string()),
        });
        manifest::write_manifest(path, &record)?;
    }

    let stats = result?;
    Ok(format!(
        "{} rows in {} batch(es), {} bytes out, in {:.1?}",
        stats.rows, stats.batches, stats.bytes_out, stats.elapsed
    ))
}

/// Whether a path uses one of the retired YAML pipeline extensions.
fn is_yaml_path(path: &std::path::Path) -> bool {
    matches!(
        path.extension().and_then(|ext| ext.to_str()),
        Some(ext) if ext.eq_ignore_ascii_case("yaml") || ext.eq_ignore_ascii_case("yml")
    )
}

/// Which reader a data file/URL needs, from its name.
pub fn input_kind(input: &str) -> &'static str {
    let lower = input.to_ascii_lowercase();
    if lower.starts_with("http://") || lower.starts_with("https://") {
        "http"
    } else if lower.ends_with(".jsonl")
        || lower.ends_with(".ndjson")
        || lower.ends_with(".jsonl.gz")
    {
        "jsonl"
    } else if lower.ends_with(".json") {
        "json"
    } else if lower.ends_with(".tptcol") {
        "columnar"
    } else {
        "csv"
    }
}

/// Attach the matching reader for a file/URL to `pipeline`.
pub(crate) fn configure_reader(pipeline: &mut Pipeline, input: &str) {
    match input_kind(input) {
        "http" => pipeline.read_http(input),
        "jsonl" => pipeline.read_jsonl(input),
        "json" => pipeline.read_json(input),
        "columnar" => pipeline.read_columnar(input),
        _ => pipeline.read_csv(input),
    };
}

/// Inspect a file/URL: read the first `rows` output rows through the
/// format's native reader.
pub(crate) async fn inspect(input: &str, rows: usize) -> Result<Vec<RecordBatch>> {
    let mut pipeline = Pipeline::new();
    configure_reader(&mut pipeline, input);
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

pub(crate) fn type_name(data_type: DataType) -> &'static str {
    match data_type {
        DataType::Int32 => "int32",
        DataType::Int64 => "int64",
        DataType::Float32 => "float32",
        DataType::Float64 => "float64",
        DataType::Bool => "bool",
        DataType::Date => "date",
        DataType::Timestamp => "timestamp",
        DataType::Utf8 => "string",
    }
}

/// Execute `tptforge preview`.
pub async fn preview_command(input: &str, num: usize) -> Result<String> {
    let batches = inspect(input, num).await?;
    Ok(tpt_stream_core::source::batches_to_csv(&batches))
}

/// Execute `tptforge completions`.
pub fn completions_command(shell: clap_complete::Shell) -> String {
    use clap::CommandFactory;
    let mut cmd = Cli::command();
    let mut buf = Vec::new();
    clap_complete::generate(shell, &mut cmd, "tptforge", &mut buf);
    String::from_utf8_lossy(&buf).into_owned()
}

/// The roff man page for `tptforge`; with `out_dir`, one page per subcommand
/// is written into that directory instead.
pub fn man_command(out_dir: Option<&std::path::Path>) -> Result<String> {
    use clap::CommandFactory;
    let cmd = Cli::command();
    match out_dir {
        None => {
            let mut buf = Vec::new();
            clap_mangen::Man::new(cmd).render(&mut buf)?;
            Ok(String::from_utf8_lossy(&buf).into_owned())
        }
        Some(dir) => {
            std::fs::create_dir_all(dir).with_context(|| format!("creating {}", dir.display()))?;
            let mut count = 1;
            let mut buf = Vec::new();
            clap_mangen::Man::new(cmd.clone()).render(&mut buf)?;
            std::fs::write(dir.join("tptforge.1"), &buf)?;
            for sub in cmd.get_subcommands() {
                let name = format!("tptforge-{}", sub.get_name());
                let mut buf = Vec::new();
                // clap wants a 'static name; this runs once per page in a
                // short-lived process, so leaking the string is harmless.
                let leaked: &'static str = Box::leak(name.clone().into_boxed_str());
                clap_mangen::Man::new(sub.clone().name(leaked)).render(&mut buf)?;
                std::fs::write(dir.join(format!("{name}.1")), &buf)?;
                count += 1;
            }
            Ok(format!("wrote {count} man page(s) to {}\n", dir.display()))
        }
    }
}
