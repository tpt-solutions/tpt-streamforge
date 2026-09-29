//! The data-oriented subcommands: `init`, `doctor`, `convert`, `diff`, schema
//! drift, and `run --watch`.

use std::cmp::Ordering;
use std::collections::HashSet;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

use anyhow::{anyhow, bail, Context, Result};
use serde_json::{json, Value as Json};
use tpt_stream_core::source::ColumnarSource;
use tpt_stream_core::value::DataType;
use tpt_stream_core::{Pipeline, RecordBatch, Source, Value};

use crate::config::{check_spec, source_local_path};
use crate::{configure_reader, inspect, type_name, StageSpec};

// ---------------------------------------------------------------------------
// Schema drift
// ---------------------------------------------------------------------------

fn schema_json(schema: &[(String, DataType)]) -> Json {
    json!({
        "version": 1,
        "columns": schema.iter().map(|(n, t)| json!({ "name": n, "type": type_name(*t) })).collect::<Vec<_>>(),
    })
}

async fn infer_schema(input: &str, rows: usize) -> Result<Vec<(String, DataType)>> {
    let batches = inspect(input, rows).await?;
    let Some(first) = batches.first() else {
        bail!("{input} has no rows; schema unknown");
    };
    Ok(first.schema())
}

/// `tptforge schema --save FILE`: write the inferred schema as JSON.
pub async fn schema_save(input: &str, rows: usize, path: &Path) -> Result<String> {
    let schema = infer_schema(input, rows).await?;
    let mut text = serde_json::to_string_pretty(&schema_json(&schema))?;
    text.push('\n');
    std::fs::write(path, text).with_context(|| format!("writing {}", path.display()))?;
    Ok(format!(
        "saved {} column(s) to {}",
        schema.len(),
        path.display()
    ))
}

/// Result of comparing a live schema with a saved one.
#[derive(Debug, PartialEq, Eq)]
pub struct DriftReport {
    pub drifted: bool,
    pub report: String,
}

/// Differences between a saved schema and the current one.
pub fn diff_schemas(saved: &[(String, String)], current: &[(String, String)]) -> Vec<String> {
    let mut lines = Vec::new();
    for (name, ty) in saved {
        match current.iter().find(|(n, _)| n == name) {
            None => lines.push(format!("- column removed: {name} ({ty})")),
            Some((_, now)) if now != ty => {
                lines.push(format!("~ column type changed: {name}: {ty} -> {now}"));
            }
            Some(_) => {}
        }
    }
    for (name, ty) in current {
        if !saved.iter().any(|(n, _)| n == name) {
            lines.push(format!("+ column added: {name} ({ty})"));
        }
    }
    let order = |cols: &[(String, String)], other: &[(String, String)]| -> Vec<String> {
        cols.iter()
            .filter(|(n, _)| other.iter().any(|(m, _)| m == n))
            .map(|(n, _)| n.clone())
            .collect()
    };
    if order(saved, current) != order(current, saved) {
        lines.push("~ column order changed".to_string());
    }
    lines
}

fn read_saved_schema(path: &Path) -> Result<Vec<(String, String)>> {
    let text = std::fs::read_to_string(path)
        .with_context(|| format!("reading saved schema {}", path.display()))?;
    let doc: Json = serde_json::from_str(&text).with_context(|| {
        format!(
            "{} is not a schema file written by `schema --save`",
            path.display()
        )
    })?;
    let cols = doc["columns"]
        .as_array()
        .ok_or_else(|| anyhow!("{}: missing `columns` array", path.display()))?;
    cols.iter()
        .map(|c| {
            Ok((
                c["name"]
                    .as_str()
                    .ok_or_else(|| anyhow!("{}: column without a name", path.display()))?
                    .to_string(),
                c["type"]
                    .as_str()
                    .ok_or_else(|| anyhow!("{}: column without a type", path.display()))?
                    .to_string(),
            ))
        })
        .collect()
}

/// `tptforge schema --against FILE`: compare the input's schema with a saved
/// one. `drifted` is true when they differ (the CLI then exits non-zero).
pub async fn schema_against(input: &str, rows: usize, path: &Path) -> Result<DriftReport> {
    let saved = read_saved_schema(path)?;
    let current: Vec<(String, String)> = infer_schema(input, rows)
        .await?
        .into_iter()
        .map(|(n, t)| (n, type_name(t).to_string()))
        .collect();
    let lines = diff_schemas(&saved, &current);
    if lines.is_empty() {
        Ok(DriftReport {
            drifted: false,
            report: format!(
                "schema matches {} ({} columns)\n",
                path.display(),
                current.len()
            ),
        })
    } else {
        Ok(DriftReport {
            drifted: true,
            report: format!(
                "schema drift against {}:\n{}\n",
                path.display(),
                lines.join("\n")
            ),
        })
    }
}

// ---------------------------------------------------------------------------
// init
// ---------------------------------------------------------------------------

fn toml_str(s: &str) -> String {
    // A JSON string literal is a valid TOML basic string.
    serde_json::to_string(s).expect("string serializes")
}

fn comment_safe(s: &str) -> String {
    s.chars()
        .map(|c| if c.is_control() { ' ' } else { c })
        .collect()
}

/// Generate a commented starter pipeline for `input`, inferring the schema
/// from its first `rows` rows.
pub async fn init_toml(input: &str, rows: usize) -> Result<String> {
    let batches = inspect(input, rows).await?;
    let Some(first) = batches.first() else {
        bail!("{input} has no rows; cannot infer a schema");
    };
    let schema = first.schema();
    let total: usize = batches.iter().map(RecordBatch::num_rows).sum();

    let mut no_nulls = Vec::new();
    let mut maybe_unique = Vec::new();
    for (ci, (name, _)) in schema.iter().enumerate() {
        let mut nulls = false;
        let mut seen: HashSet<String> = HashSet::new();
        let mut unique = true;
        for batch in &batches {
            let Some(col) = batch.column_by_index(ci) else {
                continue;
            };
            for r in 0..col.len() {
                if col.is_null(r) {
                    nulls = true;
                    unique = false;
                } else if unique {
                    let v = col.get(r).map(|v| v.to_string()).unwrap_or_default();
                    unique = seen.insert(v);
                }
            }
        }
        if !nulls {
            no_nulls.push(name.clone());
        }
        if unique && total > 1 {
            maybe_unique.push(name.clone());
        }
    }

    let kind = crate::input_kind(input);
    let mut out = String::new();
    out.push_str(&format!(
        "# Starter pipeline generated by `tptforge init` from {}\n",
        comment_safe(input)
    ));
    out.push_str(&format!(
        "# Schema inferred from the first {total} row(s):\n"
    ));
    let width = schema
        .iter()
        .map(|(n, _)| n.chars().count())
        .max()
        .unwrap_or(0);
    for (name, ty) in &schema {
        out.push_str(&format!(
            "#   {:<width$}  {}\n",
            comment_safe(name),
            type_name(*ty)
        ));
    }
    out.push_str(
        "#\n# Strings may use ${VAR} / ${VAR:-default} for environment variables\n\
         # (keep passwords out of this file); write $${ for a literal ${.\n\n",
    );
    out.push_str("error_policy = \"strict\"\n\n");
    out.push_str(&format!("[source.{kind}]\npath = {}\n\n", toml_str(input)));
    out.push_str(
        "# Add stages below. Examples (uncomment and adjust):\n\
         #\n\
         # [[stages]]\n\
         # filter = \"<column> > 0\"\n\
         #\n\
         # [[stages]]\n\
         # select = [\"<column>\", \"<column>\"]\n\
         #\n\
         # [[stages]]\n\
         # sort = { columns = [\"<column>\"], descending = false }\n\
         #\n\
         # [[stages]]\n\
         # limit = 1000\n\n",
    );
    out.push_str("# Data-quality checks: the run fails if any of these does not hold.\n");
    out.push_str("[[stages]]\n[stages.expect]\nrows_at_least = 1\n");
    if !no_nulls.is_empty() {
        let list: Vec<String> = no_nulls.iter().map(|n| toml_str(n)).collect();
        out.push_str(&format!(
            "# These columns had no nulls in the sample:\nno_nulls = [{}]\n",
            list.join(", ")
        ));
    }
    if !maybe_unique.is_empty() {
        let list: Vec<String> = maybe_unique.iter().map(|n| toml_str(n)).collect();
        out.push_str(&format!(
            "# Unique in the sample -- enable for real key columns:\n# unique = [{}]\n",
            list.join(", ")
        ));
    }
    out.push_str("\n[sink.csv]\npath = \"out.csv\"\n");
    Ok(out)
}

// ---------------------------------------------------------------------------
// doctor
// ---------------------------------------------------------------------------

/// Output of [`doctor_command`].
pub struct DoctorReport {
    pub text: String,
    /// Number of failed checks; the CLI exits non-zero when this is > 0.
    pub failures: usize,
}

pub fn doctor_command(pipeline: Option<&Path>) -> DoctorReport {
    let mut text = String::new();
    let mut failures = 0usize;
    let mut line = |level: &str, msg: String| {
        text.push_str(&format!("[{level:<4}] {msg}\n"));
    };

    line("ok", format!("tptforge {}", env!("CARGO_PKG_VERSION")));

    let probe = std::env::temp_dir().join(format!("tptforge-doctor-{}", std::process::id()));
    match std::fs::write(&probe, b"x").and_then(|()| std::fs::remove_file(&probe)) {
        Ok(()) => line(
            "ok",
            format!(
                "temp dir {} is writable (used for sort spill files)",
                std::env::temp_dir().display()
            ),
        ),
        Err(e) => {
            failures += 1;
            line(
                "FAIL",
                format!(
                    "temp dir {} is not writable: {e}",
                    std::env::temp_dir().display()
                ),
            );
        }
    }

    let aws = tpt_stream_core::CloudCredentials::from_env().is_ok();
    let azure = tpt_stream_core::AzureCredentials::from_env().is_ok();
    line(
        "info",
        format!(
            "S3/GCS credentials (AWS_*): {}",
            if aws { "found" } else { "not set" }
        ),
    );
    line(
        "info",
        format!(
            "Azure credentials (AZURE_*): {}",
            if azure { "found" } else { "not set" }
        ),
    );
    if std::env::var_os("TPT_ALLOW_INSECURE_HTTP").is_some() {
        line(
            "warn",
            "TPT_ALLOW_INSECURE_HTTP is set: plaintext http:// to non-loopback hosts is allowed"
                .into(),
        );
    }

    if let Some(path) = pipeline {
        match std::fs::read_to_string(path) {
            Err(e) => {
                failures += 1;
                line("FAIL", format!("cannot read {}: {e}", path.display()));
            }
            Ok(body) => match crate::config::parse_pipeline_toml(&body) {
                Err(e) => {
                    failures += 1;
                    line("FAIL", format!("{}: {e:#}", path.display()));
                }
                Ok(spec) => {
                    line(
                        "ok",
                        format!("{} parses ({} stage(s))", path.display(), spec.stages.len()),
                    );
                    let diag = check_spec(&spec, true);
                    for e in &diag.errors {
                        failures += 1;
                        line("FAIL", e.clone());
                    }
                    for w in &diag.warnings {
                        line("warn", w.clone());
                    }
                    let uses = |kind: &str| -> bool {
                        let src = matches!(
                            (&spec.source, kind),
                            (crate::SourceSpec::S3(_) | crate::SourceSpec::Gcs(_), "aws")
                                | (crate::SourceSpec::Azure(_), "azure")
                        );
                        let snk = matches!(
                            (&spec.sink, kind),
                            (
                                Some(crate::SinkSpec::S3(_) | crate::SinkSpec::Gcs(_)),
                                "aws"
                            ) | (Some(crate::SinkSpec::Azure(_)), "azure")
                        );
                        src || snk
                    };
                    if uses("aws") && !aws {
                        failures += 1;
                        line(
                            "FAIL",
                            "pipeline uses S3/GCS but AWS_* credentials are not set".into(),
                        );
                    }
                    if uses("azure") && !azure {
                        failures += 1;
                        line(
                            "FAIL",
                            "pipeline uses Azure but AZURE_* credentials are not set".into(),
                        );
                    }
                }
            },
        }
    }
    if failures == 0 {
        text.push_str("all checks passed\n");
    } else {
        text.push_str(&format!("{failures} check(s) failed\n"));
    }
    DoctorReport { text, failures }
}

// ---------------------------------------------------------------------------
// convert
// ---------------------------------------------------------------------------

/// `tptforge convert IN OUT`: re-encode a file; formats come from extensions.
pub async fn convert_command(
    input: &str,
    output: &Path,
    on_error: &str,
    zstd: bool,
) -> Result<String> {
    let out_str = output.to_string_lossy().to_string();
    let lower = out_str.to_ascii_lowercase();
    let mut pipeline = Pipeline::new();
    pipeline.on_error(crate::parse_error_policy(on_error)?);
    configure_reader(&mut pipeline, input);
    if lower.ends_with(".csv") {
        pipeline.write_csv(&out_str);
    } else if lower.ends_with(".jsonl") || lower.ends_with(".ndjson") {
        pipeline.write_jsonl(&out_str);
    } else if lower.ends_with(".json") {
        pipeline.write_json(&out_str, false);
    } else if lower.ends_with(".tptcol") {
        pipeline.write_columnar(&out_str, zstd);
    } else {
        bail!(
            "cannot infer the output format from {out_str:?}; use a .csv, .jsonl, .ndjson, \
             .json, or .tptcol extension"
        );
    }
    let stats = pipeline
        .execute()
        .await
        .with_context(|| format!("converting {input}"))?;
    Ok(format!(
        "converted {} row(s) from {input} to {}",
        stats.rows,
        output.display()
    ))
}

// ---------------------------------------------------------------------------
// diff
// ---------------------------------------------------------------------------

/// Counts from [`diff_command`].
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct DiffSummary {
    pub added: u64,
    pub removed: u64,
    pub changed: u64,
    pub unchanged: u64,
    /// Notes about columns present on only one side.
    pub notes: Vec<String>,
}

impl DiffSummary {
    pub fn differs(&self) -> bool {
        self.added + self.removed + self.changed > 0
    }
}

impl std::fmt::Display for DiffSummary {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for note in &self.notes {
            writeln!(f, "note: {note}")?;
        }
        write!(
            f,
            "{} added, {} removed, {} changed, {} unchanged",
            self.added, self.removed, self.changed, self.unchanged
        )
    }
}

/// Order matching the engine's sort: nulls (and empty strings) first, then
/// by value. Only ever compares same-typed cells (key types are checked).
fn cmp_value(a: &Value, b: &Value) -> Ordering {
    fn is_nullish(v: &Value) -> bool {
        matches!(v, Value::Null) || matches!(v, Value::Utf8(s) if s.is_empty())
    }
    match (is_nullish(a), is_nullish(b)) {
        (true, true) => return Ordering::Equal,
        (true, false) => return Ordering::Less,
        (false, true) => return Ordering::Greater,
        _ => {}
    }
    match (a, b) {
        (Value::Int32(x), Value::Int32(y)) => x.cmp(y),
        (Value::Int64(x), Value::Int64(y)) => x.cmp(y),
        (Value::Float32(x), Value::Float32(y)) => x.total_cmp(y),
        (Value::Float64(x), Value::Float64(y)) => x.total_cmp(y),
        (Value::Bool(x), Value::Bool(y)) => x.cmp(y),
        (Value::Date(x), Value::Date(y)) => x.cmp(y),
        (Value::Timestamp(x), Value::Timestamp(y)) => x.cmp(y),
        (Value::Utf8(x), Value::Utf8(y)) => x.as_bytes().cmp(y.as_bytes()),
        _ => a.to_string().cmp(&b.to_string()),
    }
}

fn cmp_keys(a: &[Value], b: &[Value]) -> Ordering {
    a.iter()
        .zip(b)
        .map(|(x, y)| cmp_value(x, y))
        .find(|o| *o != Ordering::Equal)
        .unwrap_or(Ordering::Equal)
}

fn values_equal(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Null, Value::Null) => true,
        (Value::Null, _) | (_, Value::Null) => false,
        _ => a == b || a.to_string() == b.to_string(),
    }
}

fn csv_field(out: &mut Vec<u8>, text: &str) {
    if text.contains([',', '"', '\n', '\r']) {
        out.push(b'"');
        out.extend_from_slice(text.replace('"', "\"\"").as_bytes());
        out.push(b'"');
    } else {
        out.extend_from_slice(text.as_bytes());
    }
}

fn write_row(out: &mut dyn Write, mark: &str, row: &[Value]) -> Result<()> {
    let mut buf = Vec::new();
    buf.extend_from_slice(mark.as_bytes());
    for v in row {
        buf.push(b',');
        if !matches!(v, Value::Null) {
            csv_field(&mut buf, &v.to_string());
        }
    }
    buf.push(b'\n');
    out.write_all(&buf)?;
    Ok(())
}

/// One sorted side of the diff, read lazily from its sorted temp file.
struct Side {
    name: String,
    src: ColumnarSource,
    batch: Option<RecordBatch>,
    idx: usize,
    schema: Option<Vec<(String, DataType)>>,
    /// Positions (in this side's schema) of the output columns.
    out_idx: Vec<usize>,
    /// Positions of the key columns within the *output* row.
    key_pos: Vec<usize>,
    cur: Option<Vec<Value>>,
    prev_key: Option<Vec<Value>>,
}

impl Side {
    fn new(name: &str, path: &Path) -> Self {
        Side {
            name: name.to_string(),
            src: ColumnarSource::open(path.to_string_lossy().to_string()),
            batch: None,
            idx: 0,
            schema: None,
            out_idx: Vec::new(),
            key_pos: Vec::new(),
            cur: None,
            prev_key: None,
        }
    }

    /// Make sure a current row is loaded; false at end of input.
    async fn fill(&mut self) -> Result<bool> {
        loop {
            if let Some(b) = &self.batch {
                if self.idx < b.num_rows() {
                    if self.cur.is_none() && !self.out_idx.is_empty() {
                        let row = self
                            .out_idx
                            .iter()
                            .map(|&c| {
                                b.column_by_index(c)
                                    .and_then(|col| col.get(self.idx))
                                    .unwrap_or(Value::Null)
                            })
                            .collect();
                        self.cur = Some(row);
                    }
                    return Ok(true);
                }
            }
            match self.src.next_batch().await? {
                Some(b) => {
                    if self.schema.is_none() {
                        self.schema = Some(b.schema());
                    }
                    self.batch = Some(b);
                    self.idx = 0;
                    if self.out_idx.is_empty() {
                        // Output layout not decided yet (first look at the
                        // schema): the caller re-fills after configuring.
                        return Ok(true);
                    }
                }
                None => {
                    self.batch = None;
                    return Ok(false);
                }
            }
        }
    }

    /// Consume the current row, enforcing strictly ascending unique keys.
    fn take(&mut self) -> Result<Vec<Value>> {
        let row = self.cur.take().expect("fill() returned true");
        self.idx += 1;
        let key: Vec<Value> = self.key_pos.iter().map(|&p| row[p].clone()).collect();
        if let Some(prev) = &self.prev_key {
            match cmp_keys(prev, &key) {
                Ordering::Less => {}
                Ordering::Equal => bail!(
                    "duplicate key ({}) in {}; `diff` needs unique keys",
                    key.iter()
                        .map(ToString::to_string)
                        .collect::<Vec<_>>()
                        .join(", "),
                    self.name
                ),
                Ordering::Greater => bail!(
                    "{} is not in key order after sorting (internal error near key ({}))",
                    self.name,
                    key.iter()
                        .map(ToString::to_string)
                        .collect::<Vec<_>>()
                        .join(", ")
                ),
            }
        }
        self.prev_key = Some(key);
        Ok(row)
    }
}

async fn sort_to_temp(input: &str, keys: &[String], dir: &Path, tag: &str) -> Result<PathBuf> {
    let path = dir.join(format!("{tag}.tptcol"));
    let mut pipeline = Pipeline::new();
    configure_reader(&mut pipeline, input);
    let refs: Vec<&str> = keys.iter().map(String::as_str).collect();
    pipeline.sort_by(&refs);
    pipeline.write_columnar(path.to_string_lossy().to_string(), false);
    pipeline
        .execute()
        .await
        .with_context(|| format!("sorting {input} by {}", keys.join(", ")))?;
    Ok(path)
}

/// `tptforge diff A B --key id`.
///
/// Both inputs are sorted by the key with the engine's external sort (spilling
/// to disk beyond the run limit) and then compared with a single merge pass, so
/// the comparison itself holds two rows at a time. Output is CSV: a `_diff`
/// column (`-` row only in A or A's version of a changed row, `+` row only in B
/// or B's version) followed by the columns both files share.
pub async fn diff_command(
    a: &str,
    b: &str,
    keys: &[String],
    out: &mut dyn Write,
) -> Result<DiffSummary> {
    if keys.is_empty() {
        bail!("diff needs at least one --key column");
    }
    let dir = tempfile::tempdir().context("creating a temp dir for the sorted inputs")?;
    let path_a = sort_to_temp(a, keys, dir.path(), "a").await?;
    let path_b = sort_to_temp(b, keys, dir.path(), "b").await?;
    let mut sa = Side::new(a, &path_a);
    let mut sb = Side::new(b, &path_b);

    // First look at both schemas (an empty input has none).
    sa.fill().await?;
    sb.fill().await?;
    let mut summary = DiffSummary::default();

    let schema_a = sa.schema.clone();
    let schema_b = sb.schema.clone();
    let columns: Vec<String> = match (&schema_a, &schema_b) {
        (Some(x), Some(y)) => {
            let only_a: Vec<&str> = x
                .iter()
                .filter(|(n, _)| !y.iter().any(|(m, _)| m == n))
                .map(|(n, _)| n.as_str())
                .collect();
            let only_b: Vec<&str> = y
                .iter()
                .filter(|(n, _)| !x.iter().any(|(m, _)| m == n))
                .map(|(n, _)| n.as_str())
                .collect();
            if !only_a.is_empty() {
                summary.notes.push(format!(
                    "columns only in {a} (ignored): {}",
                    only_a.join(", ")
                ));
            }
            if !only_b.is_empty() {
                summary.notes.push(format!(
                    "columns only in {b} (ignored): {}",
                    only_b.join(", ")
                ));
            }
            x.iter()
                .filter(|(n, _)| y.iter().any(|(m, _)| m == n))
                .map(|(n, _)| n.clone())
                .collect()
        }
        (Some(s), None) | (None, Some(s)) => s.iter().map(|(n, _)| n.clone()).collect(),
        (None, None) => Vec::new(),
    };
    if columns.is_empty() && schema_a.is_some() && schema_b.is_some() {
        bail!("{a} and {b} have no columns in common");
    }
    for k in keys {
        if !columns.is_empty() && !columns.contains(k) {
            bail!(
                "key column {k:?} is not present in both inputs; shared columns: {}",
                columns.join(", ")
            );
        }
    }
    if let (Some(x), Some(y)) = (&schema_a, &schema_b) {
        for k in keys {
            let ta = x.iter().find(|(n, _)| n == k).map(|(_, t)| *t);
            let tb = y.iter().find(|(n, _)| n == k).map(|(_, t)| *t);
            if ta != tb {
                bail!(
                    "key column {k:?} is {} in {a} but {} in {b}",
                    ta.map_or("?", type_name),
                    tb.map_or("?", type_name)
                );
            }
        }
    }
    let configure = |side: &mut Side| {
        if let Some(schema) = &side.schema {
            side.out_idx = columns
                .iter()
                .filter_map(|c| schema.iter().position(|(n, _)| n == c))
                .collect();
        }
        side.key_pos = keys
            .iter()
            .filter_map(|k| columns.iter().position(|c| c == k))
            .collect();
    };
    configure(&mut sa);
    configure(&mut sb);

    // Header, then reload the first rows now the layout is known.
    let mut header = Vec::new();
    header.extend_from_slice(b"_diff");
    for c in &columns {
        header.push(b',');
        csv_field(&mut header, c);
    }
    header.push(b'\n');
    out.write_all(&header)?;

    loop {
        let ha = sa.fill().await?;
        let hb = sb.fill().await?;
        match (ha, hb) {
            (false, false) => break,
            (true, false) => {
                write_row(out, "-", &sa.take()?)?;
                summary.removed += 1;
            }
            (false, true) => {
                write_row(out, "+", &sb.take()?)?;
                summary.added += 1;
            }
            (true, true) => {
                let ord = {
                    let ra = sa.cur.as_ref().expect("filled");
                    let rb = sb.cur.as_ref().expect("filled");
                    let ka: Vec<Value> = sa.key_pos.iter().map(|&p| ra[p].clone()).collect();
                    let kb: Vec<Value> = sb.key_pos.iter().map(|&p| rb[p].clone()).collect();
                    cmp_keys(&ka, &kb)
                };
                match ord {
                    Ordering::Less => {
                        write_row(out, "-", &sa.take()?)?;
                        summary.removed += 1;
                    }
                    Ordering::Greater => {
                        write_row(out, "+", &sb.take()?)?;
                        summary.added += 1;
                    }
                    Ordering::Equal => {
                        let ra = sa.take()?;
                        let rb = sb.take()?;
                        if ra.iter().zip(&rb).all(|(x, y)| values_equal(x, y)) {
                            summary.unchanged += 1;
                        } else {
                            write_row(out, "-", &ra)?;
                            write_row(out, "+", &rb)?;
                            summary.changed += 1;
                        }
                    }
                }
            }
        }
    }
    out.flush()?;
    Ok(summary)
}

// ---------------------------------------------------------------------------
// run --watch
// ---------------------------------------------------------------------------

type Stamp = Vec<(PathBuf, Option<(SystemTime, u64)>)>;

fn stamp_of(paths: &[PathBuf]) -> Stamp {
    paths
        .iter()
        .map(|p| {
            let meta = std::fs::metadata(p).ok();
            (
                p.clone(),
                meta.and_then(|m| m.modified().ok().map(|t| (t, m.len()))),
            )
        })
        .collect()
}

/// The pipeline file plus every local input it reads, best effort (a file
/// that does not parse is watched on its own so fixing it triggers a rerun).
fn watched_paths(pipeline: &Path) -> Vec<PathBuf> {
    let mut paths = vec![pipeline.to_path_buf()];
    if let Ok(text) = std::fs::read_to_string(pipeline) {
        if let Ok(spec) = crate::config::parse_pipeline_toml_with_env(&text, &|k| {
            Some(std::env::var(k).unwrap_or_default())
        }) {
            if let Some(p) = source_local_path(&spec.source) {
                paths.push(PathBuf::from(p));
            }
            for stage in &spec.stages {
                if let StageSpec::Join(j) = stage {
                    paths.push(PathBuf::from(&j.right));
                }
            }
        }
    }
    paths
}

/// `tptforge run --watch`: run, then rerun whenever the pipeline file or a
/// local input changes (modification time or size, polled every `poll`).
/// Failed runs are reported and watching continues. Stops after `max_runs`
/// runs when given (tests); otherwise runs until interrupted.
pub async fn watch_command(
    pipeline: &Path,
    options: &crate::RunOptions,
    poll: Duration,
    max_runs: Option<usize>,
) -> Result<()> {
    if options.metrics.is_some() {
        bail!(
            "--watch cannot be combined with --metrics (the endpoint would be rebound every run)"
        );
    }
    let mut runs = 0usize;
    loop {
        let watched = watched_paths(pipeline);
        let before = stamp_of(&watched);
        match crate::run_with(pipeline, options).await {
            Ok(summary) => println!("{summary}"),
            Err(e) => eprintln!("tptforge: {e:#}"),
        }
        runs += 1;
        if max_runs.is_some_and(|m| runs >= m) {
            return Ok(());
        }
        eprintln!(
            "watching {} for changes (Ctrl-C to stop)",
            pipeline.display()
        );
        // Re-derive the watch list each time so edits that add an input are
        // picked up; compare against the stamp taken before the run so a
        // change made *during* the run triggers an immediate rerun.
        let mut before = before;
        loop {
            tokio::time::sleep(poll).await;
            let now_paths = watched_paths(pipeline);
            let now = stamp_of(&now_paths);
            if now != before {
                break;
            }
            before = now;
        }
    }
}
