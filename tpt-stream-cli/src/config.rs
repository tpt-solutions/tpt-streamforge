//! Loading a pipeline file: `${VAR}` substitution, located error messages,
//! did-you-mean hints, static validation, and the human-readable plan.
//!
//! Parsing happens in two passes. The first deserializes the document into a
//! [`RawDoc`] whose parts are `Spanned<toml::Value>`, so every source, stage,
//! and sink remembers its byte offset. The second converts each part into its
//! typed spec; a failure there is reported as
//! `line 12, column 3: stage #2: <message>` instead of a bare serde message.
//!
//! Environment substitution runs on the parsed *values* (never on the raw
//! text), so a secret containing a quote cannot break the TOML syntax and can
//! never end up in a TOML parser snippet.

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::ops::Range;

use anyhow::{anyhow, Result};
use serde::Deserialize;
use toml::{Spanned, Value};

use crate::{PipelineSpec, SinkSpec, SourceSpec, StageSpec};

/// Looks up an environment variable; injected so tests need not touch the
/// (process-global) real environment.
pub type EnvLookup<'a> = &'a dyn Fn(&str) -> Option<String>;

// ---------------------------------------------------------------------------
// did-you-mean
// ---------------------------------------------------------------------------

/// Optimal-string-alignment distance: Levenshtein plus adjacent transposition,
/// so `cvs` is one edit from `csv`.
fn edit_distance(a: &str, b: &str) -> usize {
    let a: Vec<char> = a.chars().collect();
    let b: Vec<char> = b.chars().collect();
    let mut prev2: Vec<usize> = vec![0; b.len() + 1];
    let mut prev: Vec<usize> = (0..=b.len()).collect();
    for i in 0..a.len() {
        let mut cur = vec![i + 1];
        for j in 0..b.len() {
            let cost = usize::from(a[i] != b[j]);
            let mut best = (prev[j] + cost).min(prev[j + 1] + 1).min(cur[j] + 1);
            if i > 0 && j > 0 && a[i] == b[j - 1] && a[i - 1] == b[j] {
                best = best.min(prev2[j - 1] + 1);
            }
            cur.push(best);
        }
        prev2 = std::mem::replace(&mut prev, cur);
    }
    prev[b.len()]
}

/// The closest candidate to `word`, if any is plausibly a typo of it.
pub fn did_you_mean<'a>(word: &str, candidates: &[&'a str]) -> Option<&'a str> {
    let limit = (word.chars().count() / 3).clamp(1, 3);
    candidates
        .iter()
        .map(|c| (edit_distance(&word.to_ascii_lowercase(), c), *c))
        .filter(|(d, _)| *d <= limit)
        .min_by_key(|(d, _)| *d)
        .map(|(_, c)| c)
}

/// Backtick-quoted tokens in a serde message, in order.
fn backticked(message: &str) -> Vec<&str> {
    message
        .split('`')
        .enumerate()
        .filter(|(i, _)| i % 2 == 1)
        .map(|(_, t)| t)
        .collect()
}

/// For `unknown field `x`, expected one of `a`, `b``, the hint text
/// (` -- did you mean `a`?`), if one is close enough.
fn hint_from_serde(message: &str) -> Option<String> {
    if !message.contains("unknown field") {
        return None;
    }
    let tokens = backticked(message);
    let (unknown, expected) = tokens.split_first()?;
    did_you_mean(unknown, expected).map(|s| format!(" -- did you mean `{s}`?"))
}

// ---------------------------------------------------------------------------
// Locations
// ---------------------------------------------------------------------------

/// 1-based `(line, column)` of a byte offset.
pub fn line_col(text: &str, offset: usize) -> (usize, usize) {
    let offset = offset.min(text.len());
    let mut line = 1;
    let mut line_start = 0;
    for (i, b) in text.bytes().enumerate().take(offset) {
        if b == b'\n' {
            line += 1;
            line_start = i + 1;
        }
    }
    let col = text
        .get(line_start..offset)
        .map_or(offset - line_start, |s| s.chars().count())
        + 1;
    (line, col)
}

/// Best offset for an error inside a part that starts at `span.start`: the
/// first occurrence of a token the message names (`` `rows` `` or `"parquet"`),
/// else the start of the part.
fn refine(text: &str, span: &Range<usize>, message: &str) -> usize {
    let start = span.start.min(text.len());
    let tail = &text[start..];
    let mut tokens: Vec<String> = Vec::new();
    if let Some(t) = backticked(message).first() {
        tokens.push((*t).to_string());
    }
    if let Some(open) = message.find('"') {
        if let Some(len) = message[open + 1..].find('"') {
            tokens.push(message[open + 1..open + 1 + len].to_string());
        }
    }
    for token in tokens {
        if token.is_empty() {
            continue;
        }
        if let Some(pos) = tail.find(&token) {
            return start + pos;
        }
    }
    start
}

fn located(text: &str, offset: usize, what: &str, message: &str) -> anyhow::Error {
    let (line, col) = line_col(text, offset);
    let mut out = format!("line {line}, column {col}");
    if !what.is_empty() {
        let _ = write!(out, ": {what}");
    }
    let _ = write!(out, ": {message}");
    if let Some(hint) = hint_from_serde(message) {
        out.push_str(&hint);
    }
    anyhow!(out)
}

// ---------------------------------------------------------------------------
// ${VAR} substitution
// ---------------------------------------------------------------------------

/// Why a string failed substitution.
#[derive(Debug, PartialEq, Eq)]
pub enum SubstError {
    /// `${NAME}` with `NAME` unset and no `:-default`.
    Unset(String),
    /// `${` that is not `${NAME}` / `${NAME:-default}`.
    Malformed(String),
}

impl std::fmt::Display for SubstError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SubstError::Unset(name) => write!(
                f,
                "environment variable {name} is not set (use `${{{name}:-default}}` to \
                 give it a default, or `$${{{name}}}` for a literal `${{{name}}}`)"
            ),
            SubstError::Malformed(why) => write!(f, "{why}"),
        }
    }
}

fn valid_name(name: &str) -> bool {
    let mut chars = name.chars();
    matches!(chars.next(), Some(c) if c.is_ascii_alphabetic() || c == '_')
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

/// Expand `${VAR}` and `${VAR:-default}` in `input`.
///
/// * `${VAR}` -- the variable's value; an error if unset (set-but-empty is
///   allowed and yields an empty string).
/// * `${VAR:-default}` -- the default when `VAR` is unset *or empty*.
/// * `$${` -- an escape: yields a literal `${` and is not expanded.
///
/// A lone `$` (as in a regex or a price) is left alone; only `${` is special.
pub fn substitute_env(input: &str, env: EnvLookup) -> std::result::Result<String, SubstError> {
    let mut out = String::with_capacity(input.len());
    let mut rest = input;
    while let Some(pos) = rest.find('$') {
        out.push_str(&rest[..pos]);
        let after = &rest[pos..];
        if let Some(escaped) = after.strip_prefix("$${") {
            out.push_str("${");
            rest = escaped;
        } else if let Some(body) = after.strip_prefix("${") {
            let Some(end) = body.find('}') else {
                return Err(SubstError::Malformed(
                    "unterminated `${` (write `$${` for a literal `${`)".into(),
                ));
            };
            let inner = &body[..end];
            let (name, default) = match inner.split_once(":-") {
                Some((n, d)) => (n, Some(d)),
                None => (inner, None),
            };
            if !valid_name(name) {
                return Err(SubstError::Malformed(format!(
                    "`${{{inner}}}` is not a valid variable reference: a name is letters, digits \
                     and `_` and cannot start with a digit (write `$${{` for a literal `${{`)"
                )));
            }
            match (env(name), default) {
                (Some(v), Some(d)) => out.push_str(if v.is_empty() { d } else { &v }),
                (Some(v), None) => out.push_str(&v),
                (None, Some(d)) => out.push_str(d),
                (None, None) => return Err(SubstError::Unset(name.to_string())),
            }
            rest = &body[end + 1..];
        } else {
            out.push('$');
            rest = &after[1..];
        }
    }
    out.push_str(rest);
    Ok(out)
}

/// Substitute inside every string *value* (keys are never expanded).
fn substitute_value(value: &mut Value, env: EnvLookup) -> std::result::Result<(), SubstError> {
    match value {
        Value::String(s) => {
            if s.contains('$') {
                *s = substitute_env(s, env)?;
            }
        }
        Value::Array(items) => {
            for item in items {
                substitute_value(item, env)?;
            }
        }
        Value::Table(table) => {
            for (_, v) in table.iter_mut() {
                substitute_value(v, env)?;
            }
        }
        _ => {}
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Parsing
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawDoc {
    source: Option<Spanned<Value>>,
    #[serde(default)]
    stages: Vec<Spanned<Value>>,
    error_policy: Option<Spanned<Value>>,
    dead_letter: Option<Spanned<Value>>,
    sink: Option<Spanned<Value>>,
}

/// Parse a pipeline definition, expanding `${VAR}` from the process
/// environment. See [`parse_pipeline_toml_with_env`].
pub fn parse_pipeline_toml(text: &str) -> Result<PipelineSpec> {
    parse_pipeline_toml_with_env(text, &|k| std::env::var(k).ok())
}

/// Parse a pipeline definition with an explicit environment lookup.
///
/// Errors carry `line L, column C`, the offending part (`source`,
/// `stage #N`, `sink`), and a did-you-mean hint for misspelled keys.
pub fn parse_pipeline_toml_with_env(text: &str, env: EnvLookup) -> Result<PipelineSpec> {
    let raw: RawDoc = toml::from_str(text).map_err(|e| {
        let offset = e.span().map_or(0, |s| s.start);
        located(text, offset, "", e.message().trim())
    })?;

    let part = |spanned: Spanned<Value>, what: &str| -> Result<Value> {
        let span = spanned.span();
        let mut value = spanned.into_inner();
        if let Err(e) = substitute_value(&mut value, env) {
            // Point at the `${...` that failed, not just the start of the part.
            let needle = match &e {
                SubstError::Unset(name) => format!("${{{name}"),
                SubstError::Malformed(_) => "${".to_string(),
            };
            let offset = text
                .get(span.start..)
                .and_then(|t| t.find(&needle))
                .map_or(span.start, |p| span.start + p);
            return Err(located(text, offset, what, &e.to_string()));
        }
        Ok(value)
    };

    let source_raw = raw.source.ok_or_else(|| {
        anyhow!(
            "missing required key `source` (add a `[source.csv]` table with a `path`, \
             or another source kind)"
        )
    })?;
    let source_span = source_raw.span();
    let source_value = part(source_raw, "source")?;
    let source: SourceSpec = source_value.try_into().map_err(|e: toml::de::Error| {
        let msg = e.message().trim().to_string();
        located(text, refine(text, &source_span, &msg), "source", &msg)
    })?;

    let mut stages = Vec::with_capacity(raw.stages.len());
    for (i, stage_raw) in raw.stages.into_iter().enumerate() {
        let span = stage_raw.span();
        let what = format!("stage #{}", i + 1);
        let value = part(stage_raw, &what)?;
        let stage: StageSpec = value.try_into().map_err(|e: toml::de::Error| {
            let msg = e.message().trim().to_string();
            located(text, refine(text, &span, &msg), &what, &msg)
        })?;
        stages.push(stage);
    }

    let string_key = |raw: Option<Spanned<Value>>, key: &str| -> Result<Option<String>> {
        let Some(raw) = raw else { return Ok(None) };
        let span = raw.span();
        match part(raw, key)? {
            Value::String(s) => Ok(Some(s)),
            other => Err(located(
                text,
                span.start,
                key,
                &format!("must be a string, got {}", other.type_str()),
            )),
        }
    };
    let error_policy = string_key(raw.error_policy, "error_policy")?;
    let dead_letter = string_key(raw.dead_letter, "dead_letter")?;

    let sink = match raw.sink {
        None => None,
        Some(sink_raw) => {
            let span = sink_raw.span();
            let value = part(sink_raw, "sink")?;
            let sink: SinkSpec = value.try_into().map_err(|e: toml::de::Error| {
                let msg = e.message().trim().to_string();
                located(text, refine(text, &span, &msg), "sink", &msg)
            })?;
            Some(sink)
        }
    };

    Ok(PipelineSpec {
        source,
        stages,
        error_policy,
        dead_letter,
        sink,
    })
}

// ---------------------------------------------------------------------------
// Static validation
// ---------------------------------------------------------------------------

/// Findings from [`check_spec`].
#[derive(Debug, Default, PartialEq, Eq)]
pub struct Diagnostics {
    /// Problems that would make the run fail.
    pub errors: Vec<String>,
    /// Things worth a look that do not stop a run.
    pub warnings: Vec<String>,
}

impl Diagnostics {
    pub fn is_ok(&self) -> bool {
        self.errors.is_empty()
    }
}

fn is_remote(path: &str) -> bool {
    let lower = path.to_ascii_lowercase();
    lower.starts_with("http://") || lower.starts_with("https://")
}

/// The local file a source reads, if it reads one.
pub fn source_local_path(source: &SourceSpec) -> Option<&str> {
    match source {
        SourceSpec::Csv(s) | SourceSpec::Jsonl(s) | SourceSpec::Json(s) => Some(&s.path),
        SourceSpec::Columnar(s) => Some(&s.path),
        SourceSpec::Http(s) if !is_remote(&s.path) => Some(&s.path),
        SourceSpec::Sqlite(s) => Some(&s.path),
        _ => None,
    }
}

/// The local file a sink writes, if it writes one.
pub fn sink_local_path(sink: &SinkSpec) -> Option<&str> {
    match sink {
        SinkSpec::Csv(s) | SinkSpec::Jsonl(s) => Some(&s.path),
        SinkSpec::Json(s) => Some(&s.path),
        SinkSpec::Columnar(s) => Some(&s.path),
        SinkSpec::Sqlite(s) => Some(&s.path),
        _ => None,
    }
}

/// Check everything that can be checked without running the pipeline:
/// option values, expression syntax, stage arguments, and (as a warning, or an
/// error when `inputs_must_exist`) that local input files are present.
///
/// Column names are *not* checked against the data -- that needs the source's
/// schema, which only a real read can supply.
pub fn check_spec(spec: &PipelineSpec, inputs_must_exist: bool) -> Diagnostics {
    let mut d = Diagnostics::default();

    if let Some(policy) = &spec.error_policy {
        let ok = policy == "strict" || policy == "skip" || policy.starts_with("quarantine:");
        if !ok {
            let hint = did_you_mean(policy, &["strict", "skip"])
                .map(|s| format!(" -- did you mean `{s}`?"))
                .unwrap_or_default();
            d.errors.push(format!(
                "error_policy {policy:?} is invalid (expected 'strict', 'skip', or \
                 'quarantine:<path>'){hint}"
            ));
        }
    }

    match &spec.source {
        SourceSpec::Csv(s) | SourceSpec::Jsonl(s) | SourceSpec::Json(s) => {
            if s.chunk_rows == Some(0) {
                d.errors
                    .push("source: chunk_rows must be at least 1".into());
            }
        }
        SourceSpec::Sqlite(s) if s.chunk_rows == Some(0) => {
            d.errors
                .push("source: chunk_rows must be at least 1".into());
        }
        _ => {}
    }

    let mut stateful = false;
    for (i, stage) in spec.stages.iter().enumerate() {
        let at = format!("stage #{}", i + 1);
        match stage {
            StageSpec::Filter(expr) => {
                if let Err(e) = tpt_stream_core::expr::parse(expr) {
                    d.errors
                        .push(format!("{at} (filter): expression {expr:?}: {e}"));
                }
            }
            StageSpec::Map(map) => {
                if map.is_empty() {
                    d.errors.push(format!("{at} (map): no output columns"));
                }
                for (name, expr) in map {
                    if let Err(e) = tpt_stream_core::expr::parse(expr) {
                        d.errors.push(format!(
                            "{at} (map): column {name:?}: expression {expr:?}: {e}"
                        ));
                    }
                }
            }
            StageSpec::Select(cols) if cols.is_empty() => {
                d.errors.push(format!("{at} (select): no columns"));
            }
            StageSpec::Select(_) => {}
            StageSpec::Aggregate(agg) => {
                stateful = true;
                if agg.aggs.is_empty() && agg.group_by.is_empty() {
                    d.errors
                        .push(format!("{at} (aggregate): needs group_by and/or aggs"));
                }
                for (column, function) in &agg.aggs {
                    let column = if column == "*" { "" } else { column.as_str() };
                    if let Err(e) = crate::parse_agg(column, function) {
                        let hint = did_you_mean(
                            function,
                            &["sum", "avg", "count", "count_all", "min", "max"],
                        )
                        .map(|s| format!(" -- did you mean `{s}`?"))
                        .unwrap_or_default();
                        d.errors.push(format!("{at} (aggregate): {e}{hint}"));
                    }
                }
            }
            StageSpec::Sort(sort) => {
                stateful = true;
                if sort.columns.is_empty() {
                    d.errors.push(format!("{at} (sort): no columns"));
                }
            }
            StageSpec::Dedup(cols) => {
                stateful = true;
                if cols.is_empty() {
                    d.warnings
                        .push(format!("{at} (dedup): no columns -- compares whole rows"));
                }
            }
            StageSpec::Join(join) => {
                stateful = true;
                if let Err(e) = crate::parse_join_type(&join.join_type) {
                    let hint = did_you_mean(&join.join_type, &["inner", "left", "right"])
                        .map(|s| format!(" -- did you mean `{s}`?"))
                        .unwrap_or_default();
                    d.errors.push(format!("{at} (join): {e}{hint}"));
                }
                if join.left_keys.is_empty() || join.left_keys.len() != join.right_keys.len() {
                    d.errors.push(format!(
                        "{at} (join): left_keys and right_keys must be non-empty and the same \
                         length ({} vs {})",
                        join.left_keys.len(),
                        join.right_keys.len()
                    ));
                }
                if !is_remote(&join.right) && !std::path::Path::new(&join.right).is_file() {
                    let msg = format!("{at} (join): right file {:?} does not exist", join.right);
                    if inputs_must_exist {
                        d.errors.push(msg);
                    } else {
                        d.warnings.push(msg);
                    }
                }
            }
            StageSpec::Expect(e) => {
                stateful = true;
                if e.rows_at_least.is_none()
                    && e.rows_at_most.is_none()
                    && e.no_nulls.is_empty()
                    && e.unique.is_empty()
                    && e.ranges.is_empty()
                    && e.one_of.is_empty()
                    && e.types.is_empty()
                {
                    d.errors.push(format!("{at} (expect): stage has no checks"));
                }
                if let (Some(lo), Some(hi)) = (e.rows_at_least, e.rows_at_most) {
                    if lo > hi {
                        d.errors.push(format!(
                            "{at} (expect): rows_at_least ({lo}) exceeds rows_at_most ({hi})"
                        ));
                    }
                }
            }
            StageSpec::Sample(sm) => {
                if !(0.0..=1.0).contains(&sm.fraction) {
                    d.errors.push(format!(
                        "{at} (sample): fraction {} is outside [0, 1]",
                        sm.fraction
                    ));
                }
                if sm.key.is_empty() {
                    d.errors
                        .push(format!("{at} (sample): key must not be empty"));
                }
            }
            StageSpec::Limit(n) => {
                if usize::try_from(*n).is_err() {
                    d.errors
                        .push(format!("{at} (limit): {n} does not fit in usize"));
                }
            }
        }
    }
    if stateful && spec.dead_letter.is_some() {
        d.errors.push(
            "dead_letter cannot be combined with stateful stages (aggregate/sort/dedup/join/expect)"
                .into(),
        );
    }

    if let Some(path) = source_local_path(&spec.source) {
        if !std::path::Path::new(path).is_file() {
            let msg = format!("source file {path:?} does not exist");
            if inputs_must_exist {
                d.errors.push(msg);
            } else {
                d.warnings.push(msg);
            }
        }
    }
    if let Some(sink) = &spec.sink {
        if let Some(path) = sink_local_path(sink) {
            let parent = std::path::Path::new(path).parent();
            if let Some(parent) = parent.filter(|p| !p.as_os_str().is_empty()) {
                if !parent.is_dir() {
                    d.warnings.push(format!(
                        "sink directory {:?} does not exist",
                        parent.display().to_string()
                    ));
                }
            }
        }
    } else {
        d.warnings
            .push("no sink: results are computed and discarded".into());
    }
    d
}

// ---------------------------------------------------------------------------
// explain
// ---------------------------------------------------------------------------

fn list(items: &[String]) -> String {
    items.join(", ")
}

/// One-line description of a source (credentials are never shown).
pub fn describe_source(source: &SourceSpec) -> String {
    match source {
        SourceSpec::Csv(s) => format!("read CSV {}", s.path),
        SourceSpec::Jsonl(s) => format!("read JSONL {}", s.path),
        SourceSpec::Json(s) => format!("read JSON array {}", s.path),
        SourceSpec::Columnar(s) => format!("read .tptcol {}", s.path),
        SourceSpec::Http(s) => format!(
            "read HTTP {}",
            tpt_stream_core::httpclient::redact_url(&s.path)
        ),
        SourceSpec::Sqlite(s) => format!("read SQLite {} ({})", s.path, s.query),
        SourceSpec::Postgres(s) => format!("read PostgreSQL ({})", s.query),
        SourceSpec::S3(s) => format!("read S3 {} / {}", s.bucket_url, s.key),
        SourceSpec::Gcs(s) => format!("read GCS gs://{}/{}", s.bucket, s.key),
        SourceSpec::Azure(s) => {
            format!("read Azure {} / {} / {}", s.account_url, s.container, s.key)
        }
    }
}

/// One-line description of a stage.
pub fn describe_stage(stage: &StageSpec) -> String {
    match stage {
        StageSpec::Filter(e) => format!("filter: keep rows where {e}"),
        StageSpec::Map(m) => {
            let cols: BTreeMap<_, _> = m.iter().collect();
            let parts: Vec<String> = cols.iter().map(|(k, v)| format!("{k} = {v}")).collect();
            format!("map: replace schema with {}", parts.join(", "))
        }
        StageSpec::Select(c) => format!("select: {}", list(c)),
        StageSpec::Aggregate(a) => {
            let aggs: Vec<String> = a.aggs.iter().map(|(k, v)| format!("{v}({k})")).collect();
            format!(
                "aggregate: group by [{}] compute {} (buffers all groups)",
                list(&a.group_by),
                aggs.join(", ")
            )
        }
        StageSpec::Sort(s) => format!(
            "sort: by [{}] {} (external merge sort, spills to disk)",
            list(&s.columns),
            if s.descending {
                "descending"
            } else {
                "ascending"
            }
        ),
        StageSpec::Dedup(c) if c.is_empty() => "dedup: whole rows".to_string(),
        StageSpec::Dedup(c) => format!("dedup: keep first row per [{}]", list(c)),
        StageSpec::Join(j) => format!(
            "join: {} join with {} on [{}] = [{}] (right side loaded into memory)",
            j.join_type,
            j.right,
            list(&j.left_keys),
            list(&j.right_keys)
        ),
        StageSpec::Expect(e) => {
            let mut checks = Vec::new();
            if let Some(n) = e.rows_at_least {
                checks.push(format!("rows >= {n}"));
            }
            if let Some(n) = e.rows_at_most {
                checks.push(format!("rows <= {n}"));
            }
            if !e.no_nulls.is_empty() {
                checks.push(format!("no nulls in [{}]", list(&e.no_nulls)));
            }
            if !e.unique.is_empty() {
                checks.push(format!("unique [{}]", list(&e.unique)));
            }
            for (c, r) in &e.ranges {
                checks.push(format!("{c} in [{:?}, {:?}]", r.min, r.max));
            }
            for (c, v) in &e.one_of {
                checks.push(format!("{c} is one of {} values", v.len()));
            }
            for (c, t) in &e.types {
                checks.push(format!("{c} is {t}"));
            }
            format!("expect: fail the run unless {}", checks.join(" and "))
        }
        StageSpec::Limit(n) => format!("limit: first {n} rows"),
        StageSpec::Sample(sm) => format!(
            "sample: keep ~{}% of keys [{}] (seed {})",
            sm.fraction * 100.0,
            list(&sm.key),
            sm.seed
        ),
    }
}

/// One-line description of a sink.
pub fn describe_sink(sink: &SinkSpec) -> String {
    match sink {
        SinkSpec::Csv(s) => format!("write CSV {}", s.path),
        SinkSpec::Jsonl(s) => format!("write JSONL {}", s.path),
        SinkSpec::Json(s) => format!(
            "write JSON {}{}",
            s.path,
            if s.pretty { " (pretty)" } else { "" }
        ),
        SinkSpec::Columnar(s) => format!(
            "write .tptcol {}{}",
            s.path,
            if s.use_zstd { " (zstd)" } else { "" }
        ),
        SinkSpec::Sqlite(s) => format!("write SQLite {} table {}", s.path, s.table),
        SinkSpec::Postgres(s) => format!("write PostgreSQL table {}", s.table),
        SinkSpec::S3(s) => format!("write S3 {} / {}", s.bucket_url, s.key),
        SinkSpec::Gcs(s) => format!("write GCS gs://{}/{}", s.bucket, s.key),
        SinkSpec::Azure(s) => format!(
            "write Azure {} / {} / {}",
            s.account_url, s.container, s.key
        ),
    }
}

/// The numbered plan `tptforge explain` prints.
pub fn explain_spec(spec: &PipelineSpec) -> String {
    let mut out = String::new();
    let _ = writeln!(out, "source  {}", describe_source(&spec.source));
    for (i, stage) in spec.stages.iter().enumerate() {
        let _ = writeln!(out, "stage {} {}", i + 1, describe_stage(stage));
    }
    match &spec.sink {
        Some(sink) => {
            let _ = writeln!(out, "sink    {}", describe_sink(sink));
        }
        None => {
            let _ = writeln!(out, "sink    (none)");
        }
    }
    if let Some(policy) = &spec.error_policy {
        let _ = writeln!(out, "error_policy: {policy}");
    }
    if let Some(path) = &spec.dead_letter {
        let _ = writeln!(out, "dead_letter: {path}");
    }
    out
}
