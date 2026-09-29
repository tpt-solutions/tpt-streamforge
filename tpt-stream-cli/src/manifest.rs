//! `--manifest`: a provenance record of one run -- what went in (hashes), what
//! the spec was (hash), what happened (counts, per-stage stats), what came out.
//!
//! The spec is hashed as the raw file bytes, i.e. *before* `${VAR}`
//! substitution, so the manifest never contains (or depends on) a secret.

use std::io::Read;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use tpt_stream_core::StageMetrics;

use crate::config::{sink_local_path, source_local_path};
use crate::{PipelineSpec, SinkSpec, SourceSpec, StageSpec};

/// Lower-case hex SHA-256 of `bytes`.
pub fn sha256_hex(bytes: &[u8]) -> String {
    hex(&Sha256::digest(bytes))
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// Lower-case hex SHA-256 of a file, streamed in 64 KiB blocks.
pub fn sha256_file(path: &Path) -> Result<String> {
    let mut file =
        std::fs::File::open(path).with_context(|| format!("hashing {}", path.display()))?;
    let mut hasher = Sha256::new();
    let mut buf = vec![0u8; 64 * 1024];
    loop {
        let n = file.read(&mut buf)?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    Ok(hex(&hasher.finalize()))
}

/// RFC 3339 UTC timestamp (second precision) for a Unix time.
pub fn rfc3339_utc(unix_secs: u64) -> String {
    let days = (unix_secs / 86_400) as i32;
    let rem = unix_secs % 86_400;
    let (y, m, d) = tpt_stream_core::value::civil_from_days(days);
    format!(
        "{y:04}-{m:02}-{d:02}T{:02}:{:02}:{:02}Z",
        rem / 3600,
        rem % 3600 / 60,
        rem % 60
    )
}

pub fn now_unix() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_secs())
}

fn source_kind(source: &SourceSpec) -> &'static str {
    match source {
        SourceSpec::Csv(_) => "csv",
        SourceSpec::Jsonl(_) => "jsonl",
        SourceSpec::Json(_) => "json",
        SourceSpec::Columnar(_) => "columnar",
        SourceSpec::Http(_) => "http",
        SourceSpec::Sqlite(_) => "sqlite",
        SourceSpec::Postgres(_) => "postgres",
        SourceSpec::S3(_) => "s3",
        SourceSpec::Gcs(_) => "gcs",
        SourceSpec::Azure(_) => "azure",
    }
}

fn sink_kind(sink: &SinkSpec) -> &'static str {
    match sink {
        SinkSpec::Csv(_) => "csv",
        SinkSpec::Jsonl(_) => "jsonl",
        SinkSpec::Json(_) => "json",
        SinkSpec::Columnar(_) => "columnar",
        SinkSpec::Sqlite(_) => "sqlite",
        SinkSpec::Postgres(_) => "postgres",
        SinkSpec::S3(_) => "s3",
        SinkSpec::Gcs(_) => "gcs",
        SinkSpec::Azure(_) => "azure",
    }
}

fn file_entry(kind: &str, path: &str) -> Value {
    let p = Path::new(path);
    match (std::fs::metadata(p), sha256_file(p)) {
        (Ok(meta), Ok(hash)) => json!({
            "kind": kind, "path": path, "bytes": meta.len(), "sha256": hash
        }),
        _ => json!({ "kind": kind, "path": path, "missing": true }),
    }
}

/// Hash every local input (source file and join right-hand files). Call this
/// *before* the run: a pipeline may overwrite its own input.
pub fn hash_inputs(spec: &PipelineSpec) -> Vec<Value> {
    let mut inputs = Vec::new();
    match source_local_path(&spec.source) {
        Some(path) => inputs.push(file_entry(source_kind(&spec.source), path)),
        None => inputs.push(json!({ "kind": source_kind(&spec.source), "remote": true })),
    }
    for stage in &spec.stages {
        if let StageSpec::Join(j) = stage {
            inputs.push(file_entry("csv", &j.right));
        }
    }
    inputs
}

/// Hash the local output, if the sink writes a local file. Call after the run.
pub fn hash_outputs(spec: &PipelineSpec) -> Vec<Value> {
    match &spec.sink {
        Some(sink) => match sink_local_path(sink) {
            Some(path) => vec![file_entry(sink_kind(sink), path)],
            None => vec![json!({ "kind": sink_kind(sink), "remote": true })],
        },
        None => Vec::new(),
    }
}

/// Everything the manifest records about a finished (or failed) run.
pub struct ManifestInput<'a> {
    pub pipeline_path: &'a Path,
    pub spec_bytes: &'a [u8],
    pub started_unix: u64,
    pub inputs: Vec<Value>,
    pub outputs: Vec<Value>,
    pub stats: Option<&'a tpt_stream_core::PipelineStats>,
    pub stages: &'a [StageMetrics],
    pub dead_letter_rows: u64,
    pub error: Option<String>,
}

pub fn build_manifest(m: &ManifestInput) -> Value {
    let stats = m.stats.map(|s| {
        json!({
            "rows": s.rows,
            "batches": s.batches,
            "bytes_in": s.bytes_in,
            "bytes_out": s.bytes_out,
            "elapsed_ms": s.elapsed.as_millis() as u64,
        })
    });
    let stages: Vec<Value> = m
        .stages
        .iter()
        .enumerate()
        .map(|(i, s)| {
            json!({
                "index": i + 1,
                "name": s.name,
                "rows_in": s.rows_in,
                "rows_out": s.rows_out,
                "batches": s.batches,
                "elapsed_ms": s.elapsed.as_millis() as u64,
            })
        })
        .collect();
    json!({
        "manifest_version": 1,
        "tool": "tptforge",
        "tool_version": env!("CARGO_PKG_VERSION"),
        "status": if m.error.is_some() { "failed" } else { "ok" },
        "error": m.error,
        "started_at": rfc3339_utc(m.started_unix),
        "finished_at": rfc3339_utc(now_unix()),
        "pipeline": m.pipeline_path.display().to_string(),
        "spec_sha256": sha256_hex(m.spec_bytes),
        "inputs": m.inputs,
        "outputs": m.outputs,
        "stats": stats,
        "stages": stages,
        "dead_letter_rows": m.dead_letter_rows,
    })
}

pub fn write_manifest(path: &Path, manifest: &Value) -> Result<()> {
    let mut text = serde_json::to_string_pretty(manifest)?;
    text.push('\n');
    std::fs::write(path, text).with_context(|| format!("writing manifest {}", path.display()))
}
