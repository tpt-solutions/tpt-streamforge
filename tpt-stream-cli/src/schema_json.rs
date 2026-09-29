//! Hand-written JSON Schema (draft-07) for the pipeline TOML, for editor
//! autocomplete and validation (Taplo / "Even Better TOML", VS Code, ...).
//!
//! There is no `schemars`: the schema is spelled out here next to the spec
//! structs it describes. Two tests keep it honest -- one compares the output
//! with the checked-in `pipeline.schema.json` (regenerate with
//! `TPT_UPDATE_SCHEMA=1 cargo test -p tpt-stream-cli schema`), the other
//! checks each object's property list against the field list the real serde
//! structs report.

use serde_json::{json, Map, Value};

fn string() -> Value {
    json!({ "type": "string" })
}

fn string_array(min: usize) -> Value {
    json!({ "type": "array", "items": { "type": "string" }, "minItems": min })
}

fn chunk_rows() -> Value {
    json!({
        "type": "integer",
        "minimum": 1,
        "description": "Rows per batch (default 65536)."
    })
}

fn described(mut schema: Value, text: &str) -> Value {
    schema["description"] = json!(text);
    schema
}

/// `{ "type": "object", "properties": .., "required": .., "additionalProperties": false }`
fn object(props: Vec<(&str, Value)>, required: &[&str]) -> Value {
    let mut properties = Map::new();
    for (name, schema) in props {
        properties.insert(name.to_string(), schema);
    }
    let mut out = json!({
        "type": "object",
        "properties": Value::Object(properties),
        "additionalProperties": false,
    });
    if !required.is_empty() {
        out["required"] = json!(required);
    }
    out
}

/// A table with exactly one key chosen from `variants` (the `keyed_enum`
/// shape used by sources, stages, and sinks).
fn one_of_keys(variants: Vec<(&str, Value)>, what: &str) -> Value {
    let mut properties = Map::new();
    for (name, schema) in variants {
        properties.insert(name.to_string(), schema);
    }
    json!({
        "type": "object",
        "description": format!("Exactly one {what} kind."),
        "properties": Value::Object(properties),
        "minProperties": 1,
        "maxProperties": 1,
        "additionalProperties": false,
    })
}

fn r(name: &str) -> Value {
    json!({ "$ref": format!("#/definitions/{name}") })
}

/// Object schemas for the spec structs, by definition name.
pub fn definitions() -> Vec<(&'static str, Value)> {
    let path_or_table = json!({
        "description": "A path string, or a table with `path` (and optional `chunk_rows`).",
        "oneOf": [
            { "type": "string" },
            object(vec![("path", string()), ("chunk_rows", chunk_rows())], &["path"]),
        ]
    });
    vec![
        ("pathOrTable", path_or_table),
        (
            "fileSource",
            object(
                vec![("path", string()), ("chunk_rows", chunk_rows())],
                &["path"],
            ),
        ),
        (
            "sqliteSource",
            object(
                vec![
                    ("path", string()),
                    ("query", string()),
                    ("chunk_rows", chunk_rows()),
                ],
                &["path", "query"],
            ),
        ),
        (
            "connectionQuery",
            object(
                vec![("connection", string()), ("query", string())],
                &["connection", "query"],
            ),
        ),
        (
            "s3Object",
            object(
                vec![("bucket_url", string()), ("key", string())],
                &["bucket_url", "key"],
            ),
        ),
        (
            "gcsObject",
            object(
                vec![("bucket", string()), ("key", string())],
                &["bucket", "key"],
            ),
        ),
        (
            "azureObject",
            object(
                vec![
                    ("account_url", string()),
                    ("container", string()),
                    ("key", string()),
                ],
                &["account_url", "container", "key"],
            ),
        ),
        (
            "aggregate",
            object(
                vec![
                    ("group_by", string_array(0)),
                    (
                        "aggs",
                        json!({
                            "type": "object",
                            "description": "{column: fn}; use the key \"*\" with count_all for a row count.",
                            "additionalProperties": {
                                "enum": ["sum", "avg", "count", "count_all", "min", "max"]
                            }
                        }),
                    ),
                ],
                &["group_by"],
            ),
        ),
        (
            "sort",
            object(
                vec![
                    ("columns", string_array(1)),
                    ("descending", json!({ "type": "boolean", "default": false })),
                ],
                &["columns"],
            ),
        ),
        (
            "join",
            object(
                vec![
                    ("right", described(string(), "Right-hand CSV file.")),
                    ("left_keys", string_array(1)),
                    ("right_keys", string_array(1)),
                    ("type", json!({ "enum": ["inner", "left", "right"] })),
                ],
                &["right", "left_keys", "right_keys", "type"],
            ),
        ),
        (
            "expect",
            object(
                vec![
                    ("rows_at_least", json!({ "type": "integer", "minimum": 0 })),
                    ("rows_at_most", json!({ "type": "integer", "minimum": 0 })),
                    ("no_nulls", string_array(0)),
                    ("unique", string_array(0)),
                    (
                        "ranges",
                        json!({
                            "type": "object",
                            "description": "{column: {min, max}} inclusive numeric bounds.",
                            "additionalProperties": object(
                                vec![
                                    ("min", json!({ "type": "number" })),
                                    ("max", json!({ "type": "number" })),
                                ],
                                &[],
                            )
                        }),
                    ),
                    (
                        "one_of",
                        json!({
                            "type": "object",
                            "description": "{column: [allowed values]}.",
                            "additionalProperties": {
                                "type": "array",
                                "items": { "type": ["string", "number", "boolean"] }
                            }
                        }),
                    ),
                    (
                        "types",
                        json!({
                            "type": "object",
                            "description": "{column: type name}.",
                            "additionalProperties": {
                                "enum": ["int32", "int64", "float32", "float64",
                                         "utf8", "bool", "date", "timestamp"]
                            }
                        }),
                    ),
                ],
                &[],
            ),
        ),
        (
            "sample",
            object(
                vec![
                    (
                        "fraction",
                        json!({ "type": "number", "minimum": 0, "maximum": 1 }),
                    ),
                    ("key", string_array(1)),
                    (
                        "seed",
                        json!({ "type": "integer", "minimum": 0, "default": 0 }),
                    ),
                ],
                &["fraction", "key"],
            ),
        ),
        (
            "jsonSink",
            object(
                vec![
                    ("path", string()),
                    ("pretty", json!({ "type": "boolean", "default": false })),
                ],
                &["path"],
            ),
        ),
        (
            "columnarSink",
            object(
                vec![
                    ("path", string()),
                    ("use_zstd", json!({ "type": "boolean", "default": false })),
                ],
                &["path"],
            ),
        ),
        (
            "sqliteSink",
            object(
                vec![("path", string()), ("table", string())],
                &["path", "table"],
            ),
        ),
        (
            "connectionTable",
            object(
                vec![("connection", string()), ("table", string())],
                &["connection", "table"],
            ),
        ),
    ]
}

/// The complete schema document.
pub fn pipeline_schema() -> Value {
    let mut defs = Map::new();
    for (name, schema) in definitions() {
        defs.insert(name.to_string(), schema);
    }

    let source = one_of_keys(
        vec![
            ("csv", r("fileSource")),
            ("jsonl", r("fileSource")),
            ("json", r("fileSource")),
            ("columnar", r("pathOrTable")),
            ("http", r("pathOrTable")),
            ("sqlite", r("sqliteSource")),
            ("postgres", r("connectionQuery")),
            ("s3", r("s3Object")),
            ("gcs", r("gcsObject")),
            ("azure", r("azureObject")),
        ],
        "source",
    );
    let stage = one_of_keys(
        vec![
            (
                "filter",
                described(string(), "Row expression; keep rows where true."),
            ),
            (
                "map",
                json!({
                    "type": "object",
                    "description": "{output_column: expression}; replaces the schema.",
                    "additionalProperties": { "type": "string" }
                }),
            ),
            ("select", string_array(1)),
            ("aggregate", r("aggregate")),
            ("sort", r("sort")),
            ("dedup", string_array(0)),
            ("join", r("join")),
            ("expect", r("expect")),
            ("limit", json!({ "type": "integer", "minimum": 0 })),
            ("sample", r("sample")),
        ],
        "stage",
    );
    let sink = one_of_keys(
        vec![
            ("csv", r("pathOrTable")),
            ("jsonl", r("pathOrTable")),
            ("json", r("jsonSink")),
            ("columnar", r("columnarSink")),
            ("sqlite", r("sqliteSink")),
            ("postgres", r("connectionTable")),
            ("s3", r("s3Object")),
            ("gcs", r("gcsObject")),
            ("azure", r("azureObject")),
        ],
        "sink",
    );
    defs.insert("source".into(), source);
    defs.insert("stage".into(), stage);
    defs.insert("sink".into(), sink);

    json!({
        "$schema": "http://json-schema.org/draft-07/schema#",
        "title": "tptforge pipeline",
        "description": "A tptforge pipeline file. String values may use ${VAR} / ${VAR:-default}; write $${ for a literal ${.",
        "type": "object",
        "properties": {
            "source": r("source"),
            "stages": { "type": "array", "items": r("stage") },
            "error_policy": {
                "type": "string",
                "description": "strict | skip | quarantine:<path>",
                "anyOf": [
                    { "enum": ["strict", "skip"] },
                    { "pattern": "^quarantine:.+" }
                ]
            },
            "dead_letter": {
                "type": "string",
                "description": "CSV file receiving rows a stage rejects."
            },
            "sink": r("sink"),
        },
        "required": ["source"],
        "additionalProperties": false,
        "definitions": Value::Object(defs),
    })
}

/// The schema as pretty-printed JSON with a trailing newline -- exactly the
/// bytes of the checked-in `pipeline.schema.json`.
pub fn pipeline_schema_string() -> String {
    let mut text = serde_json::to_string_pretty(&pipeline_schema()).expect("schema serializes");
    text.push('\n');
    text
}
