use std::io::Write;
use std::sync::atomic::{AtomicUsize, Ordering};

use tpt_stream_core::{Column, DataType, Pipeline, PipelineStats, RecordBatch, Value};

static COUNTER: AtomicUsize = AtomicUsize::new(0);

fn temp_path(suffix: &str) -> String {
    let n = COUNTER.fetch_add(1, Ordering::SeqCst);
    let dir = std::env::temp_dir();
    dir.join(format!("tpt-streamforge-test-{n}-{suffix}"))
        .to_string_lossy()
        .into_owned()
}

fn write_text(path: &str, contents: &str) {
    let mut f = std::fs::File::create(path).unwrap();
    f.write_all(contents.as_bytes()).unwrap();
}

fn read_to_string(path: &str) -> String {
    std::fs::read_to_string(path).unwrap()
}

#[tokio::test]
async fn csv_filter_map_roundtrip() {
    let input = temp_path("in.csv");
    let output = temp_path("out.csv");

    write_text(
        &input,
        "id,name,score\n1,alice,90.5\n2,bob,55.0\n3,carol,72.25\n4,dave,101.5\n",
    );

    let mut pipeline = Pipeline::new();
    pipeline
        .with_chunk_size(2)
        .read_csv(&input)
        .filter(|row| {
            let score = match row.get("score") {
                Some(Value::Float64(v)) => v,
                _ => 0.0,
            };
            score > 60.0
        })
        .map(&["id", "name"], |row| {
            let id = match row.get("id") {
                Some(Value::Int32(v)) => Value::Int64(v as i64),
                _ => Value::Null,
            };
            let name = row.get("name").unwrap_or(Value::Null);
            vec![id, name]
        })
        .write_csv(&output);

    let stats = pipeline.execute().await.unwrap();
    assert_eq!(stats.rows, 4);

    let out = read_to_string(&output);
    let expected = "id,name\n1,alice\n3,carol\n4,dave\n";
    assert_eq!(out, expected);
}

#[tokio::test]
async fn jsonl_filter_map_roundtrip() {
    let input = temp_path("in.jsonl");
    let output = temp_path("out.jsonl");

    write_text(
        &input,
        "{\"id\": 1, \"city\": \"paris\"}\n{\"id\": 2, \"city\": \"london\"}\n{\"id\": 3, \"city\": \"paris\"}\n",
    );

    let mut pipeline = Pipeline::new();
    pipeline
        .with_chunk_size(1)
        .read_jsonl(&input)
        .filter(|row| row.get("city") == Some(Value::Utf8("paris".into())))
        .map(&["id"], |row| {
            row.get("id").map_or(vec![Value::Null], |v| vec![v])
        })
        .write_jsonl(&output);

    let stats = pipeline.execute().await.unwrap();
    assert_eq!(stats.rows, 3);

    let out = read_to_string(&output);
    assert!(
        out.contains("\"id\":1") || out.contains("\"id\": 1"),
        "unexpected: {out}"
    );
    assert!(!out.contains("\"id\":2"), "unexpected: {out}");
    assert!(
        out.contains("\"id\":3") || out.contains("\"id\": 3"),
        "unexpected: {out}"
    );
}

#[tokio::test]
async fn json_array_roundtrip() {
    let input = temp_path("in.json");
    let output = temp_path("out.json");

    write_text(
        &input,
        "[{\"a\": 1, \"b\": true}, {\"a\": 2, \"b\": false}]",
    );

    let mut pipeline = Pipeline::new();
    pipeline
        .read_json(&input)
        .map(&["a", "b"], |row| {
            vec![
                row.get("a").unwrap_or(Value::Null),
                row.get("b").unwrap_or(Value::Null),
            ]
        })
        .write_json(&output, true);

    let stats = pipeline.execute().await.unwrap();
    assert_eq!(stats.rows, 2);

    let out = read_to_string(&output);
    assert!(out.starts_with('['), "expected array, got: {out}");
    assert!(out.ends_with(']'), "expected array, got: {out}");
    assert!(
        out.contains("\"a\": 1") || out.contains("\"a\":1"),
        "unexpected: {out}"
    );
}

#[tokio::test]
async fn csv_to_jsonl_filter() {
    let input = temp_path("in.csv");
    let output = temp_path("out.jsonl");

    write_text(&input, "name,active\nalice,true\nbob,false\ncarol,true\n");

    let mut pipeline = Pipeline::new();
    pipeline
        .read_csv(&input)
        .filter(|row| row.get("active") == Some(Value::Bool(true)))
        .write_jsonl(&output);

    pipeline.execute().await.unwrap();

    let out = read_to_string(&output);
    let lines: Vec<&str> = out.lines().collect();
    assert_eq!(lines.len(), 2);
}

#[tokio::test]
async fn empty_source_ok() {
    let input = temp_path("empty.csv");
    write_text(&input, "id,name\n");
    let output = temp_path("empty-out.csv");

    let mut pipeline = Pipeline::new();
    pipeline.read_csv(&input).write_csv(&output);
    let stats = pipeline.execute().await.unwrap();
    assert_eq!(stats.rows, 0);
    assert_eq!(read_to_string(&output), "");
}

#[tokio::test]
async fn missing_file_errors() {
    let mut pipeline = Pipeline::new();
    pipeline.read_csv("definitely-not-a-real-file.csv");
    let err = pipeline.execute().await.err();
    assert!(err.is_some(), "expected an I/O error");
}

fn sample_batch() -> RecordBatch {
    let mut id = Column::new("id", DataType::Int32, 3);
    id.push(Value::Int32(1));
    id.push(Value::Null);
    id.push(Value::Int32(-7));
    let mut name = Column::new("name", DataType::Utf8, 3);
    name.push(Value::Utf8("héllo".into()));
    name.push(Value::Utf8("".into()));
    name.push(Value::Utf8("wörld".into()));
    let mut ok = Column::new("ok", DataType::Bool, 3);
    ok.push(Value::Bool(true));
    ok.push(Value::Bool(false));
    ok.push(Value::Null);
    RecordBatch::new(vec![id, name, ok])
}

#[tokio::test]
async fn columnar_roundtrip_via_pipeline() {
    let tptcol = temp_path("data.tptcol");

    // Encode a handcrafted batch (nulls, utf8, bool) directly to the file...
    {
        let file = std::fs::File::create(&tptcol).unwrap();
        let mut writer = tpt_stream_columnar::format::ChunkedWriter::new(file, false);
        writer.write_batch(&sample_batch()).unwrap();
        writer.finish().unwrap();
    }

    // ...then stream it back through the pipeline.
    let out = temp_path("out.jsonl");
    let mut read = Pipeline::new();
    read.read_columnar(&tptcol).write_jsonl(&out);
    read.execute().await.unwrap();

    let lines = read_to_string(&out);
    let parsed: Vec<serde_json::Value> = lines
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect();
    assert_eq!(parsed.len(), 3);
    assert_eq!(parsed[0]["name"], serde_json::json!("héllo"));
    assert_eq!(parsed[0]["ok"], serde_json::json!(true));
    assert_eq!(parsed[1]["id"], serde_json::json!(null));
    assert_eq!(parsed[2]["ok"], serde_json::json!(null));
}

#[tokio::test]
async fn columnar_csv_roundtrip() {
    let input = temp_path("in.csv");
    let tptcol = temp_path("data.tptcol");
    let output = temp_path("out.csv");

    write_text(
        &input,
        "id,name,score\n1,alice,90.5\n2,bob,55.0\n3,carol,72.25\n4,dave,101.5\n",
    );

    let mut csv_to_col = Pipeline::new();
    csv_to_col.read_csv(&input).write_columnar(&tptcol, false);
    csv_to_col.execute().await.unwrap();

    let mut col_to_csv = Pipeline::new();
    col_to_csv.read_columnar(&tptcol).write_csv(&output);
    let stats = col_to_csv.execute().await.unwrap();
    assert_eq!(stats.rows, 4);

    let out = read_to_string(&output);
    let expected = "id,name,score\n1,alice,90.5\n2,bob,55\n3,carol,72.25\n4,dave,101.5\n";
    assert_eq!(out, expected);
}

#[cfg(feature = "zstd")]
#[tokio::test]
async fn columnar_zstd_roundtrip() {
    // Enough rows that the zstd path actually compresses (>512 bytes per buffer).
    let totptcol = temp_path("zstd.tptcol");
    let from = temp_path("zstd-out.csv");

    let mut csv = String::from("id,name\n");
    for i in 0..2000 {
        csv.push_str(&format!("{i},user number {i}\n"));
    }
    let input = temp_path("in.csv");
    write_text(&input, &csv);

    let mut write = Pipeline::new();
    write.read_csv(&input).write_columnar(&totptcol, true);
    write.execute().await.unwrap();
    let zstd_size = std::path::Path::new(&totptcol).metadata().unwrap().len();
    assert!(zstd_size > 0);

    let mut read = Pipeline::new();
    read.read_columnar(&totptcol).write_csv(&from);
    let stats: PipelineStats = read.execute().await.unwrap();
    assert_eq!(stats.rows, 2000);
    assert_eq!(read_to_string(&from), csv);
}

#[tokio::test]
async fn columnar_streams_multiple_chunks() {
    // 10k rows with a 2k chunk size => multiple chunks; verify all persist.
    let input = temp_path("many.csv");
    let tptcol = temp_path("multi.tptcol");
    let out_jsonl = temp_path("multi.jsonl");

    let mut csv = String::from("id\n");
    for i in 0..10_000 {
        csv.push_str(&format!("{i}\n"));
    }
    write_text(&input, &csv);

    let mut write = Pipeline::new();
    write
        .with_chunk_size(2_000)
        .read_csv(&input)
        .write_columnar(&tptcol, false);
    write.execute().await.unwrap();

    let mut read = Pipeline::new();
    read.read_columnar(&tptcol).write_jsonl(&out_jsonl);
    let stats = read.execute().await.unwrap();
    assert_eq!(stats.rows, 10_000);
    let jsonl = read_to_string(&out_jsonl);
    assert_eq!(jsonl.lines().count(), 10_000);
}
