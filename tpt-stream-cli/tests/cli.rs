//! End-to-end tests for the tptforge CLI library: TOML pipeline execution,
//! spec parsing, schema, and preview.

use tpt_stream_cli::{parse_pipeline_toml, preview_command, run_command, schema_command};

fn write(path: &std::path::Path, content: &str) {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).unwrap();
    }
    std::fs::write(path, content).unwrap();
}

/// Paths go into TOML basic strings, so use forward slashes: a Windows
/// backslash would otherwise be read as an escape sequence and make the
/// document invalid.
fn toml_path(path: &std::path::Path) -> String {
    path.to_string_lossy().replace('\\', "/")
}

fn sample_csv() -> String {
    let mut csv = String::from("id,region,amount\n");
    for i in 0..100 {
        csv.push_str(&format!("{},region{},{}\n", i, i % 4, (i * 13) % 50));
    }
    csv
}

#[tokio::test]
async fn toml_pipeline_csv_to_csv() {
    let dir = tempfile::tempdir().unwrap();
    let in_csv = dir.path().join("in.csv");
    let out_csv = dir.path().join("out.csv");
    write(&in_csv, &sample_csv());

    let toml = format!(
        r#"
error_policy = "strict"

[source.csv]
path = "{in_csv}"

[[stages]]
filter = "amount > 0"

[[stages]]
[stages.aggregate]
group_by = ["region"]

[stages.aggregate.aggs]
amount = "sum"
"*" = "count_all"

[[stages]]
[stages.sort]
columns = ["region"]
descending = false

[sink]
csv = "{out_csv}"
"#,
        in_csv = toml_path(&in_csv),
        out_csv = toml_path(&out_csv)
    );
    let pipeline_file = dir.path().join("pipeline.toml");
    write(&pipeline_file, &toml);

    let summary = run_command(&pipeline_file, true).await.unwrap();
    assert!(summary.contains("100 rows"), "{summary}");

    let out = std::fs::read_to_string(&out_csv).unwrap();
    let lines: Vec<&str> = out.lines().collect();
    assert_eq!(lines[0], "region,count_all,sum_amount");
    assert_eq!(lines.len(), 5); // header + 4 regions
                                // Every surviving row passed the filter, so sums are positive and the
                                // per-region counts are the filtered row totals.
    let mut total_counted = 0u64;
    for line in &lines[1..] {
        let fields: Vec<&str> = line.split(',').collect();
        assert_eq!(fields.len(), 3, "{line}");
        assert!(fields[2].parse::<f64>().unwrap() > 0.0, "{line}");
        total_counted += fields[1].parse::<u64>().unwrap();
    }
    assert_eq!(total_counted, 98); // (i*13) % 50 == 0 only for i in {0, 50}
}

#[tokio::test]
async fn toml_pipeline_jsonl_source() {
    let dir = tempfile::tempdir().unwrap();
    let in_jsonl = dir.path().join("in.jsonl");
    let out_jsonl = dir.path().join("out.jsonl");
    write(&in_jsonl, "{\"a\":1}\n{\"a\":5}\n{\"a\":9}\n");

    let toml = format!(
        "[source.jsonl]\npath = \"{}\"\n\n[[stages]]\nfilter = \"a >= 5\"\n\n[sink.jsonl]\npath = \"{}\"\n",
        toml_path(&in_jsonl),
        toml_path(&out_jsonl)
    );
    let pipeline_file = dir.path().join("pipeline.toml");
    write(&pipeline_file, &toml);

    run_command(&pipeline_file, true).await.unwrap();
    let out = std::fs::read_to_string(&out_jsonl).unwrap();
    let mut lines: Vec<&str> = out.lines().collect();
    lines.sort();
    assert_eq!(lines, vec!["{\"a\":5}", "{\"a\":9}"]);
}

#[test]
fn toml_pipeline_expect_check_fails() {
    let dir = tempfile::tempdir().unwrap();
    let in_csv = dir.path().join("in.csv");
    let out_csv = dir.path().join("out.csv");
    write(&in_csv, "v\n1\n2\n");

    let toml = format!(
        "[source.csv]\npath = \"{}\"\n\n[[stages]]\nexpect = {{ rows_at_least = 10 }}\n\n[sink.csv]\npath = \"{}\"\n",
        toml_path(&in_csv),
        toml_path(&out_csv)
    );
    let spec = parse_pipeline_toml(&toml).unwrap();
    let mut pipeline = tpt_stream_cli::build_pipeline(&spec).unwrap();
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let err = rt.block_on(pipeline.execute()).unwrap_err();
    assert!(err.to_string().contains("data quality"), "{err}");
}

#[test]
fn toml_join_and_select_stages() {
    let dir = tempfile::tempdir().unwrap();
    let left = dir.path().join("left.csv");
    let right = dir.path().join("right.csv");
    let out_csv = dir.path().join("out.csv");
    write(&left, "id,v\n1,10\n2,20\n");
    write(&right, "id,label\n1,one\n2,two\n");

    let toml = format!(
        "[source.csv]\npath = \"{}\"\n\n[[stages]]\njoin = {{ right = \"{}\", left_keys = [\"id\"], right_keys = [\"id\"], type = \"left\" }}\n\n[[stages]]\nselect = [\"id\", \"v\", \"label\"]\n\n[sink.csv]\npath = \"{}\"\n",
        toml_path(&left),
        toml_path(&right),
        toml_path(&out_csv)
    );
    let spec = parse_pipeline_toml(&toml).unwrap();
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let mut pipeline = tpt_stream_cli::build_pipeline(&spec).unwrap();
    let stats = rt.block_on(pipeline.execute()).unwrap();
    assert_eq!(stats.rows, 2);

    let out = std::fs::read_to_string(&out_csv).unwrap();
    assert_eq!(out, "id,v,label\n1,10,one\n2,20,two\n");
}

#[test]
fn invalid_toml_and_unknown_sources_are_rejected() {
    // A source table without `path` must name the missing field.
    let err = parse_pipeline_toml("[source.csv]\nrows = 3\n").unwrap_err();
    assert!(format!("{err:#}").contains("invalid source"), "{err:#}");

    // Structurally invalid TOML.
    assert!(parse_pipeline_toml("not = [valid").is_err());

    // Unknown source kind lists the supported keys.
    let err = parse_pipeline_toml("[source.parquet]\npath = \"x\"\n").unwrap_err();
    assert!(format!("{err:#}").contains("unknown source"), "{err:#}");
}

#[tokio::test]
async fn retired_yaml_pipelines_are_rejected_with_a_hint() {
    let dir = tempfile::tempdir().unwrap();
    let pipeline_file = dir.path().join("pipeline.yaml");
    write(&pipeline_file, "source:\n  csv: { path: in.csv }\n");

    let err = run_command(&pipeline_file, true).await.unwrap_err();
    let message = format!("{err:#}");
    assert!(message.contains("TOML"), "{message}");
}

/// The shipped starter template must always parse against the current schema.
#[test]
fn starter_template_parses() {
    let text =
        std::fs::read_to_string(starter_template()).expect("starter template must be readable");
    let spec = parse_pipeline_toml(&text).expect("starter template must parse");
    assert!(!spec.stages.is_empty());
    assert!(spec.sink.is_some());
}

fn starter_template() -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("templates")
        .join("pipeline-starter")
        .join("pipeline.toml")
}

/// Parsing alone is not enough: the template must also *run*. This catches
/// stage-ordering mistakes such as a `map` that drops a column a later stage
/// (`expect`/`aggregate`) still needs.
#[tokio::test]
async fn starter_template_runs_end_to_end() {
    let dir = tempfile::tempdir().unwrap();
    let in_csv = dir.path().join("input.csv");
    let out_csv = dir.path().join("output.csv");

    let text = std::fs::read_to_string(starter_template()).unwrap();
    let toml = text
        .replace("\"input.csv\"", &format!("\"{}\"", toml_path(&in_csv)))
        .replace("\"output.csv\"", &format!("\"{}\"", toml_path(&out_csv)));
    assert!(
        !toml.contains("\"input.csv\""),
        "template input path not substituted"
    );

    write(&in_csv, "region,amount\nemea,10\nemea,5\namer,7\n");
    let pipeline_file = dir.path().join("pipeline.toml");
    write(&pipeline_file, &toml);

    let summary = run_command(&pipeline_file, true).await.unwrap();
    assert!(summary.contains("3 rows"), "{summary}");

    let out = std::fs::read_to_string(&out_csv).unwrap();
    let lines: Vec<&str> = out.lines().collect();
    assert_eq!(lines[0], "region,count_all,sum_total");
    assert_eq!(lines.len(), 3, "one row per region: {out}");
    // emea (2 rows, total 15*1.2) sorts before amer (1 row, 7*1.2).
    assert!(lines[1].starts_with("emea,"), "{out}");
    assert!(lines[2].starts_with("amer,"), "{out}");
}

#[tokio::test]
async fn schema_and_preview_commands() {
    let dir = tempfile::tempdir().unwrap();
    let in_csv = dir.path().join("in.csv");
    write(&in_csv, "id,score,name\n1,1.5,alice\n2,2.5,bob\n");

    let schema = schema_command(in_csv.to_string_lossy().as_ref(), 100)
        .await
        .unwrap();
    assert_eq!(schema, "id\tint32\nscore\tfloat64\nname\tstring\n");

    let preview = preview_command(in_csv.to_string_lossy().as_ref(), 1)
        .await
        .unwrap();
    assert_eq!(preview, "id,score,name\n1,1.5,alice\n");
}

#[tokio::test]
async fn run_reports_missing_files_clearly() {
    let err = run_command(std::path::Path::new("no/such/pipeline.yaml"), true)
        .await
        .unwrap_err();
    assert!(err.to_string().contains("reading pipeline file"), "{err}");
}
