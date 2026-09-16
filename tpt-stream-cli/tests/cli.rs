//! End-to-end tests for the tptforge CLI library: YAML pipeline execution,
//! spec parsing, schema, and preview.

use tpt_stream_cli::{parse_pipeline_yaml, preview_command, run_command, schema_command};

fn write(path: &std::path::Path, content: &str) {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).unwrap();
    }
    std::fs::write(path, content).unwrap();
}

fn sample_csv() -> String {
    let mut csv = String::from("id,region,amount\n");
    for i in 0..100 {
        csv.push_str(&format!("{},region{},{}\n", i, i % 4, (i * 13) % 50));
    }
    csv
}

const PIPELINE_YAML: &str = "\
source:
  csv: { path: IN_CSV }
error_policy: strict
stages:
  - filter: \"amount > 0\"
  - aggregate: { group_by: [region], aggs: { amount: sum, \"*\": count_all } }
  - sort: { columns: [region], descending: false }
sink:
  csv: OUT_CSV
";

#[tokio::test]
async fn yaml_pipeline_csv_to_csv() {
    let dir = tempfile::tempdir().unwrap();
    let in_csv = dir.path().join("in.csv");
    let out_csv = dir.path().join("out.csv");
    write(&in_csv, &sample_csv());

    let yaml = PIPELINE_YAML
        .replace("IN_CSV", in_csv.to_string_lossy().as_ref())
        .replace("OUT_CSV", out_csv.to_string_lossy().as_ref());
    let pipeline_file = dir.path().join("pipeline.yaml");
    write(&pipeline_file, &yaml);

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
async fn yaml_pipeline_jsonl_source() {
    let dir = tempfile::tempdir().unwrap();
    let in_jsonl = dir.path().join("in.jsonl");
    let out_jsonl = dir.path().join("out.jsonl");
    write(&in_jsonl, "{\"a\":1}\n{\"a\":5}\n{\"a\":9}\n");

    let yaml = format!(
        "source:\n  jsonl: {{ path: {} }}\nstages:\n  - filter: \"a >= 5\"\nsink:\n  jsonl: {{ path: {} }}\n",
        in_jsonl.to_string_lossy(),
        out_jsonl.to_string_lossy()
    );
    let pipeline_file = dir.path().join("pipeline.yaml");
    write(&pipeline_file, &yaml);

    run_command(&pipeline_file, true).await.unwrap();
    let out = std::fs::read_to_string(&out_jsonl).unwrap();
    let mut lines: Vec<&str> = out.lines().collect();
    lines.sort();
    assert_eq!(lines, vec!["{\"a\":5}", "{\"a\":9}"]);
}

#[test]
fn yaml_pipeline_expect_check_fails() {
    let dir = tempfile::tempdir().unwrap();
    let in_csv = dir.path().join("in.csv");
    let out_csv = dir.path().join("out.csv");
    write(&in_csv, "v\n1\n2\n");

    let yaml = format!(
        "source:\n  csv: {{ path: {} }}\nstages:\n  - expect: {{ rows_at_least: 10 }}\nsink:\n  csv: {{ path: {} }}\n",
        in_csv.to_string_lossy(),
        out_csv.to_string_lossy()
    );
    let spec = parse_pipeline_yaml(&yaml).unwrap();
    let mut pipeline = tpt_stream_cli::build_pipeline(&spec).unwrap();
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let err = rt.block_on(pipeline.execute()).unwrap_err();
    assert!(err.to_string().contains("data quality"), "{err}");
}

#[test]
fn yaml_join_and_select_stages() {
    let dir = tempfile::tempdir().unwrap();
    let left = dir.path().join("left.csv");
    let right = dir.path().join("right.csv");
    let out_csv = dir.path().join("out.csv");
    write(&left, "id,v\n1,10\n2,20\n");
    write(&right, "id,label\n1,one\n2,two\n");

    let yaml = format!(
        "source:\n  csv: {{ path: {} }}\nstages:\n  - join: {{ right: {}, left_keys: [id], right_keys: [id], type: left }}\n  - select: [id, v, label]\nsink:\n  csv: {{ path: {} }}\n",
        left.to_string_lossy(),
        right.to_string_lossy(),
        out_csv.to_string_lossy()
    );
    let spec = parse_pipeline_yaml(&yaml).unwrap();
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
fn invalid_yaml_and_unknown_fields_are_rejected() {
    let err = parse_pipeline_yaml("source: { csv: {} }").unwrap_err();
    assert!(format!("{err:#}").contains("path"), "{err:#}");

    assert!(parse_pipeline_yaml("not: [valid").is_err());
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
