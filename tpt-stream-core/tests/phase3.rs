use std::io::Write;
use std::sync::atomic::{AtomicUsize, Ordering};

use tpt_stream_core::{AggSpec, Column, DataType, JoinType, Pipeline, RecordBatch, Value};

static COUNTER: AtomicUsize = AtomicUsize::new(0);

fn temp_path(suffix: &str) -> String {
    let n = COUNTER.fetch_add(1, Ordering::SeqCst);
    let dir = std::env::temp_dir();
    dir.join(format!("tpt-streamforge-phase3-{n}-{suffix}"))
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
async fn aggregate_by_group() {
    let input = temp_path("agg.csv");
    let output = temp_path("agg-out.csv");

    write_text(
        &input,
        "dept,score\nsales,10\nsales,20\neng,30\neng,\ndept_x,5\n",
    );

    let mut pipeline = Pipeline::new();
    pipeline
        .read_csv(&input)
        .aggregate(
            &["dept"],
            &[
                AggSpec::sum("score"),
                AggSpec::avg("score"),
                AggSpec::count("score"),
                AggSpec::count_all("count_all"),
                AggSpec::min("score"),
                AggSpec::max("score"),
            ],
        )
        .write_csv(&output);

    let stats = pipeline.execute().await.unwrap();
    assert_eq!(stats.rows, 5); // stats counts source rows

    let out = read_to_string(&output);
    let mut lines: Vec<&str> = out.lines().collect();
    lines.sort();
    assert_eq!(
        lines[0],
        "dept,sum_score,avg_score,count_score,count_all,min_score,max_score"
    );
    // CSVs display Int64 30 and Int32 5; avg_score is Float64.
    assert!(
        lines.contains(&"eng,30,30,1,2,30,30"),
        "unexpected rows: {out}"
    );
    assert!(
        lines.contains(&"sales,30,15,2,2,10,20"),
        "unexpected rows: {out}"
    );
    assert!(
        lines.iter().any(|l| l.starts_with("dept_x,")),
        "unexpected rows: {out}"
    );
}

#[tokio::test]
async fn sort_then_write() {
    let input = temp_path("sort.csv");
    let output = temp_path("sort-out.csv");

    write_text(&input, "id,score\n3,10\n1,20\n2,30\n");

    let mut pipeline = Pipeline::new();
    pipeline
        .read_csv(&input)
        .sort_by(&["score"])
        .write_csv(&output);

    pipeline.execute().await.unwrap();
    let out = read_to_string(&output);
    assert_eq!(out, "id,score\n3,10\n1,20\n2,30\n");
}

#[tokio::test]
async fn sort_descending_then_write() {
    let input = temp_path("sortd.csv");
    let output = temp_path("sortd-out.csv");

    write_text(&input, "id\n3\n1\n2\n");

    let mut pipeline = Pipeline::new();
    pipeline
        .read_csv(&input)
        .sort_by_desc(&["id"])
        .write_csv(&output);

    pipeline.execute().await.unwrap();
    assert_eq!(read_to_string(&output), "id\n3\n2\n1\n");
}

#[tokio::test]
async fn dedup_pipeline() {
    let input = temp_path("dedup.csv");
    let output = temp_path("dedup-out.csv");

    write_text(
        &input,
        "id,val\n1,hello\n2,world\n1,again\n3,extra\n2,dup\n",
    );

    let mut pipeline = Pipeline::new();
    pipeline.read_csv(&input).dedup(&["id"]).write_csv(&output);

    let stats = pipeline.execute().await.unwrap();
    assert_eq!(stats.rows, 5); // source rows
    let out = read_to_string(&output);
    let body: Vec<&str> = out.lines().skip(1).collect();
    // First occurrence of each id survives; later dupes (1, 2) are dropped.
    assert_eq!(body, vec!["1,hello", "2,world", "3,extra"]);
}

fn right_relation_batches() -> Vec<RecordBatch> {
    let mut id = Column::new("id", DataType::Int32, 3);
    for v in [1, 2, 3] {
        id.push(Value::Int32(v));
    }
    let mut name = Column::new("name", DataType::Utf8, 3);
    for v in ["alice", "bob", "carol"] {
        name.push(Value::Utf8(v.into()));
    }
    vec![RecordBatch::new(vec![id, name])]
}

#[tokio::test]
async fn join_pipeline_inner() {
    let input = temp_path("left.csv");
    let output = temp_path("joined.csv");

    write_text(&input, "id,score\n1,10\n1,20\n3,30\n99,40\n");

    let mut pipeline = Pipeline::new();
    pipeline
        .read_csv(&input)
        .join(right_relation_batches(), &["id"], &["id"], JoinType::Inner)
        .unwrap()
        .write_csv(&output);

    let stats = pipeline.execute().await.unwrap();
    assert_eq!(stats.rows, 4); // source rows
    let out = read_to_string(&output);
    let mut lines: Vec<String> = out.lines().skip(1).map(|l| l.to_string()).collect();
    lines.sort();
    // Colliding right "id" becomes "id_r": id,score,id_r,name
    assert_eq!(
        lines,
        vec![
            "1,10,1,alice".to_string(),
            "1,20,1,alice".to_string(),
            "3,30,3,carol".to_string(),
        ]
    );
}

#[tokio::test]
async fn join_pipeline_left() {
    let input = temp_path("left.csv");
    let output = temp_path("left-out.csv");

    write_text(&input, "id,score\n1,10\n7,50\n");

    let mut pipeline = Pipeline::new();
    pipeline
        .read_csv(&input)
        .join(right_relation_batches(), &["id"], &["id"], JoinType::Left)
        .unwrap()
        .write_csv(&output);

    pipeline.execute().await.unwrap();
    let out = read_to_string(&output);
    let mut lines: Vec<String> = out.lines().skip(1).map(|l| l.to_string()).collect();
    lines.sort();
    // Unmatched id=7 keeps score but gets null id_r and name.
    assert_eq!(
        lines,
        vec!["1,10,1,alice".to_string(), "7,50,,".to_string()]
    );
}

#[tokio::test]
async fn join_pipeline_right() {
    let input = temp_path("left.csv");
    let output = temp_path("right-out.csv");

    write_text(&input, "id,score\n2,20\n3,30\n");

    let mut pipeline = Pipeline::new();
    pipeline
        .read_csv(&input)
        .join(right_relation_batches(), &["id"], &["id"], JoinType::Right)
        .unwrap()
        .write_csv(&output);

    pipeline.execute().await.unwrap();
    let out = read_to_string(&output);
    let mut lines: Vec<String> = out.lines().skip(1).map(|l| l.to_string()).collect();
    lines.sort();
    // alice (id=1) is unmatched on the build side.
    assert_eq!(
        lines,
        vec![
            ",,1,alice".to_string(),
            "2,20,2,bob".to_string(),
            "3,30,3,carol".to_string(),
        ]
    );
}

#[tokio::test]
async fn expression_filter_and_map() {
    let input = temp_path("expr.csv");
    let output = temp_path("expr-out.csv");

    write_text(&input, "id,name,score\n1,alice,90\n2,bob,40\n3,carol,72\n");

    let mut pipeline = Pipeline::new();
    pipeline
        .read_csv(&input)
        .filter_expr("score > 60 and length(name) > 3")
        .map_expr(&[("id", "id"), ("label", "upper(name)")])
        .write_csv(&output);

    pipeline.execute().await.unwrap();
    let out = read_to_string(&output);
    assert_eq!(out, "id,label\n1,ALICE\n3,CAROL\n");
}

#[tokio::test]
async fn expression_error_surfaces_at_execute() {
    let input = temp_path("badexpr.csv");
    let output = temp_path("badexpr-out.csv");
    write_text(&input, "a\n1\n");

    let mut pipeline = Pipeline::new();
    pipeline
        .read_csv(&input)
        .filter_expr("a >")
        .write_csv(&output);
    let err = pipeline.execute().await.err();
    let msg = match &err {
        Some(e) => e.to_string(),
        None => String::new(),
    };
    assert!(err.is_some(), "bad expression must fail execute");
    assert!(msg.contains("expression"), "unexpected error: {msg}");
}

#[tokio::test]
async fn aggregate_then_sort() {
    let input = temp_path("as.csv");
    let output = temp_path("as-out.csv");

    let mut csv = String::from("k,v\n");
    for i in 0..100 {
        let k = i % 10;
        csv.push_str(&format!("{k},{i}\n"));
    }
    write_text(&input, &csv);

    let mut pipeline = Pipeline::new();
    pipeline
        .read_csv(&input)
        .aggregate(&["k"], &[AggSpec::sum("v")])
        .sort_by(&["sum_v"])
        .write_csv(&output);

    let stats = pipeline.execute().await.unwrap();
    assert_eq!(stats.rows, 100); // source rows
    let out = read_to_string(&output);
    let mut lines: Vec<&str> = out.lines().collect();
    lines.sort();
    assert_eq!(lines.len(), 11, "header + one row per group: {out}");
    let sums: Vec<i32> = lines
        .iter()
        .filter(|l| !l.starts_with("k,")) // skip header
        .map(|l| l.split(',').nth(1).unwrap().parse().unwrap())
        .collect();
    assert!(
        sums.windows(2).all(|w| w[0] <= w[1]),
        "must be sorted: {out}"
    );
}
