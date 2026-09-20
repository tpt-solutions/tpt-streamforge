//! End-to-end tests for `tptforge sql`: projection, WHERE, GROUP BY with
//! aggregates, aliases, ORDER BY, and LIMIT.

use tpt_stream_cli::sql::build_sql_pipeline;

fn write(path: &std::path::Path, content: &str) {
    std::fs::write(path, content).unwrap();
}

fn sample_csv() -> String {
    let mut csv = String::from("region,amount,product\n");
    // north: 10 + 20 + 30 = 60 (3 rows), south: 5 (1 row), west: 0 (1 row)
    csv.push_str("north,10,widget\n");
    csv.push_str("north,20,widget\n");
    csv.push_str("north,30,gadget\n");
    csv.push_str("south,5,gadget\n");
    csv.push_str("west,0,widget\n");
    csv
}

fn run_to_csv(sql: &str) -> String {
    let dir = tempfile::tempdir().unwrap();
    let csv_path = dir.path().join("in.csv");
    write(&csv_path, &sample_csv());
    let sql = sql.replace("IN_CSV", csv_path.to_string_lossy().as_ref());
    let mut pipeline = build_sql_pipeline(&sql).unwrap();
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let batches = rt.block_on(pipeline.collect()).unwrap();
    tpt_stream_core::source::batches_to_csv(&batches)
}

#[test]
fn select_all() {
    let out = run_to_csv("SELECT * FROM 'IN_CSV'");
    assert_eq!(out, sample_csv());
}

#[test]
fn select_projection_and_where() {
    let out = run_to_csv("SELECT region, amount FROM 'IN_CSV' WHERE amount >= 20");
    assert_eq!(out, "region,amount\nnorth,20\nnorth,30\n");
}

#[test]
fn compound_where_and_arithmetic() {
    let out = run_to_csv("SELECT product, amount * 2 AS doubled FROM 'IN_CSV' WHERE region == 'north' AND amount >= 20");
    let lines: Vec<&str> = out.lines().collect();
    assert_eq!(lines[0], "product,doubled");
    assert!(lines.contains(&"widget,40"));
    assert!(lines.contains(&"gadget,60"));
}

#[test]
fn group_by_with_sum_and_count() {
    let out = run_to_csv(
        "SELECT region, SUM(amount) AS total, COUNT(*) AS n FROM 'IN_CSV' WHERE amount > 0 GROUP BY region",
    );
    let lines: Vec<&str> = out.lines().collect();
    assert_eq!(lines[0], "region,total,n");
    assert!(lines.contains(&"north,60,3"));
    assert!(lines.contains(&"south,5,1"));
    // west's only row has amount 0 and is filtered out
    assert_eq!(lines.len(), 3);
}

#[test]
fn order_by_and_limit() {
    let out = run_to_csv(
        "SELECT region, SUM(amount) AS total FROM 'IN_CSV' GROUP BY region ORDER BY total DESC LIMIT 1",
    );
    assert_eq!(out, "region,total\nnorth,60\n");
}

#[test]
fn group_by_without_aggregates_distinct_keys() {
    let out = run_to_csv("SELECT region FROM 'IN_CSV' GROUP BY region");
    let mut lines: Vec<&str> = out.lines().collect();
    lines.sort_unstable();
    assert_eq!(lines, vec!["north", "region", "south", "west"]);
}

#[test]
fn avg_min_max_count_column() {
    let out = run_to_csv(
        "SELECT AVG(amount) AS avg_amount, MIN(amount) AS min_amount, MAX(amount) AS max_amount, COUNT(amount) AS n_amount FROM 'IN_CSV'",
    );
    assert_eq!(
        out,
        "avg_amount,min_amount,max_amount,n_amount\n13,0,30,5\n"
    );
}

#[test]
fn unsupported_sql_is_rejected_clearly() {
    let err = build_sql_pipeline("UPDATE t SET a = 1")
        .map(|_: tpt_stream_core::Pipeline| ())
        .unwrap_err();
    assert!(err.to_string().contains("SELECT"), "{err}");

    let err = build_sql_pipeline("SELECT a FROM 'IN_CSV' JOIN b ON a = b")
        .map(|_: tpt_stream_core::Pipeline| ())
        .unwrap_err();
    assert!(err.to_string().contains("JOIN"), "{err}");

    let dir = tempfile::tempdir().unwrap();
    let csv_path = dir.path().join("in.csv");
    write(&csv_path, "a\n1\n");
    let err = build_sql_pipeline(&format!(
        "SELECT a FROM '{}' WHERE a BETWEEN 1 AND 5",
        csv_path.to_string_lossy()
    ))
    .map(|_: tpt_stream_core::Pipeline| ())
    .unwrap_err();
    assert!(err.to_string().contains("unsupported"), "{err}");
}
