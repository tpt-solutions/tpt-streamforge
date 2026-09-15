//! Integration tests for SQLite source/sink (feature `sqlite`).
#![cfg(feature = "sqlite")]
use tpt_stream_core::{Pipeline, Source, SqliteSink, SqliteSource, Value};

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
}

#[test]
fn sqlite_sink_source_roundtrip() {
    let dir = tempfile::tempdir().unwrap();
    let csv_path = dir.path().join("in.csv");
    let db_path = dir.path().join("out.sqlite");
    let mut csv = String::from("id,name,score,active\n");
    for i in 0..500 {
        csv.push_str(&format!(
            "{},name{i},{:.2},{}\n",
            i,
            i as f64 / 4.0,
            if i % 2 == 0 { "true" } else { "false" }
        ));
    }
    std::fs::write(&csv_path, csv).unwrap();

    runtime().block_on(async {
        let mut write = Pipeline::new();
        write
            .read_csv(csv_path.to_string_lossy())
            .write_sqlite(db_path.to_string_lossy(), "metrics");
        let stats = write.execute().await.unwrap();
        assert_eq!(stats.rows, 500);

        let mut read = Pipeline::new();
        read.read_sqlite(
            db_path.to_string_lossy(),
            "SELECT id, name, score, active FROM metrics WHERE id < 100 ORDER BY id",
        )
        .write_csv(dir.path().join("back.csv").to_string_lossy());
        let stats = read.execute().await.unwrap();
        assert_eq!(stats.rows, 100);
    });

    let csv = std::fs::read_to_string(dir.path().join("back.csv")).unwrap();
    let mut lines = csv.lines();
    assert_eq!(lines.next(), Some("id,name,score,active"));
    assert_eq!(lines.next(), Some("0,name0,0,1")); // bool round-trips as int
    assert_eq!(lines.next(), Some("1,name1,0.25,0"));
}

#[test]
fn sqlite_source_typed_columns_and_chunking() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("typed.sqlite");
    let conn = rusqlite::Connection::open(&db_path).unwrap();
    conn.execute_batch(
        "CREATE TABLE t (a INTEGER, b REAL, c TEXT, d BOOLEAN);
         INSERT INTO t VALUES (1, 1.5, 'x', 1);
         INSERT INTO t VALUES (2, 2.5, 'y', 0);",
    )
    .unwrap();
    drop(conn);

    runtime().block_on(async {
        let mut source =
            SqliteSource::open_with_chunk_size(db_path.to_string_lossy(), "SELECT * FROM t", 1);
        let mut batches = Vec::new();
        while let Some(batch) = source.next_batch().await.unwrap() {
            batches.push(batch);
        }
        assert_eq!(batches.len(), 2); // chunk size 1, 2 rows
        assert_eq!(batches[0].num_rows(), 1);
        assert_eq!(batches[0].cell(0, "a"), Some(Value::Int64(1)));
        assert_eq!(batches[0].cell(0, "b"), Some(Value::Float64(1.5)));
        assert_eq!(batches[0].cell(0, "c"), Some(Value::Utf8("x".into())));
        assert_eq!(batches[0].cell(0, "d"), Some(Value::Bool(true)));
        assert_eq!(batches[1].cell(0, "d"), Some(Value::Bool(false)));
    });
}

#[test]
fn sqlite_sink_overwrite_replaces_table() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("typed.sqlite");
    let conn = rusqlite::Connection::open(&db_path).unwrap();
    conn.execute_batch(
        "CREATE TABLE t (a INTEGER);
         INSERT INTO t VALUES (1),(2),(3);",
    )
    .unwrap();
    drop(conn);
    let seed = dir.path().join("seed.csv");
    std::fs::write(&seed, "a\n10\n20\n").unwrap();

    runtime().block_on(async {
        let mut pipeline = Pipeline::new();
        pipeline
            .read_csv(seed.to_string_lossy())
            .sink(SqliteSink::open(db_path.to_string_lossy(), "t").overwrite());
        let stats = pipeline.execute().await.unwrap();
        assert_eq!(stats.rows, 2);
    });

    let conn = rusqlite::Connection::open(&db_path).unwrap();
    let n: i64 = conn
        .query_row("SELECT COUNT(*) FROM t", [], |r| r.get(0))
        .unwrap();
    assert_eq!(n, 2);
}

#[test]
fn sqlite_pipeline_filter_stats() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("chunked.sqlite");
    let mut csv = String::from("v\n");
    for i in 0..1000 {
        csv.push_str(&format!("{i}\n"));
    }
    std::fs::write(dir.path().join("v.csv"), csv).unwrap();

    runtime().block_on(async {
        let mut pipeline = Pipeline::new();
        pipeline
            .with_chunk_size(64)
            .read_csv(dir.path().join("v.csv").to_string_lossy())
            .filter_expr("v >= 500")
            .write_sqlite(db_path.to_string_lossy(), "vals");
        let stats = pipeline.execute().await.unwrap();
        assert_eq!(stats.rows, 1000);

        let mut count = Pipeline::new();
        count
            .read_sqlite(db_path.to_string_lossy(), "SELECT COUNT(*) AS n FROM vals")
            .write_csv(dir.path().join("count.csv").to_string_lossy());
        let stats = count.execute().await.unwrap();
        assert_eq!(stats.rows, 1);
    });
    let count = std::fs::read_to_string(dir.path().join("count.csv")).unwrap();
    assert_eq!(count.trim(), "n\n500");
}
