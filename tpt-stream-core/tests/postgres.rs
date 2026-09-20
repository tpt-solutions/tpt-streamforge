//! Integration tests for the PostgreSQL source/sink (feature `postgres`).
//!
//! Skipped unless `TPT_TEST_POSTGRES_URL` is set, e.g.
//! `postgres://postgres:postgres@localhost:5432/postgres`.
//! The `postgres-integration` CI job sets this against a Postgres service
//! container; other jobs (and local runs without Postgres) leave it unset,
//! so these tests print a note and pass.
#![cfg(all(feature = "async", feature = "postgres"))]
use tpt_stream_core::{Pipeline, PostgresSink, PostgresSource, Source, Value};

fn postgres_url() -> Option<String> {
    std::env::var("TPT_TEST_POSTGRES_URL").ok()
}

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
}

fn skip_note() {
    eprintln!("skipping postgres test: TPT_TEST_POSTGRES_URL not set");
}

/// Wipe and recreate a fresh table between tests.
fn reset_table(conn_string: &str, table: &str) {
    runtime().block_on(async {
        let (client, conn) = tokio_postgres::connect(conn_string, tokio_postgres::NoTls)
            .await
            .unwrap();
        tokio::spawn(conn);
        client
            .batch_execute(&format!(
                "DROP TABLE IF EXISTS {table}; DROP TABLE IF EXISTS {table}_readback;"
            ))
            .await
            .unwrap();
    });
}

#[test]
fn postgres_sink_copy_source_roundtrip() {
    let Some(url) = postgres_url() else {
        skip_note();
        return;
    };
    reset_table(&url, "pg_roundtrip");

    let dir = tempfile::tempdir().unwrap();
    let csv_path = dir.path().join("in.csv");
    let mut csv = String::from("id,name,score,active\n");
    for i in 0..2000 {
        csv.push_str(&format!(
            "{},name{i},{:.2},{}\n",
            i,
            i as f64 / 4.0,
            if i % 2 == 0 { "true" } else { "false" }
        ));
    }
    std::fs::write(&csv_path, csv).unwrap();

    runtime().block_on(async {
        // CSV -> COPY -> table
        let mut write = Pipeline::new();
        write
            .with_chunk_size(300)
            .read_csv(csv_path.to_string_lossy())
            .write_postgres(&url, "pg_roundtrip");
        let stats = write.execute().await.unwrap();
        assert_eq!(stats.rows, 2000);

        // table -> SELECT -> CSV
        let mut read = Pipeline::new();
        read.read_postgres(
            &url,
            "SELECT id, name, score, active FROM pg_roundtrip WHERE id < 100 ORDER BY id",
        )
        .write_csv(dir.path().join("back.csv").to_string_lossy());
        let stats = read.execute().await.unwrap();
        assert_eq!(stats.rows, 100);
    });

    let csv = std::fs::read_to_string(dir.path().join("back.csv")).unwrap();
    let mut lines = csv.lines();
    assert_eq!(lines.next(), Some("id,name,score,active"));
    assert_eq!(lines.next(), Some("0,name0,0,true"));
    assert_eq!(lines.next(), Some("1,name1,0.25,false"));
}

#[test]
fn postgres_sink_handles_nulls_and_special_text() {
    let Some(url) = postgres_url() else {
        skip_note();
        return;
    };
    reset_table(&url, "pg_special");

    let dir = tempfile::tempdir().unwrap();
    let csv_path = dir.path().join("in.csv");
    // Values that exercise COPY text-format escaping: backslash, tab,
    // newline, carriage return, single quote, and an empty field (NULL).
    std::fs::write(
        &csv_path,
        "id,text\n1,back\\slash\n2,tab\there\n3,quote's\n4,\n5,\"line\nbreak\"\n",
    )
    .unwrap();

    runtime().block_on(async {
        let mut write = Pipeline::new();
        write
            .read_csv(csv_path.to_string_lossy())
            .write_postgres(&url, "pg_special");
        let stats = write.execute().await.unwrap();
        assert_eq!(stats.rows, 5);

        let (client, conn) = tokio_postgres::connect(&url, tokio_postgres::NoTls)
            .await
            .unwrap();
        tokio::spawn(conn);
        let rows = client
            .query("SELECT id, text FROM pg_special ORDER BY id", &[])
            .await
            .unwrap();
        assert_eq!(rows.len(), 5);
        assert_eq!(rows[0].get::<_, String>(1), "back\\slash");
        assert_eq!(rows[1].get::<_, String>(1), "tab\there");
        assert_eq!(rows[2].get::<_, String>(1), "quote's");
        assert!(rows[3].get::<_, Option<String>>(1).is_none());
        assert_eq!(rows[4].get::<_, String>(1), "line\nbreak");
    });
}

#[test]
fn postgres_source_typed_columns_and_chunking() {
    let Some(url) = postgres_url() else {
        skip_note();
        return;
    };
    runtime().block_on(async {
        let (client, conn) = tokio_postgres::connect(&url, tokio_postgres::NoTls)
            .await
            .unwrap();
        tokio::spawn(conn);
        client
            .batch_execute(
                "DROP TABLE IF EXISTS pg_types;
                 CREATE TABLE pg_types (a INT4, b INT8, c FLOAT4, d FLOAT8, e BOOL, f TEXT, g DATE);
                 INSERT INTO pg_types VALUES (1, 2, 1.5, 2.5, TRUE, 'x', '2024-01-15');
                 INSERT INTO pg_types VALUES (3, 4, 3.5, 4.5, FALSE, 'y', '2024-03-01');",
            )
            .await
            .unwrap();

        let mut source = PostgresSource::open_with_chunk_size(
            &url,
            "SELECT a, b, c, d, e, f, g FROM pg_types ORDER BY a",
            1,
        );
        let mut batches = Vec::new();
        while let Some(batch) = source.next_batch().await.unwrap() {
            batches.push(batch);
        }
        assert_eq!(batches.len(), 2); // chunk size 1, 2 rows
        let b = &batches[0];
        assert_eq!(b.cell(0, "a"), Some(Value::Int32(1)));
        assert_eq!(b.cell(0, "b"), Some(Value::Int64(2)));
        assert_eq!(b.cell(0, "c"), Some(Value::Float32(1.5)));
        assert_eq!(b.cell(0, "d"), Some(Value::Float64(2.5)));
        assert_eq!(b.cell(0, "e"), Some(Value::Bool(true)));
        assert_eq!(b.cell(0, "f"), Some(Value::Utf8("x".into())));
        assert_eq!(b.cell(0, "g"), Some(Value::Date(19_737))); // 2024-01-15
        assert_eq!(batches[1].cell(0, "e"), Some(Value::Bool(false)));
    });
}

#[test]
fn postgres_sink_overwrite_replaces_table() {
    let Some(url) = postgres_url() else {
        skip_note();
        return;
    };
    let dir = tempfile::tempdir().unwrap();
    let seed = dir.path().join("seed.csv");
    std::fs::write(&seed, "a\n10\n20\n").unwrap();

    runtime().block_on(async {
        let (client, conn) = tokio_postgres::connect(&url, tokio_postgres::NoTls)
            .await
            .unwrap();
        tokio::spawn(conn);
        client
            .batch_execute(
                "DROP TABLE IF EXISTS pg_overwrite;
                 CREATE TABLE pg_overwrite (a INT4);
                 INSERT INTO pg_overwrite VALUES (1),(2),(3);",
            )
            .await
            .unwrap();

        let mut pipeline = Pipeline::new();
        pipeline
            .read_csv(seed.to_string_lossy())
            .sink(PostgresSink::new(&url, "pg_overwrite").overwrite());
        let stats = pipeline.execute().await.unwrap();
        assert_eq!(stats.rows, 2);

        let n: i64 = client
            .query_one("SELECT COUNT(*) FROM pg_overwrite", &[])
            .await
            .unwrap()
            .get(0);
        assert_eq!(n, 2);
    });
}
