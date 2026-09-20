//! Date/timestamp end-to-end tests: CSV inference, expression comparisons,
//! sorting, `.tptcol` round trips, JSON output, and SQLite storage.

use tpt_stream_core::{Pipeline, Value};

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
}

fn write(path: &std::path::Path, content: &str) {
    std::fs::write(path, content).unwrap();
}

#[test]
fn csv_infers_dates_and_timestamps() {
    let dir = tempfile::tempdir().unwrap();
    let src = dir.path().join("events.csv");
    write(
        &src,
        "day,at,note\n\
         2024-01-15,2024-01-15T10:30:00Z,launch\n\
         2024-03-01,2024-03-01T08:00:00.5Z,review\n\
         1999-12-31,1999-12-31T23:59:59Z,party\n",
    );

    let rt = runtime();
    rt.block_on(async {
        let mut p = Pipeline::new();
        p.read_csv(src.to_string_lossy());
        let batch = p.preview(1).await.unwrap().remove(0);
        assert_eq!(batch.schema()[0].1, tpt_stream_core::DataType::Date);
        assert_eq!(batch.schema()[1].1, tpt_stream_core::DataType::Timestamp);
        assert_eq!(batch.cell(0, "day"), Some(Value::Date(19_737)));
    });
}

#[test]
fn date_filter_and_sort_by_date() {
    let dir = tempfile::tempdir().unwrap();
    let src = dir.path().join("in.csv");
    let out = dir.path().join("out.csv");
    write(
        &src,
        "day,v\n2024-01-15,1\n2024-03-01,2\n1999-12-31,3\n2024-02-10,4\n",
    );

    let rt = runtime();
    rt.block_on(async {
        let mut p = Pipeline::new();
        p.read_csv(src.to_string_lossy())
            .filter_expr("day >= '2024-01-15'")
            .sort_by(&["day"])
            .write_csv(out.to_string_lossy());
        let stats = p.execute().await.unwrap();
        assert_eq!(stats.rows, 4);
    });

    assert_eq!(std::fs::read_to_string(&out).unwrap().lines().count(), 4); // header + 3
    let out = std::fs::read_to_string(&out).unwrap();
    let lines: Vec<&str> = out.lines().collect();
    assert_eq!(lines[0], "day,v");
    // The 1999 row was filtered out by day >= '2024-01-15'.
    assert_eq!(lines[1], "2024-01-15,1");
    assert_eq!(lines[2], "2024-02-10,4");
    assert_eq!(lines[3], "2024-03-01,2");
}

#[test]
fn timestamps_sort_chronologically() {
    let dir = tempfile::tempdir().unwrap();
    let src = dir.path().join("in.csv");
    let out = dir.path().join("out.csv");
    write(
        &src,
        "at\n2024-01-01T12:00:00Z\n2024-01-01T11:00:00Z\n2024-01-01T12:00:00.000001Z\n",
    );

    let rt = runtime();
    rt.block_on(async {
        let mut p = Pipeline::new();
        p.read_csv(src.to_string_lossy())
            .sort_by(&["at"])
            .write_csv(out.to_string_lossy());
        p.execute().await.unwrap();
    });

    let out = std::fs::read_to_string(&out).unwrap();
    let lines: Vec<&str> = out.lines().collect();
    assert_eq!(lines[1], "2024-01-01T11:00:00Z");
    assert_eq!(lines[2], "2024-01-01T12:00:00Z");
    assert_eq!(lines[3], "2024-01-01T12:00:00.000001Z");
}

#[test]
fn date_tptcol_roundtrip() {
    let dir = tempfile::tempdir().unwrap();
    let src = dir.path().join("in.csv");
    let col = dir.path().join("data.tptcol");
    let out = dir.path().join("out.csv");
    write(
        &src,
        "day,ts\n2024-01-15,2024-01-15T10:30:00Z\n1970-01-01,1970-01-01T00:00:00Z\n",
    );

    runtime().block_on(async {
        let mut p = Pipeline::new();
        p.read_csv(src.to_string_lossy())
            .write_columnar(col.to_string_lossy(), true);
        p.execute().await.unwrap();

        let mut p = Pipeline::new();
        p.read_columnar(col.to_string_lossy())
            .write_csv(out.to_string_lossy());
        p.execute().await.unwrap();
    });

    let out = std::fs::read_to_string(&out).unwrap();
    assert_eq!(
        out,
        "day,ts\n2024-01-15,2024-01-15T10:30:00Z\n1970-01-01,1970-01-01T00:00:00Z\n"
    );
}

#[test]
fn dates_serialize_as_iso_in_json() {
    let dir = tempfile::tempdir().unwrap();
    let src = dir.path().join("in.csv");
    let out = dir.path().join("out.json");
    write(&src, "day\n2024-01-15\n");

    runtime().block_on(async {
        let mut p = Pipeline::new();
        p.read_csv(src.to_string_lossy())
            .write_json(out.to_string_lossy(), false);
        p.execute().await.unwrap();
    });

    let out = std::fs::read_to_string(&out).unwrap();
    assert_eq!(out, r#"[{"day":"2024-01-15"}]"#);
}

#[test]
fn sqlite_date_roundtrip() {
    let dir = tempfile::tempdir().unwrap();
    let src = dir.path().join("in.csv");
    let db = dir.path().join("out.sqlite");
    let out = dir.path().join("back.csv");
    write(&src, "day,v\n2024-01-15,1\n2024-03-01,2\n");

    runtime().block_on(async {
        let mut p = Pipeline::new();
        p.read_csv(src.to_string_lossy())
            .write_sqlite(db.to_string_lossy(), "events");
        p.execute().await.unwrap();

        let mut p = Pipeline::new();
        p.read_sqlite(
            db.to_string_lossy(),
            "SELECT day, v FROM events ORDER BY day",
        )
        .write_csv(out.to_string_lossy());
        p.execute().await.unwrap();
    });

    // Declared DATE affinity is preserved through the round trip.
    let out = std::fs::read_to_string(&out).unwrap();
    assert!(out.contains("2024-01-15,1"));
    assert!(out.contains("2024-03-01,2"));
}

#[test]
fn limit_stage_truncates_stream() {
    let dir = tempfile::tempdir().unwrap();
    let src = dir.path().join("in.csv");
    let out = dir.path().join("out.csv");
    write(&src, "v\n1\n2\n3\n4\n5\n");

    runtime().block_on(async {
        let mut p = Pipeline::new();
        p.read_csv(src.to_string_lossy())
            .limit(3)
            .write_csv(out.to_string_lossy());
        let stats = p.execute().await.unwrap();
        assert_eq!(stats.rows, 5); // source rows read
    });

    let out = std::fs::read_to_string(&out).unwrap();
    assert_eq!(out, "v\n1\n2\n3\n");
}
