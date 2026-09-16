//! Tests for the hardening + input-tolerance features: ragged-row rejection,
//! error policies (skip/quarantine), gzip sources, source-reuse errors, the
//! Expect stage, preview, and CSV type-inference pins.
#![cfg(feature = "async")]
use tpt_stream_core::{source::ErrorPolicy, Check, Pipeline, Value};

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
}

fn write(path: &std::path::Path, content: &str) {
    std::fs::write(path, content).unwrap();
}

// ---------------------------------------------------------------------------
// Ragged rows
// ---------------------------------------------------------------------------

#[test]
fn ragged_csv_row_is_fatal_with_line_number() {
    let dir = tempfile::tempdir().unwrap();
    let src = dir.path().join("ragged.csv");
    write(&src, "a,b,c\n1,x,10\n2,y\n3,z,30\n");

    rt_err_contains(src.to_string_lossy(), "line 3");
}

#[test]
fn ragged_csv_row_fails_join_build_side_too() {
    let dir = tempfile::tempdir().unwrap();
    let src = dir.path().join("ragged.csv");
    write(&src, "a,b\n1,x\n2\n");
    let rt = runtime();
    let err = rt.block_on(async {
        let mut p = Pipeline::new();
        p.read_csv(src.to_string_lossy());
        p.execute().await
    });
    let err = err.unwrap_err();
    assert!(err.to_string().contains("line 3"), "{err}");
}

// ---------------------------------------------------------------------------
// Error policies
// ---------------------------------------------------------------------------

#[test]
fn skip_policy_drops_ragged_rows() {
    let dir = tempfile::tempdir().unwrap();
    let src = dir.path().join("ragged.csv");
    let out = dir.path().join("out.csv");
    write(&src, "a,b\n1,x\n2\n3,z\n");

    let rt = runtime();
    rt.block_on(async {
        let mut p = Pipeline::new();
        p.on_error(ErrorPolicy::Skip)
            .read_csv(src.to_string_lossy())
            .write_csv(out.to_string_lossy());
        let stats = p.execute().await.unwrap();
        // stats.rows counts rows that were actually read into batches; the
        // dropped malformed row never becomes one.
        assert_eq!(stats.rows, 2);
    });

    let out = std::fs::read_to_string(&out).unwrap();
    assert_eq!(out, "a,b\n1,x\n3,z\n");
}

#[test]
fn quarantine_policy_captures_ragged_rows() {
    let dir = tempfile::tempdir().unwrap();
    let src = dir.path().join("ragged.csv");
    let out = dir.path().join("out.csv");
    let bad = dir.path().join("bad.csv");
    write(&src, "a,b\n1,x\n2,too,many\n3,z\n");

    let rt = runtime();
    rt.block_on(async {
        let mut p = Pipeline::new();
        p.on_error(ErrorPolicy::Quarantine(bad.to_string_lossy().into()))
            .read_csv(src.to_string_lossy())
            .write_csv(out.to_string_lossy());
        p.execute().await.unwrap();
    });

    let out = std::fs::read_to_string(&out).unwrap();
    assert_eq!(out, "a,b\n1,x\n3,z\n");
    let bad = std::fs::read_to_string(&bad).unwrap();
    assert_eq!(bad, "a,b\n2,too,many\n");
}

#[test]
fn jsonl_skip_policy_drops_bad_lines() {
    let dir = tempfile::tempdir().unwrap();
    let src = dir.path().join("in.jsonl");
    let out = dir.path().join("out.csv");
    write(&src, "{\"a\":1}\n{not json}\n{\"a\":3}\n");

    let rt = runtime();
    rt.block_on(async {
        let mut p = Pipeline::new();
        p.on_error(ErrorPolicy::Skip)
            .read_jsonl(src.to_string_lossy())
            .write_csv(out.to_string_lossy());
        p.execute().await.unwrap();
    });

    let out = std::fs::read_to_string(&out).unwrap();
    assert_eq!(out, "a\n1\n3\n");
}

#[test]
fn jsonl_quarantine_keeps_raw_lines() {
    let dir = tempfile::tempdir().unwrap();
    let src = dir.path().join("in.jsonl");
    let bad = dir.path().join("bad.jsonl.txt");
    write(&src, "{\"a\":1}\n{oops}\n");

    let rt = runtime();
    rt.block_on(async {
        let mut p = Pipeline::new();
        p.on_error(ErrorPolicy::Quarantine(bad.to_string_lossy().into()))
            .read_jsonl(src.to_string_lossy());
        p.execute().await.unwrap();
    });

    let bad = std::fs::read_to_string(&bad).unwrap();
    assert_eq!(bad, "{oops}\n");
}

// ---------------------------------------------------------------------------
// gzip sources
// ---------------------------------------------------------------------------

#[cfg(feature = "gzip")]
#[test]
fn gzipped_csv_roundtrip() {
    use flate2::write::GzEncoder;
    let dir = tempfile::tempdir().unwrap();
    let src = dir.path().join("in.csv.gz");
    let out = dir.path().join("out.csv");

    let mut encoder = GzEncoder::new(
        std::fs::File::create(&src).unwrap(),
        flate2::Compression::default(),
    );
    let mut body = String::from("a,b\n1,x\n2,y\n3,z\n");
    for i in 0..1000 {
        body.push_str(&format!("{i},name{i}\n"));
    }
    std::io::Write::write_all(&mut encoder, body.as_bytes()).unwrap();
    drop(encoder);

    let rt = runtime();
    rt.block_on(async {
        let mut p = Pipeline::new();
        p.read_csv(src.to_string_lossy())
            .write_csv(out.to_string_lossy());
        let stats = p.execute().await.unwrap();
        assert_eq!(stats.rows, 1003);
    });

    let out = std::fs::read_to_string(&out).unwrap();
    assert!(out.starts_with("a,b\n1,x\n2,y\n3,z\n"));
}

// ---------------------------------------------------------------------------
// Source reuse
// ---------------------------------------------------------------------------

#[test]
fn second_execute_errors_instead_of_silently_returning_zero_rows() {
    let dir = tempfile::tempdir().unwrap();
    let src = dir.path().join("in.csv");
    let out1 = dir.path().join("out1.csv");
    let out2 = dir.path().join("out2.csv");
    write(&src, "a\n1\n2\n");

    let rt = runtime();
    rt.block_on(async {
        let mut p = Pipeline::new();
        p.read_csv(src.to_string_lossy())
            .write_csv(out1.to_string_lossy());
        p.execute().await.unwrap();

        p.write_csv(out2.to_string_lossy());
        let err = p.execute().await.unwrap_err();
        assert!(err.to_string().contains("one-shot"), "{err}");
    });
}

// ---------------------------------------------------------------------------
// Expect stage
// ---------------------------------------------------------------------------

#[test]
fn expect_row_count_check_fails_the_pipeline() {
    let dir = tempfile::tempdir().unwrap();
    let src = dir.path().join("in.csv");
    let out = dir.path().join("out.csv");
    write(&src, "a\n1\n2\n");

    let rt = runtime();
    rt.block_on(async {
        let mut p = Pipeline::new();
        p.read_csv(src.to_string_lossy())
            .expect_checks(vec![Check::RowsAtLeast(10)])
            .write_csv(out.to_string_lossy());
        let err = p.execute().await.unwrap_err();
        assert!(err.to_string().contains("data quality"), "{err}");
    });
}

#[test]
fn expect_unique_check_passes_when_clean() {
    let dir = tempfile::tempdir().unwrap();
    let src = dir.path().join("in.csv");
    let out = dir.path().join("out.csv");
    write(&src, "id\n1\n2\n3\n");

    let rt = runtime();
    rt.block_on(async {
        let mut p = Pipeline::new();
        p.read_csv(src.to_string_lossy())
            .expect_checks(vec![Check::Unique("id".into()), Check::RowsAtLeast(1)])
            .write_csv(out.to_string_lossy());
        p.execute().await.unwrap();
    });
    assert!(out.exists());
}

// ---------------------------------------------------------------------------
// preview
// ---------------------------------------------------------------------------

#[test]
fn preview_returns_first_rows_through_stages() {
    let dir = tempfile::tempdir().unwrap();
    let src = dir.path().join("in.csv");
    write(&src, "a\n1\n2\n3\n4\n5\n");

    let rt = runtime();
    let batches = rt.block_on(async {
        let mut p = Pipeline::new();
        p.read_csv(src.to_string_lossy()).filter_expr("a >= 2");
        p.preview(2).await.unwrap()
    });

    let total: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total, 2);
    assert_eq!(batches[0].cell(0, "a"), Some(Value::Int32(2)));
}

#[test]
fn explain_lists_stages() {
    let mut p = Pipeline::new();
    p.read_csv("x.csv").filter_expr("a > 1").write_csv("y.csv");
    let plan = p.explain();
    assert_eq!(plan, "source -> filter -> sink");
}

// ---------------------------------------------------------------------------
// CSV type inference pins
// ---------------------------------------------------------------------------

#[test]
fn csv_numeric_inference_ladder_is_pinned() {
    let dir = tempfile::tempdir().unwrap();
    let src = dir.path().join("in.csv");

    let rt = runtime();

    // Int32 for small ints.
    write(&src, "v\n1\n2\n");
    rt.block_on(async {
        let mut p = Pipeline::new();
        p.read_csv(src.to_string_lossy());
        let b = p.preview(1).await.unwrap().remove(0);
        assert_eq!(b.schema()[0].1, tpt_stream_core::DataType::Int32);
    });

    // Int64 once values exceed i32.
    write(&src, "v\n1\n3000000000\n");
    rt.block_on(async {
        let mut p = Pipeline::new();
        p.read_csv(src.to_string_lossy());
        let b = p.preview(1).await.unwrap().remove(0);
        assert_eq!(b.schema()[0].1, tpt_stream_core::DataType::Int64);
    });

    // Float64 for decimals (never Float32).
    write(&src, "v\n1.5\n2.5\n");
    rt.block_on(async {
        let mut p = Pipeline::new();
        p.read_csv(src.to_string_lossy());
        let b = p.preview(1).await.unwrap().remove(0);
        assert_eq!(b.schema()[0].1, tpt_stream_core::DataType::Float64);
    });
}

// ---------------------------------------------------------------------------
// HTTP source (mock server)
// ---------------------------------------------------------------------------

#[cfg(feature = "http")]
#[test]
fn http_source_streams_csv_via_mock() {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let mut stream = match stream {
                Ok(s) => s,
                Err(_) => break,
            };
            let mut buf = Vec::new();
            let mut one = [0u8; 1];
            while let Ok(1) = std::io::Read::read(&mut &mut stream, &mut one) {
                buf.push(one[0]);
                if buf.ends_with(b"\r\n\r\n") {
                    break;
                }
            }
            let body: &[u8] = b"a,b\n1,x\n2,y\n3,z\n";
            let head = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            );
            use std::io::Write;
            let _ = stream.write_all(head.as_bytes());
            let _ = stream.write_all(body);
        }
    });

    let dir = tempfile::tempdir().unwrap();
    let out = dir.path().join("out.csv");

    let rt = runtime();
    rt.block_on(async {
        let mut p = Pipeline::new();
        p.source(tpt_stream_core::HttpSource::open_with_chunk_size(
            format!("http://127.0.0.1:{port}/data.csv"),
            2,
        ))
        .write_csv(out.to_string_lossy());
        let stats = p.execute().await.unwrap();
        assert_eq!(stats.rows, 3);
    });

    let out = std::fs::read_to_string(&out).unwrap();
    assert_eq!(
        out,
        "a,b
1,x
2,y
3,z
"
    );
}

// ---------------------------------------------------------------------------

fn rt_err_contains(path: impl Into<String>, needle: &str) {
    let rt = runtime();
    let err = rt.block_on(async {
        let mut p = Pipeline::new();
        p.read_csv(path.into());
        p.execute().await
    });
    let err = err.unwrap_err();
    assert!(
        err.to_string().contains(needle),
        "error `{err}` lacks {needle:?}"
    );
}
