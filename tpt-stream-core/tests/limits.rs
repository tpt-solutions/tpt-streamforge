//! Regression tests for the input-size caps (audit findings F1 follow-up, F14):
//! oversized CSV records, JSONL lines, JSON array elements, gzip output and
//! HTTP bodies must fail with a clear error rather than exhaust memory.
#![cfg(feature = "async")]
use tpt_stream_core::source::{CsvSource, JsonArraySource, JsonlSource, Source};
use tpt_stream_core::SourceLimits;

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
}

/// Drain a source, returning total rows or the first error message.
fn drain(mut src: impl Source) -> Result<usize, String> {
    runtime().block_on(async {
        let mut rows = 0;
        while let Some(b) = src.next_batch().await.map_err(|e| e.to_string())? {
            rows += b.num_rows();
        }
        Ok(rows)
    })
}

fn small_limits(max_record_bytes: usize) -> SourceLimits {
    SourceLimits {
        max_record_bytes,
        ..SourceLimits::default()
    }
}

#[test]
fn csv_unterminated_quote_hits_record_cap() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("q.csv");
    let mut text = String::from("a,b\n1,\"");
    text.push_str(&"x".repeat(500_000));
    std::fs::write(&path, text).unwrap();
    let err =
        drain(CsvSource::open(path.to_string_lossy()).with_limits(small_limits(4096))).unwrap_err();
    assert!(err.contains("record size limit"), "{err}");
}

#[test]
fn csv_within_cap_still_reads() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("ok.csv");
    std::fs::write(&path, "a,b\n1,x\n2,y\n").unwrap();
    let rows =
        drain(CsvSource::open(path.to_string_lossy()).with_limits(small_limits(64))).unwrap();
    assert_eq!(rows, 2);
}

#[test]
fn jsonl_line_without_newline_hits_cap() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("big.jsonl");
    let mut text = String::from("{\"a\":1}\n{\"s\":\"");
    text.push_str(&"y".repeat(500_000));
    std::fs::write(&path, text).unwrap();
    let err = drain(JsonlSource::open(path.to_string_lossy()).with_limits(small_limits(4096)))
        .unwrap_err();
    assert!(err.contains("record size limit"), "{err}");
}

#[test]
fn jsonl_within_cap_still_reads() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("ok.jsonl");
    std::fs::write(&path, "{\"a\":1}\n{\"a\":2}\n").unwrap();
    let rows =
        drain(JsonlSource::open(path.to_string_lossy()).with_limits(small_limits(64))).unwrap();
    assert_eq!(rows, 2);
}

#[test]
fn json_array_element_hits_cap() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("big.json");
    let mut text = String::from("[{\"a\":1},{\"s\":\"");
    text.push_str(&"z".repeat(500_000));
    text.push_str("\"}]");
    std::fs::write(&path, text).unwrap();
    let err = drain(JsonArraySource::open_with_limits(
        path.to_string_lossy(),
        1024,
        small_limits(4096),
    ))
    .unwrap_err();
    assert!(err.contains("record size limit"), "{err}");
}

#[test]
fn json_array_within_cap_still_reads() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("ok.json");
    std::fs::write(&path, "[{\"a\":1},{\"a\":2}]").unwrap();
    let rows = drain(JsonArraySource::open_with_limits(
        path.to_string_lossy(),
        1024,
        small_limits(64),
    ))
    .unwrap();
    assert_eq!(rows, 2);
}

#[cfg(feature = "gzip")]
fn gzip_bytes(body: &[u8]) -> Vec<u8> {
    use std::io::Write;
    let mut enc = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    enc.write_all(body).unwrap();
    enc.finish().unwrap()
}

#[cfg(feature = "gzip")]
#[test]
fn gzip_file_decompressed_output_is_capped() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("bomb.csv.gz");
    // ~2 MB of highly compressible rows, capped at 64 KiB decompressed.
    let mut body = String::from("a\n");
    for _ in 0..500_000 {
        body.push_str("1\n");
    }
    std::fs::write(&path, gzip_bytes(body.as_bytes())).unwrap();
    let limits = SourceLimits {
        max_decompressed_bytes: 64 * 1024,
        ..SourceLimits::default()
    };
    let err = drain(CsvSource::open(path.to_string_lossy()).with_limits(limits)).unwrap_err();
    assert!(err.contains("decompressed gzip output exceeds"), "{err}");

    // The same file is fine under the default cap.
    let rows = drain(CsvSource::open(path.to_string_lossy())).unwrap();
    assert_eq!(rows, 500_000);
}

#[cfg(feature = "gzip")]
#[test]
fn gzip_output_of_exactly_the_cap_is_accepted() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("exact.csv.gz");
    let body = "a\n1\n2\n";
    std::fs::write(&path, gzip_bytes(body.as_bytes())).unwrap();
    let limits = SourceLimits {
        max_decompressed_bytes: body.len() as u64,
        ..SourceLimits::default()
    };
    assert_eq!(
        drain(CsvSource::open(path.to_string_lossy()).with_limits(limits)).unwrap(),
        2
    );
}

#[cfg(all(feature = "http", feature = "gzip"))]
fn serve_body(headers: &'static str, body: Vec<u8>) -> u16 {
    use std::io::{Read, Write};
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    std::thread::spawn(move || {
        if let Ok((mut s, _)) = listener.accept() {
            let mut buf = Vec::new();
            let mut one = [0u8; 1];
            while let Ok(1) = s.read(&mut one) {
                buf.push(one[0]);
                if buf.ends_with(b"\r\n\r\n") {
                    break;
                }
            }
            let head = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\n{headers}Connection: close\r\n\r\n",
                body.len()
            );
            let _ = s.write_all(head.as_bytes());
            let _ = s.write_all(&body);
        }
    });
    port
}

#[cfg(all(feature = "http", feature = "gzip"))]
#[test]
fn http_gzip_response_output_is_capped() {
    let mut body = String::from("a\n");
    for _ in 0..500_000 {
        body.push_str("1\n");
    }
    let port = serve_body("", gzip_bytes(body.as_bytes()));
    let limits = SourceLimits {
        max_decompressed_bytes: 64 * 1024,
        ..SourceLimits::default()
    };
    let src = tpt_stream_core::HttpSource::open(format!("http://127.0.0.1:{port}/d.csv.gz"))
        .with_limits(limits);
    let err = drain(src).unwrap_err();
    assert!(err.contains("decompressed gzip output exceeds"), "{err}");
}

#[cfg(feature = "http")]
#[test]
fn http_max_body_rejects_oversized_response() {
    use std::io::{Read, Write};
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    std::thread::spawn(move || {
        if let Ok((mut s, _)) = listener.accept() {
            let mut buf = Vec::new();
            let mut one = [0u8; 1];
            while let Ok(1) = s.read(&mut one) {
                buf.push(one[0]);
                if buf.ends_with(b"\r\n\r\n") {
                    break;
                }
            }
            let body = b"a\n1\n2\n3\n4\n5\n6\n";
            let head = format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n", body.len());
            let _ = s.write_all(head.as_bytes());
            let _ = s.write_all(body);
        }
    });
    let src = tpt_stream_core::HttpSource::open(format!("http://127.0.0.1:{port}/d.csv"))
        .with_max_body(4);
    let err = drain(src).unwrap_err();
    assert!(err.contains("exceeds the 4-byte limit"), "{err}");
}
