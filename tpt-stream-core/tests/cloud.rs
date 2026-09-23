//! Integration tests for the S3 / GCS / Azure Blob sources and sinks.
//!
//! Two tiers:
//! 1. Offline: a tiny in-process HTTP server stands in for the object store,
//!    exercising the signing plumbing, streaming GET, PUT, and multipart
//!    upload against real sockets. No network access needed.
//! 2. Env-gated (`TPT_TEST_S3_ENDPOINT`, `TPT_TEST_AZURE_ENDPOINT`): run
//!    against LocalStack / Azurite in CI.
#![cfg(all(
    feature = "async",
    any(feature = "s3", feature = "gcs", feature = "azure")
))]

use std::io::{Read, Write};
use std::net::TcpListener;
use std::sync::mpsc as std_mpsc;
use std::thread;

use tpt_stream_core::s3::CloudCredentials;
use tpt_stream_core::{Pipeline, S3Sink, S3Source, S3Store, Source};

// ---------------------------------------------------------------------------
// In-process mock object server
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct MockRequest {
    method: String,
    path: String,
    query: String,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
}

impl MockRequest {
    fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
    }
}

struct MockResponse {
    status: u16,
    reason: &'static str,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
}

impl MockResponse {
    fn ok(body: Vec<u8>) -> Self {
        MockResponse {
            status: 200,
            reason: "OK",
            headers: Vec::new(),
            body,
        }
    }

    fn with_header(mut self, name: &str, value: &str) -> Self {
        self.headers.push((name.to_string(), value.to_string()));
        self
    }
}

/// Spawn the mock server; returns its base URL (`http://127.0.0.1:port`).
/// One request per connection (the client pool simply reconnects).
fn spawn_mock_server(handler: impl Fn(MockRequest) -> MockResponse + Send + 'static) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind mock server");
    let port = listener.local_addr().unwrap().port();
    thread::spawn(move || {
        for stream in listener.incoming() {
            let mut stream = match stream {
                Ok(s) => s,
                Err(_) => break,
            };
            let request = match read_request(&mut stream) {
                Some(r) => r,
                None => continue,
            };

            let response = handler(request);
            let mut head = format!(
                "HTTP/1.1 {} {}\r\nContent-Length: {}\r\nConnection: close\r\n",
                response.status,
                response.reason,
                response.body.len()
            );
            for (k, v) in &response.headers {
                head.push_str(&format!("{k}: {v}\r\n"));
            }
            head.push_str("\r\n");
            let _ = stream.write_all(head.as_bytes());
            let _ = stream.write_all(&response.body);
            let _ = stream.flush();
        }
    });
    format!("http://127.0.0.1:{port}")
}

/// Read one HTTP request; loops on `read` until the full body (per
/// Content-Length) has arrived.
fn read_request(stream: &mut impl Read) -> Option<MockRequest> {
    let mut buf = Vec::new();
    let mut one = [0u8; 1];
    loop {
        match stream.read(&mut one) {
            Ok(1) => {
                buf.push(one[0]);
                if buf.ends_with(b"\r\n\r\n") {
                    break;
                }
            }
            _ => return None,
        }
    }
    let head = String::from_utf8_lossy(&buf);
    let mut lines = head.split("\r\n");
    let request_line = lines.next()?;
    let mut parts = request_line.split(' ');
    let method = parts.next()?.to_string();
    let target = parts.next()?.to_string();
    let (path, query) = match target.split_once('?') {
        Some((p, q)) => (p.to_string(), q.to_string()),
        None => (target, String::new()),
    };
    let mut headers = Vec::new();
    for line in lines {
        if let Some((k, v)) = line.split_once(':') {
            headers.push((k.trim().to_string(), v.trim().to_string()));
        }
    }
    let content_length: usize = headers
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case("content-length"))
        .and_then(|(_, v)| v.parse().ok())
        .unwrap_or(0);
    let mut body = vec![0u8; content_length];
    if content_length > 0 {
        stream.read_exact(&mut body).ok()?;
    }
    Some(MockRequest {
        method,
        path,
        query,
        headers,
        body,
    })
}

fn test_creds() -> CloudCredentials {
    CloudCredentials::new("test-access", "test-secret")
}

fn sample_csv(rows: usize) -> String {
    let mut csv = String::from("id,name\n");
    for i in 0..rows {
        csv.push_str(&format!("{i},name{i}\n"));
    }
    csv
}

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
}

// ---------------------------------------------------------------------------
// Offline tests (mock server)
// ---------------------------------------------------------------------------

#[test]
fn s3_source_roundtrip_via_mock() {
    let csv_body = sample_csv(300);
    let body_clone = csv_body.clone();
    let (req_tx, req_rx) = std_mpsc::channel::<MockRequest>();
    let base = spawn_mock_server(move |req| {
        let _ = req_tx.send(req.clone());
        MockResponse::ok(body_clone.clone().into_bytes())
    });

    let store = S3Store::new(&format!("{base}/test-bucket"), &test_creds()).unwrap();
    runtime().block_on(async {
        let mut source = S3Source::open_with_chunk_size(store, "data/events.csv", 100);
        let mut batches = Vec::new();
        while let Some(batch) = source.next_batch().await.unwrap() {
            batches.push(batch);
        }
        assert_eq!(batches.len(), 3); // chunk 100, 300 rows
        assert_eq!(batches[0].num_rows(), 100);
        assert_eq!(batches[2].num_rows(), 100);
        assert_eq!(
            batches[2].cell(0, "id"),
            Some(tpt_stream_core::Value::Int32(200))
        );
        assert_eq!(
            batches[2].cell(99, "id"),
            Some(tpt_stream_core::Value::Int32(299))
        );
    });

    let req = req_rx.try_recv().expect("mock got a request");
    assert_eq!(req.method, "GET");
    assert!(req.path.contains("test-bucket/data/events.csv"));
    // SigV4 presigned query params must be present.
    assert!(req.query.contains("X-Amz-Algorithm=AWS4-HMAC-SHA256"));
    assert!(req.query.contains("X-Amz-Signature="));
}

#[test]
fn s3_sink_single_put_via_mock() {
    let (req_tx, req_rx) = std_mpsc::channel::<MockRequest>();
    let base = spawn_mock_server(move |req| {
        let _ = req_tx.send(req.clone());
        MockResponse::ok(Vec::new())
    });
    let store = S3Store::new(&format!("{base}/test-bucket"), &test_creds()).unwrap();

    runtime().block_on(async {
        let dir = tempfile::tempdir().unwrap();
        let csv_path = dir.path().join("in.csv");
        std::fs::write(&csv_path, sample_csv(50)).unwrap();
        let mut pipeline = Pipeline::new();
        pipeline
            .read_csv(csv_path.to_string_lossy())
            .sink(S3Sink::new(store, "out/results.csv"));
        let stats = pipeline.execute().await.unwrap();
        assert_eq!(stats.rows, 50);
    });

    let req = req_rx.try_recv().expect("mock got the PUT");
    assert_eq!(req.method, "PUT");
    assert!(req.path.contains("test-bucket/out/results.csv"));
    let body = String::from_utf8(req.body).unwrap();
    assert!(body.starts_with("id,name\n0,name0\n"));
    assert!(body.ends_with("49,name49\n"));
}

#[test]
fn s3_sink_multipart_via_mock() {
    let part_size = 1024 * 1024;
    // Parts must be >= 5 MiB (S3 minimum enforced by with_part_size), so
    // generate > 2 x 5 MiB to force two part uploads plus a tail.
    let effective_part = part_size.max(5 * 1024 * 1024);
    let mut csv_big = String::from("id,payload\n");
    let mut i = 0usize;
    while csv_big.len() < 2 * effective_part + 1024 {
        csv_big.push_str(&format!("{i},xxxxxxxxxxxxxxxxxxxxxxxxxxxxxx\n"));
        i += 1;
    }
    let dir = tempfile::tempdir().unwrap();
    let csv_path = dir.path().join("big.csv");
    std::fs::write(&csv_path, &csv_big).unwrap();

    let (req_tx, req_rx) = std_mpsc::channel::<MockRequest>();
    let base = spawn_mock_server(move |req| {
        let _ = req_tx.send(req.clone());
        if req.method == "POST" && req.query.contains("uploadId=") {
            // CompleteMultipartUpload
            MockResponse::ok(Vec::new())
        } else if req.method == "POST" {
            // CreateMultipartUpload
            MockResponse::ok(
                b"<InitiateMultipartUploadResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><UploadId>UPLOAD-1</UploadId></InitiateMultipartUploadResult>"
                    .to_vec(),
            )
        } else if req.method == "PUT" && req.query.contains("partNumber=") {
            MockResponse::ok(Vec::new()).with_header("ETag", "\"etag-mock\"")
        } else {
            MockResponse::ok(Vec::new())
        }
    });
    let store = S3Store::new(&format!("{base}/test-bucket"), &test_creds()).unwrap();

    runtime().block_on(async {
        let mut pipeline = Pipeline::new();
        pipeline
            .read_csv(csv_path.to_string_lossy())
            .sink(S3Sink::new(store, "out/big.csv").with_part_size(part_size));
        let stats = pipeline.execute().await.unwrap();
        assert!(stats.rows > 0);
    });

    let mut requests = Vec::new();
    while let Ok(r) = req_rx.try_recv() {
        requests.push(r);
    }
    // CreateMultipartUpload is the only POST with an "uploads" query param
    // (rusty-s3 signs the bare key); Complete carries "uploadId=".
    let created = requests
        .iter()
        .any(|r| r.method == "POST" && r.query.contains("uploads="));
    let parts: Vec<&MockRequest> = requests
        .iter()
        .filter(|r| r.method == "PUT" && r.query.contains("partNumber="))
        .collect();
    let completed = requests.iter().any(|r| {
        r.method == "POST"
            && r.query.contains("uploadId=")
            && String::from_utf8_lossy(&r.body).contains("CompleteMultipartUpload")
    });
    assert!(
        created,
        "expected an InitiateMultipartUpload POST; got {:?}",
        requests
            .iter()
            .map(|r| (&r.method, r.query.clone()))
            .collect::<Vec<_>>()
    );
    assert!(
        parts.len() >= 2,
        "expected >= 2 part uploads, got {}",
        parts.len()
    );
    assert!(completed, "expected a CompleteMultipartUpload POST");
}

#[test]
fn s3_missing_object_surfaces_error() {
    let (req_tx, _req_rx) = std_mpsc::channel::<MockRequest>();
    let base = spawn_mock_server(move |req| {
        let _ = req_tx.send(req.clone());
        MockResponse {
            status: 404,
            reason: "Not Found",
            headers: Vec::new(),
            body: b"<Error><Code>NoSuchKey</Code></Error>".to_vec(),
        }
    });
    let store = S3Store::new(&format!("{base}/test-bucket"), &test_creds()).unwrap();
    runtime().block_on(async {
        let mut source = S3Source::open(store, "nope.csv");
        let err = source.next_batch().await.unwrap_err();
        assert!(err.to_string().contains("404"), "unexpected error: {err}");
    });
}

#[cfg(feature = "gcs")]
#[test]
fn gcs_store_parses_bucket() {
    let creds = test_creds();
    let store = tpt_stream_core::GcsStore::new("my-bucket", &creds).unwrap();
    assert_eq!(store.bucket_name(), "my-bucket");
}

#[cfg(feature = "azure")]
#[test]
fn azure_roundtrip_via_mock() {
    use base64::Engine as _;
    use tpt_stream_core::{AzureBlobSink, AzureBlobSource, AzureBlobStore, AzureCredentials};

    let creds = AzureCredentials::new(
        "devstoreaccount1",
        base64::engine::general_purpose::STANDARD.encode([7u8; 32]),
    );
    let (req_tx, req_rx) = std_mpsc::channel::<MockRequest>();
    let stored = std::sync::Arc::new(std::sync::Mutex::new(Vec::<u8>::new()));
    let base = spawn_mock_server(move |req| {
        let response = if req.method == "GET" {
            let data = stored.lock().unwrap().clone();
            MockResponse::ok(data)
        } else {
            *stored.lock().unwrap() = req.body.clone();
            MockResponse::ok(Vec::new())
        };
        let _ = req_tx.send(req.clone());
        response
    });
    // Path-style URL (Azurite layout): account comes from the path.
    let store = AzureBlobStore::new(&format!("{base}/devstoreaccount1"), "c", &creds).unwrap();

    // Single-PUT write path.
    runtime().block_on(async {
        let dir = tempfile::tempdir().unwrap();
        let csv_path = dir.path().join("in.csv");
        std::fs::write(&csv_path, sample_csv(10)).unwrap();
        let mut pipeline = Pipeline::new();
        pipeline
            .read_csv(csv_path.to_string_lossy())
            .sink(AzureBlobSink::new(store.clone(), "data/out.csv"));
        pipeline.execute().await.unwrap();
    });

    let put = req_rx.try_recv().expect("mock got the PUT");
    assert_eq!(put.method, "PUT");
    assert_eq!(put.header("x-ms-blob-type"), Some("BlockBlob"));
    let auth = put
        .header("authorization")
        .expect("signed request")
        .to_string();
    assert!(
        auth.starts_with("SharedKey devstoreaccount1:"),
        "auth: {auth}"
    );
    assert!(put.header("x-ms-date").is_some());
    assert!(String::from_utf8_lossy(&put.body).starts_with("id,name\n"));

    // Read path streams the stored bytes back through the pipeline.
    runtime().block_on(async {
        let mut source = AzureBlobSource::open_with_chunk_size(store, "data/out.csv", 4);
        let mut batches = Vec::new();
        while let Some(batch) = source.next_batch().await.unwrap() {
            batches.push(batch);
        }
        assert_eq!(batches.len(), 3); // 10 rows / chunk 4
        assert_eq!(batches[0].num_rows(), 4);
    });
}

#[cfg(feature = "azure")]
#[test]
fn azure_credential_key_must_be_base64() {
    use base64::Engine as _;
    use tpt_stream_core::AzureCredentials;
    let good = AzureCredentials::new(
        "acct",
        base64::engine::general_purpose::STANDARD.encode([7u8; 32]),
    );
    assert!(tpt_stream_core::AzureBlobStore::new("http://127.0.0.1:1/acct", "c", &good).is_ok());
    let bad = AzureCredentials::new("acct", "not base64 !!!");
    assert!(tpt_stream_core::AzureBlobStore::new("http://127.0.0.1:1/acct", "c", &bad).is_err());
}

// ---------------------------------------------------------------------------
// Env-gated tests (LocalStack / Azurite in CI)
// ---------------------------------------------------------------------------

#[test]
fn s3_localstack_roundtrip() {
    let Ok(endpoint) = std::env::var("TPT_TEST_S3_ENDPOINT") else {
        eprintln!("skipping: TPT_TEST_S3_ENDPOINT not set");
        return;
    };
    let creds = CloudCredentials::new(
        std::env::var("TPT_TEST_S3_ACCESS_KEY").unwrap_or_else(|_| "test".into()),
        std::env::var("TPT_TEST_S3_SECRET_KEY").unwrap_or_else(|_| "test".into()),
    );
    let bucket = std::env::var("TPT_TEST_S3_BUCKET").unwrap_or_else(|_| "tpt-test".into());
    let bucket_url = format!("{endpoint}/{bucket}");
    let dir = tempfile::tempdir().unwrap();
    let csv_path = dir.path().join("in.csv");
    std::fs::write(&csv_path, sample_csv(1000)).unwrap();

    runtime().block_on(async {
        let mut write = Pipeline::new();
        write
            .with_chunk_size(200)
            .read_csv(csv_path.to_string_lossy())
            .write_s3(&bucket_url, "out/people.csv", &creds)
            .unwrap();
        let stats = write.execute().await.unwrap();
        assert_eq!(stats.rows, 1000);

        let mut read = Pipeline::new();
        read.read_s3(&bucket_url, "out/people.csv", &creds)
            .unwrap()
            .filter_expr("id >= 990")
            .write_csv(dir.path().join("back.csv").to_string_lossy());
        let stats = read.execute().await.unwrap();
        // `stats.rows` counts rows read from the source, before filtering
        // (see `sqlite_pipeline_filter_stats`); the filtered count only
        // shows up in what actually reaches the sink.
        assert_eq!(stats.rows, 1000);
    });

    let back = std::fs::read_to_string(dir.path().join("back.csv")).unwrap();
    assert_eq!(back.lines().count(), 11); // header + 10 filtered rows
    assert!(back.contains("990,name990"));
}

#[cfg(feature = "azure")]
#[test]
fn azure_azurite_roundtrip() {
    use tpt_stream_core::AzureCredentials;
    let Ok(endpoint) = std::env::var("TPT_TEST_AZURE_ENDPOINT") else {
        eprintln!("skipping: TPT_TEST_AZURE_ENDPOINT not set");
        return;
    };
    let creds = AzureCredentials::from_env().unwrap_or_else(|_| {
        // Azurite's well-known public dev credentials.
        AzureCredentials::new(
            "devstoreaccount1",
            "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==",
        )
    });
    let container = std::env::var("TPT_TEST_AZURE_CONTAINER").unwrap_or_else(|_| "tpt-test".into());
    let store = tpt_stream_core::AzureBlobStore::new(&endpoint, &container, &creds).unwrap();
    let dir = tempfile::tempdir().unwrap();
    let csv_path = dir.path().join("in.csv");
    std::fs::write(&csv_path, sample_csv(500)).unwrap();

    runtime().block_on(async {
        let mut write = Pipeline::new();
        write
            .with_chunk_size(120)
            .read_csv(csv_path.to_string_lossy())
            .write_azure_blob(store.clone(), "out/people.csv");
        let stats = write.execute().await.unwrap();
        assert_eq!(stats.rows, 500);

        let mut read = Pipeline::new();
        read.read_azure_blob(store, "out/people.csv")
            .filter_expr("id >= 495")
            .write_csv(dir.path().join("back.csv").to_string_lossy());
        let stats = read.execute().await.unwrap();
        // `stats.rows` counts rows read from the source, before filtering
        // (see `sqlite_pipeline_filter_stats`); the filtered count only
        // shows up in what actually reaches the sink.
        assert_eq!(stats.rows, 500);
    });

    let back = std::fs::read_to_string(dir.path().join("back.csv")).unwrap();
    assert_eq!(back.lines().count(), 6); // header + 5 filtered rows
    assert!(back.contains("495,name495"));
}
