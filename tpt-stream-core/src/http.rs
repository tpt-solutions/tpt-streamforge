//! HTTP(S) source (feature `http`): stream a plain URL as pipeline batches.
//!
//! Reuses the `ureq` (rustls TLS) transport from the cloud modules, so no
//! new dependencies are pulled in beyond `url`. The data format comes from
//! the URL path's extension (`.csv`, `.jsonl`/`.ndjson`, `.json`,
//! `.tptcol`), defaulting to CSV; `.gz` URLs are decompressed with the
//! `gzip` feature.

use crate::cloud::{decode_object_stream, CloudFormat};
use crate::error::{Error, Result};
use crate::source::{BatchTx, ErrorPolicy, Source, StreamingReader};
use crate::table::RecordBatch;
use std::io::BufRead;

/// Streams an HTTP(S) resource as pipeline batches.
pub struct HttpSource {
    reader: Option<StreamingReader>,
    chunk_rows: usize,
    url: String,
    policy: ErrorPolicy,
}

impl HttpSource {
    pub fn open(url: impl Into<String>) -> Self {
        HttpSource::open_with_chunk_size(url, crate::DEFAULT_CHUNK_ROWS)
    }

    pub fn open_with_chunk_size(url: impl Into<String>, chunk_rows: usize) -> Self {
        HttpSource {
            reader: None,
            url: url.into(),
            chunk_rows,
            policy: ErrorPolicy::default(),
        }
    }

    pub fn with_chunk_size(mut self, rows: usize) -> Self {
        self.chunk_rows = rows;
        self
    }

    /// Set how malformed rows are handled.
    pub fn with_error_policy(mut self, policy: ErrorPolicy) -> Self {
        self.policy = policy;
        self
    }

    fn spawn_reader(&mut self) -> &mut StreamingReader {
        if self.reader.is_none() {
            let url = self.url.clone();
            let chunk_rows = self.chunk_rows;
            let policy = self.policy.clone();
            let format = CloudFormat::detect(&url);
            let (reader, handle) = StreamingReader::spawn(move |tx: &BatchTx| {
                http_read(tx, &url, format, chunk_rows, &policy);
            });
            let _ = handle;
            self.reader = Some(reader);
        }
        self.reader.as_mut().expect("just spawned")
    }
}

impl std::fmt::Debug for HttpSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HttpSource")
            .field("url", &self.url)
            .field("chunk_rows", &self.chunk_rows)
            .finish()
    }
}

fn http_read(
    tx: &BatchTx,
    url: &str,
    format: CloudFormat,
    chunk_rows: usize,
    policy: &ErrorPolicy,
) {
    let agent = ureq::AgentBuilder::new().build();
    let response = match agent.get(url).call() {
        Ok(r) => r,
        Err(ureq::Error::Status(code, resp)) => {
            let _ = tx.send(Err(Error::Cloud(format!(
                "http get {url:?}: status {code}: {}",
                resp.into_string()
                    .map(|b| b.chars().take(200).collect::<String>())
                    .unwrap_or_default()
            ))));
            return;
        }
        Err(ureq::Error::Transport(t)) => {
            let _ = tx.send(Err(Error::Cloud(format!("http get {url:?}: {t}"))));
            return;
        }
    };
    #[cfg(feature = "gzip")]
    let body: Box<dyn BufRead + Send> = {
        // Decompress transparently when the URL or the response says gzip.
        let content_gzipped = url.ends_with(".gz")
            || response
                .header("Content-Encoding")
                .is_some_and(|e| e.contains("gzip"));
        let reader = response.into_reader();
        if content_gzipped {
            Box::new(std::io::BufReader::with_capacity(
                64 * 1024,
                flate2::read::MultiGzDecoder::new(reader),
            ))
        } else {
            Box::new(std::io::BufReader::with_capacity(64 * 1024, reader))
        }
    };
    #[cfg(not(feature = "gzip"))]
    let body: Box<dyn BufRead + Send> = Box::new(std::io::BufReader::with_capacity(
        64 * 1024,
        response.into_reader(),
    ));

    decode_object_stream(tx, body, format, chunk_rows, policy);
}

#[async_trait::async_trait]
impl Source for HttpSource {
    async fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        if self.reader.is_none() {
            self.spawn_reader();
        }
        let reader = self.reader.as_mut().expect("spawned above");
        if reader.done {
            return Ok(None);
        }
        match reader.next().await {
            Some(Ok(batch)) => Ok(Some(batch)),
            Some(Err(e)) => {
                reader.done = true;
                Err(e)
            }
            None => {
                reader.done = true;
                Ok(None)
            }
        }
    }
}
