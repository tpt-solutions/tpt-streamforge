//! Google Cloud Storage source and sink (feature `gcs`).
//!
//! GCS exposes an S3-compatible XML API: with an *HMAC key* (created per
//! service account in the Google Cloud console, "Interoperability" tab) the
//! exact same SigV4 request signing works against
//! `https://storage.googleapis.com`. This module is a thin configuration of
//! the S3 client, so no extra Google SDK dependency is pulled in.
//!
//! ```no_run
//! # async fn run() -> tpt_stream_core::Result<()> {
//! use tpt_stream_core::{CloudCredentials, Pipeline};
//! let creds = CloudCredentials::new("GOOG1E...,hmac-access", "hmac-secret");
//! let mut pipeline = Pipeline::new();
//! pipeline
//!     .read_gcs("my-bucket", "in/events.csv", &creds)?
//!     .write_gcs("my-bucket", "out/events.jsonl", &creds)?;
//! pipeline.execute().await?;
//! # Ok(())
//! # }
//! ```

use crate::error::Result;
use crate::s3::{CloudCredentials, S3Sink, S3Source, S3Store};
use crate::source::Source;
use crate::table::RecordBatch;
use std::io::BufRead;

const GCS_ENDPOINT: &str = "https://storage.googleapis.com";

/// A signed client for one GCS bucket via the S3-compatible XML API.
/// Requires an HMAC key (not a OAuth2 bearer token).
#[derive(Clone, Debug)]
pub struct GcsStore(S3Store);

impl GcsStore {
    pub fn new(bucket: &str, credentials: &CloudCredentials) -> Result<Self> {
        let bucket_url = format!("{GCS_ENDPOINT}/{bucket}");
        Ok(GcsStore(S3Store::new(&bucket_url, credentials)?))
    }

    pub fn bucket_name(&self) -> &str {
        self.0.bucket_name()
    }

    /// Stream an object body with a `GET`.
    pub fn read_object(&self, key: &str) -> Result<Box<dyn BufRead + Send>> {
        self.0.read_object(key)
    }

    /// `PUT` an in-memory payload as one object.
    pub fn write_object(&self, key: &str, payload: Vec<u8>) -> Result<()> {
        self.0.write_object(key, payload)
    }
}

/// Streams a GCS object as pipeline batches. Format comes from the key
/// extension (`.csv`, `.jsonl`/`.ndjson`, `.json`, `.tptcol`).
pub struct GcsSource {
    inner: S3Source,
}

impl GcsSource {
    pub fn open(store: GcsStore, key: impl Into<String>) -> Self {
        GcsSource::open_with_chunk_size(store, key, crate::DEFAULT_CHUNK_ROWS)
    }

    pub fn open_with_chunk_size(
        store: GcsStore,
        key: impl Into<String>,
        chunk_rows: usize,
    ) -> Self {
        GcsSource {
            inner: S3Source::open_with_chunk_size(store.0, key, chunk_rows),
        }
    }

    pub fn with_chunk_size(mut self, rows: usize) -> Self {
        self.inner = self.inner.with_chunk_size(rows);
        self
    }
}

impl std::fmt::Debug for GcsSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GcsSource")
            .field("inner", &self.inner)
            .finish()
    }
}

#[async_trait::async_trait]
impl Source for GcsSource {
    async fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        self.inner.next_batch().await
    }
}

/// Uploads pipeline batches to one GCS object (multipart for large streams,
/// single `PUT` for small ones — see [`S3Sink`]).
pub struct GcsSink(S3Sink);

impl GcsSink {
    pub fn new(store: GcsStore, key: impl Into<String>) -> Self {
        GcsSink(S3Sink::new(store.0, key))
    }

    pub fn with_part_size(mut self, bytes: usize) -> Self {
        self.0 = self.0.with_part_size(bytes);
        self
    }

    pub fn bytes_out(&self) -> u64 {
        self.0.bytes_out()
    }
}

impl std::fmt::Debug for GcsSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GcsSink").field("inner", &self.0).finish()
    }
}

#[async_trait::async_trait]
impl crate::sink::Sink for GcsSink {
    async fn write_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        self.0.write_batch(batch).await
    }

    async fn finish(&mut self) -> Result<()> {
        self.0.finish().await
    }
}
