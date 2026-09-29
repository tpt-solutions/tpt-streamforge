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

/// Check `bucket` against GCS bucket-naming rules before it is spliced into a
/// URL: without this a name like `b/../x` or `b?x=1` rewrites the request path
/// or query. Rules (cloud.google.com/storage/docs/buckets#naming): 3-63
/// characters (up to 222 when dotted, each dot-separated part at most 63),
/// only lowercase letters, digits, `-`, `_` and `.`, starting and ending with a
/// letter or digit, no `..`, not an IPv4 address, no `goog` prefix, no `google`.
pub(crate) fn validate_bucket_name(bucket: &str) -> Result<()> {
    let bad = |why: &str| {
        Err(crate::error::Error::Cloud(format!(
            "invalid GCS bucket name {bucket:?}: {why}"
        )))
    };
    if !(3..=222).contains(&bucket.len()) {
        return bad("length must be 3-222 characters");
    }
    if !bucket
        .bytes()
        .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || matches!(b, b'-' | b'_' | b'.'))
    {
        return bad("only lowercase letters, digits, '-', '_' and '.' are allowed");
    }
    let first = bucket.as_bytes()[0];
    let last = bucket.as_bytes()[bucket.len() - 1];
    if !(first.is_ascii_lowercase() || first.is_ascii_digit())
        || !(last.is_ascii_lowercase() || last.is_ascii_digit())
    {
        return bad("must start and end with a letter or digit");
    }
    if bucket.contains("..") {
        return bad("must not contain '..'");
    }
    if bucket.split('.').any(|part| part.len() > 63) {
        return bad("each dot-separated part must be at most 63 characters");
    }
    if bucket.parse::<std::net::Ipv4Addr>().is_ok() {
        return bad("must not look like an IP address");
    }
    if bucket.starts_with("goog") || bucket.contains("google") {
        return bad("must not start with 'goog' or contain 'google'");
    }
    Ok(())
}

/// A signed client for one GCS bucket via the S3-compatible XML API.
/// Requires an HMAC key (not a OAuth2 bearer token).
#[derive(Clone, Debug)]
pub struct GcsStore(S3Store);

impl GcsStore {
    pub fn new(bucket: &str, credentials: &CloudCredentials) -> Result<Self> {
        validate_bucket_name(bucket)?;
        let bucket_url = format!("{GCS_ENDPOINT}/{bucket}");
        Ok(GcsStore(S3Store::new(&bucket_url, credentials)?))
    }

    pub fn bucket_name(&self) -> &str {
        self.0.bucket_name()
    }

    /// Retry transient request failures (connection resets, 408/429/5xx)
    /// according to `policy`. Off by default.
    #[must_use]
    pub fn with_retry(mut self, policy: crate::httpclient::RetryPolicy) -> Self {
        self.0 = self.0.with_retry(policy);
        self
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

#[cfg(test)]
mod tests {
    use super::validate_bucket_name;

    #[test]
    fn valid_bucket_names_pass() {
        for ok in ["my-bucket", "abc", "a.b.c", "data_lake-01", "0start9"] {
            assert!(validate_bucket_name(ok).is_ok(), "{ok}");
        }
    }

    #[test]
    fn hostile_or_malformed_bucket_names_are_rejected() {
        for bad in [
            "",
            "ab",
            "b/../x",
            "b?x=1",
            "b#frag",
            "a b c",
            "UPPER",
            "-lead",
            "trail-",
            "a..b",
            "192.168.5.4",
            "goog-bucket",
            "my-google-bucket",
            "b\r\nHost: evil",
            "b@evil.com",
            "b\0c",
        ] {
            assert!(validate_bucket_name(bad).is_err(), "{bad:?}");
        }
    }

    #[test]
    fn store_constructor_validates_bucket() {
        let creds = crate::s3::CloudCredentials::new("AK", "SK");
        assert!(super::GcsStore::new("a/b", &creds).is_err());
        assert!(super::GcsStore::new("fine-bucket", &creds).is_ok());
    }
}
