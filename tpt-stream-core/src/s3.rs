//! S3-compatible object storage source and sink (feature `s3`).
//!
//! Uses [`rusty_s3`](https://docs.rs/rusty-s3) (sans-IO AWS SigV4 signing)
//! plus the in-house `httpclient` (rustls TLS) transport — a minimal
//! dependency footprint compared to a full AWS SDK. Works with AWS S3, MinIO,
//! LocalStack, Cloudflare R2, and any other S3-compatible endpoint.
//!
//! ```no_run
//! # async fn run() -> tpt_stream_core::Result<()> {
//! use tpt_stream_core::{CloudCredentials, Pipeline};
//! let creds = CloudCredentials::from_env()?;
//! let mut pipeline = Pipeline::new();
//! pipeline
//!     .read_s3("https://s3.us-east-1.amazonaws.com/my-bucket", "in/events.csv", &creds)?
//!     .filter_expr("amount > 0")
//!     .write_s3("https://s3.us-east-1.amazonaws.com/my-bucket", "out/events.csv", &creds)?;
//! pipeline.execute().await?;
//! # Ok(())
//! # }
//! ```
//!
//! The data format is chosen from the object key's extension: `.csv`
//! (default), `.jsonl`/`.ndjson`, `.json` (array), `.tptcol` (columnar).
//! Small objects are uploaded with a single `PUT`; larger ones use the S3
//! multipart upload protocol with a configurable part size, so memory stays
//! bounded while streaming.

use crate::cloud::{decode_object_stream, BatchEncoder, CloudFormat};
use crate::error::{Error, Result};
use crate::source::{BatchTx, Source, StreamingReader};
use crate::table::RecordBatch;
use rusty_s3::actions::{CreateMultipartUpload, GetObject, PutObject, S3Action};
use rusty_s3::{Bucket, Credentials, UrlStyle};
use std::io::BufRead;
use std::time::Duration;

/// Presigned requests live for one hour; each request is signed just before
/// it is sent.
const SIGN_TTL: Duration = Duration::from_secs(3600);

// ---------------------------------------------------------------------------
// Credentials
// ---------------------------------------------------------------------------

/// Static or session credentials for an S3-compatible endpoint (also used
/// for Google Cloud Storage's S3-compatible XML API).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CloudCredentials {
    pub access_key: String,
    pub secret_key: String,
    pub session_token: Option<String>,
}

impl CloudCredentials {
    pub fn new(access_key: impl Into<String>, secret_key: impl Into<String>) -> Self {
        CloudCredentials {
            access_key: access_key.into(),
            secret_key: secret_key.into(),
            session_token: None,
        }
    }

    /// Credentials with an STS session token (temporary AWS credentials).
    pub fn with_session_token(
        access_key: impl Into<String>,
        secret_key: impl Into<String>,
        session_token: impl Into<String>,
    ) -> Self {
        CloudCredentials {
            access_key: access_key.into(),
            secret_key: secret_key.into(),
            session_token: Some(session_token.into()),
        }
    }

    /// Read `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, and (optionally)
    /// `AWS_SESSION_TOKEN` from the environment.
    pub fn from_env() -> Result<Self> {
        let access_key = std::env::var("AWS_ACCESS_KEY_ID")
            .map_err(|_| Error::Cloud("AWS_ACCESS_KEY_ID not set".into()))?;
        let secret_key = std::env::var("AWS_SECRET_ACCESS_KEY")
            .map_err(|_| Error::Cloud("AWS_SECRET_ACCESS_KEY not set".into()))?;
        let session_token = std::env::var("AWS_SESSION_TOKEN").ok();
        Ok(CloudCredentials {
            access_key,
            secret_key,
            session_token,
        })
    }

    fn to_rusty(&self) -> Credentials {
        match &self.session_token {
            Some(token) => Credentials::new_with_token(
                self.access_key.clone(),
                self.secret_key.clone(),
                token.clone(),
            ),
            None => Credentials::new(self.access_key.clone(), self.secret_key.clone()),
        }
    }
}

// ---------------------------------------------------------------------------
// Store (signed HTTP plumbing)
// ---------------------------------------------------------------------------

/// A signed client for one S3 bucket. Cheap to clone.
#[derive(Clone)]
pub struct S3Store {
    pub(crate) bucket: Bucket,
    pub(crate) credentials: Credentials,
    pub(crate) agent: crate::httpclient::Agent,
    pub(crate) bucket_url: String,
}

impl std::fmt::Debug for S3Store {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("S3Store")
            .field("bucket_url", &self.bucket_url)
            .field("bucket", &self.bucket.name())
            .field("region", &self.bucket.region())
            .finish()
    }
}

impl S3Store {
    /// Build a client from a bucket URL and credentials. Both URL styles are
    /// accepted:
    ///
    /// - path style: `https://s3.us-east-1.amazonaws.com/my-bucket`
    /// - virtual-host style: `https://my-bucket.s3.us-east-1.amazonaws.com`
    ///
    /// The SigV4 region defaults to `us-east-1` (also accepted by MinIO,
    /// LocalStack, and R2); override with [`S3Store::with_region`].
    pub fn new(bucket_url: &str, credentials: &CloudCredentials) -> Result<Self> {
        let url: url::Url = url::Url::parse(bucket_url)
            .map_err(|e| Error::Cloud(format!("invalid bucket URL {bucket_url:?}: {e}")))?;
        let bucket_name = match url.path().trim_matches('/') {
            "" => url
                .host_str()
                .and_then(|host| host.split('.').next())
                .ok_or_else(|| Error::Cloud(format!("bucket URL {bucket_url:?} has no host")))?
                .to_string(),
            path => match path.split('/').next() {
                Some(name) if !name.is_empty() => name.to_string(),
                _ => {
                    return Err(Error::Cloud(format!(
                        "cannot read bucket name from URL path {path:?}"
                    )))
                }
            },
        };
        let path_style = !url.path().trim_matches('/').is_empty();
        let bucket = Bucket::new(
            url,
            if path_style {
                UrlStyle::Path
            } else {
                UrlStyle::VirtualHost
            },
            bucket_name,
            "us-east-1",
        )
        .map_err(|e| Error::Cloud(format!("invalid bucket URL {bucket_url:?}: {e}")))?;
        Ok(S3Store {
            bucket,
            credentials: credentials.to_rusty(),
            agent: crate::httpclient::Agent::new(),
            bucket_url: bucket_url.to_string(),
        })
    }

    /// Override the SigV4 signing region (defaults to `us-east-1`).
    pub fn with_region(mut self, region: &str) -> Self {
        let base_url = self.bucket.base_url().clone();
        let path_style = self.is_path_style();
        let name = self.bucket.name().to_string();
        self.bucket = Bucket::new(
            base_url,
            if path_style {
                UrlStyle::Path
            } else {
                UrlStyle::VirtualHost
            },
            name,
            region.to_string(),
        )
        .expect("re-validating a known-good bucket URL");
        self
    }

    fn is_path_style(&self) -> bool {
        self.bucket.base_url().path() != "/"
    }

    pub fn bucket_name(&self) -> &str {
        self.bucket.name()
    }

    /// `GET` an object, returning a streaming reader over its body.
    pub fn read_object(&self, key: &str) -> Result<Box<dyn BufRead + Send>> {
        let action = GetObject::new(&self.bucket, Some(&self.credentials), key);
        let signed = action.sign(SIGN_TTL);
        let response = self
            .agent
            .get(signed.as_str())
            .call()
            .map_err(|e| http_error("s3 get", key, e))?;
        Ok(Box::new(std::io::BufReader::with_capacity(
            64 * 1024,
            response.into_reader(),
        )))
    }

    /// `PUT` an in-memory payload as one object (fine up to a few hundred MB).
    pub fn write_object(&self, key: &str, payload: Vec<u8>) -> Result<()> {
        let action = PutObject::new(&self.bucket, Some(&self.credentials), key);
        let signed = action.sign(SIGN_TTL);
        self.agent
            .put(signed.as_str())
            .send_bytes(&payload)
            .map_err(|e| http_error("s3 put", key, e))?;
        Ok(())
    }

    /// Start a multipart upload and return its upload id.
    pub(crate) fn create_multipart(&self, key: &str) -> Result<String> {
        let action = CreateMultipartUpload::new(&self.bucket, Some(&self.credentials), key);
        let signed = action.sign(SIGN_TTL);
        let response = self
            .agent
            .post(signed.as_str())
            .call()
            .map_err(|e| http_error("s3 create multipart upload", key, e))?;
        let body = response.into_string().map_err(|e| {
            Error::Cloud(format!(
                "s3 create multipart upload {key:?}: read body: {e}"
            ))
        })?;
        CreateMultipartUpload::parse_response(&body)
            .map(|r| r.upload_id().to_string())
            .map_err(|e| {
                Error::Cloud(format!(
                    "s3 create multipart upload {key:?}: parse response: {e}"
                ))
            })
    }

    /// Upload one part of a multipart upload; returns its ETag.
    pub(crate) fn upload_part(
        &self,
        key: &str,
        upload_id: &str,
        part_number: u16,
        data: Vec<u8>,
    ) -> Result<String> {
        use rusty_s3::actions::UploadPart;
        let action = UploadPart::new(
            &self.bucket,
            Some(&self.credentials),
            key,
            part_number,
            upload_id,
        );
        let signed = action.sign(SIGN_TTL);
        let response = self
            .agent
            .put(signed.as_str())
            .send_bytes(&data)
            .map_err(|e| http_error("s3 upload part", key, e))?;
        response
            .header("ETag")
            .map(|s| s.to_string())
            .ok_or_else(|| {
                Error::Cloud(format!(
                    "s3 upload part {key:?} #{part_number}: response has no ETag header"
                ))
            })
    }

    /// Complete a multipart upload with the collected part ETags.
    pub(crate) fn complete_multipart(
        &self,
        key: &str,
        upload_id: &str,
        etags: &[String],
    ) -> Result<()> {
        use rusty_s3::actions::CompleteMultipartUpload;
        let action = CompleteMultipartUpload::new(
            &self.bucket,
            Some(&self.credentials),
            key,
            upload_id,
            etags.iter().map(|s| s.as_str()),
        );
        let signed = action.sign(SIGN_TTL);
        let body = action.body();
        self.agent
            .post(signed.as_str())
            .set("Content-Type", "application/xml")
            .send_bytes(body.as_bytes())
            .map_err(|e| http_error("s3 complete multipart upload", key, e))?;
        Ok(())
    }

    /// Abort a multipart upload, discarding the uploaded parts.
    pub(crate) fn abort_multipart(&self, key: &str, upload_id: &str) {
        use rusty_s3::actions::AbortMultipartUpload;
        let action =
            AbortMultipartUpload::new(&self.bucket, Some(&self.credentials), key, upload_id);
        let signed = action.sign(SIGN_TTL);
        let _ = self
            .agent
            .delete(signed.as_str())
            .call()
            .map_err(|e| http_error("s3 abort multipart upload", key, e));
    }
}

fn http_error(op: &str, key: &str, err: crate::httpclient::Error) -> Error {
    match err {
        crate::httpclient::Error::Status(code, response) => {
            let reason = response
                .into_string()
                .map(|body| {
                    let snippet: String = body.chars().take(200).collect();
                    format!("status {code}: {snippet}")
                })
                .unwrap_or_else(|_| format!("status {code}"));
            Error::Cloud(format!("{op} {key:?}: {reason}"))
        }
        crate::httpclient::Error::Transport(t) => {
            Error::Cloud(format!("{op} {key:?}: transport: {t}"))
        }
    }
}

// ---------------------------------------------------------------------------
// Source
// ---------------------------------------------------------------------------

/// Streams an S3 object as pipeline batches. Format comes from the key
/// extension (`.csv`, `.jsonl`/`.ndjson`, `.json`, `.tptcol`).
pub struct S3Source {
    reader: StreamingReader,
    pending: Option<std::result::Result<RecordBatch, Error>>,
    chunk_rows: usize,
}

impl S3Source {
    pub fn open(store: S3Store, key: impl Into<String>) -> Self {
        S3Source::open_with_chunk_size(store, key, crate::DEFAULT_CHUNK_ROWS)
    }

    pub fn open_with_chunk_size(store: S3Store, key: impl Into<String>, chunk_rows: usize) -> Self {
        let key = key.into();
        let format = CloudFormat::detect(&key);
        let (reader, handle) = StreamingReader::spawn(move |tx: &BatchTx| {
            cloud_read(tx, &store, &key, format, chunk_rows);
        });
        let _ = handle;
        S3Source {
            reader,
            pending: None,
            chunk_rows,
        }
    }

    pub fn with_chunk_size(mut self, rows: usize) -> Self {
        self.chunk_rows = rows;
        self
    }
}

impl std::fmt::Debug for S3Source {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("S3Source")
            .field("chunk_rows", &self.chunk_rows)
            .finish()
    }
}

/// Shared read path for S3-style stores (S3, GCS interop, MinIO, ...).
pub(crate) fn cloud_read(
    tx: &BatchTx,
    store: &S3Store,
    key: &str,
    format: CloudFormat,
    chunk_rows: usize,
) {
    match store.read_object(key) {
        Ok(body) => decode_object_stream(tx, body, format, chunk_rows, &Default::default()),
        Err(e) => {
            let _ = tx.send(Err(e));
        }
    }
}

#[async_trait::async_trait]
impl Source for S3Source {
    async fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        if self.reader.done {
            return Ok(None);
        }
        if let Some(result) = self.pending.take() {
            self.reader.done = true;
            return match result {
                Ok(b) => Ok(Some(b)),
                Err(e) => Err(e),
            };
        }
        match self.reader.next().await {
            Some(Ok(batch)) => Ok(Some(batch)),
            Some(Err(e)) => {
                self.reader.done = true;
                Err(e)
            }
            None => {
                self.reader.done = true;
                Ok(None)
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Sink
// ---------------------------------------------------------------------------

/// Uploads pipeline batches to one S3 object. Buffers up to `part_size`
/// bytes (default 64 MiB, minimum 5 MiB per the S3 API) and streams parts
/// with the multipart upload protocol; a payload that fits in one part is
/// uploaded with a single plain `PUT` on `finish()`.
pub struct S3Sink {
    store: S3Store,
    key: String,
    format: CloudFormat,
    part_size: usize,
    encoder: BatchEncoder,
    buffer: Vec<u8>,
    upload: Option<MultipartState>,
    bytes_out: u64,
}

#[derive(Debug)]
struct MultipartState {
    upload_id: String,
    etags: Vec<String>,
    next_part: u16,
}

impl S3Sink {
    pub fn new(store: S3Store, key: impl Into<String>) -> Self {
        let key = key.into();
        let format = CloudFormat::detect(&key);
        S3Sink {
            store,
            key,
            format,
            part_size: 64 * 1024 * 1024,
            encoder: BatchEncoder::new(format),
            buffer: Vec::with_capacity(4 * 1024 * 1024),
            upload: None,
            bytes_out: 0,
        }
    }

    /// Buffer size before a part is uploaded. Values below 5 MiB are raised
    /// to 5 MiB (the S3 minimum for non-final parts); values above 5 GiB are
    /// lowered because a non-multipart `PUT` is capped at 5 GiB.
    pub fn with_part_size(mut self, bytes: usize) -> Self {
        const MIIB: usize = 1024 * 1024;
        const GIB5: usize = 5 * 1024 * MIIB;
        self.part_size = bytes.clamp(5 * MIIB, GIB5);
        self
    }

    pub fn bytes_out(&self) -> u64 {
        self.bytes_out
    }

    fn flush_part(&mut self) -> Result<()> {
        if self.buffer.is_empty() {
            return Ok(());
        }
        if self.upload.is_none() {
            let upload_id = self.store.create_multipart(&self.key)?;
            self.upload = Some(MultipartState {
                upload_id,
                etags: Vec::new(),
                next_part: 1,
            });
        }
        let state = self.upload.as_mut().expect("upload started");
        let etag = self.store.upload_part(
            &self.key,
            &state.upload_id,
            state.next_part,
            std::mem::take(&mut self.buffer),
        )?;
        state.etags.push(etag);
        state.next_part += 1;
        Ok(())
    }
}

impl std::fmt::Debug for S3Sink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("S3Sink")
            .field("key", &self.key)
            .field("format", &self.format)
            .field("part_size", &self.part_size)
            .field("bytes_out", &self.bytes_out)
            .finish()
    }
}

#[async_trait::async_trait]
impl crate::sink::Sink for S3Sink {
    async fn write_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        let before = self.buffer.len();
        self.encoder.encode(&mut self.buffer, batch)?;
        self.bytes_out += (self.buffer.len() - before) as u64;
        if self.buffer.len() >= self.part_size {
            self.flush_part()?;
        }
        Ok(())
    }

    async fn finish(&mut self) -> Result<()> {
        let result = if self.upload.is_some() {
            // Multipart in flight: push the tail as the final part (an empty
            // tail contributes no part), then commit the part list. Abort on
            // failure so no orphaned parts are left on the service.
            self.flush_part().and_then(|()| {
                let state = self.upload.as_ref().expect("multipart in flight");
                self.store
                    .complete_multipart(&self.key, &state.upload_id, &state.etags)
            })
        } else {
            let mut payload = std::mem::take(&mut self.buffer);
            self.encoder.finish(&mut payload);
            self.store.write_object(&self.key, payload)
        };
        if let Err(e) = result {
            if let Some(state) = self.upload.take() {
                self.store.abort_multipart(&self.key, &state.upload_id);
            }
            return Err(e);
        }
        let _ = self.upload.take();
        self.buffer = Vec::with_capacity(self.part_size.min(4 * 1024 * 1024));
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bucket_url_parsing_both_styles() {
        let creds = CloudCredentials::new("k", "s");
        let path_style = S3Store::new("https://s3.us-east-1.amazonaws.com/my-bucket", &creds)
            .expect("path style url");
        assert_eq!(path_style.bucket_name(), "my-bucket");

        let vhost = S3Store::new("https://my-bucket.s3.us-east-1.amazonaws.com", &creds)
            .expect("virtual-host url");
        assert_eq!(vhost.bucket_name(), "my-bucket");
    }

    #[test]
    fn bucket_url_rejects_garbage() {
        let creds = CloudCredentials::new("k", "s");
        assert!(S3Store::new("not a url", &creds).is_err());
        assert!(S3Store::new("ftp://example.com/bucket", &creds).is_err());
    }

    #[test]
    fn with_region_preserves_bucket() {
        let creds = CloudCredentials::new("k", "s");
        let store = S3Store::new("https://s3.eu-west-1.amazonaws.com/bkt", &creds)
            .unwrap()
            .with_region("eu-west-1");
        assert_eq!(store.bucket.region(), "eu-west-1");
        assert_eq!(store.bucket_name(), "bkt");
    }
}
