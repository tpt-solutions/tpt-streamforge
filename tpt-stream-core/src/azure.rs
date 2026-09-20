//! Azure Blob Storage source and sink (feature `azure`).
//!
//! Talks to the Blob service REST API directly, using Shared Key
//! authorization (HMAC-SHA256 over a canonical string) — the same scheme
//! Azure Storage SDKs use, implemented on the in-house `httpclient` +
//! `hmac`/`sha2`/`base64` with no Azure SDK dependency. Works against real
//! Azure Storage and the Azurite emulator.
//!
//! ```no_run
//! # async fn run() -> tpt_stream_core::Result<()> {
//! use tpt_stream_core::{AzureBlobStore, AzureCredentials, Pipeline};
//! let creds = AzureCredentials::from_env()?;
//! let store = AzureBlobStore::new("https://myaccount.blob.core.windows.net", "container", &creds)?;
//! let mut pipeline = Pipeline::new();
//! pipeline
//!     .read_azure_blob(store.clone(), "in/events.csv")
//!     .write_azure_blob(store, "out/events.csv");
//! pipeline.execute().await?;
//! # Ok(())
//! # }
//! ```
//!
//! Formats follow the key extension, exactly like the S3 module.

use base64::Engine as _;
use hmac::{Hmac, KeyInit, Mac};
use sha2::Sha256;

use crate::cloud::{decode_object_stream, BatchEncoder, CloudFormat};
use crate::error::{Error, Result};
use crate::source::{BatchTx, Source, StreamingReader};
use crate::table::RecordBatch;
use std::io::BufRead;

/// API version announced in `x-ms-version`; Shared Key signing is stable
/// across these versions.
const API_VERSION: &str = "2021-08-06";

/// Azure Storage account credentials: the account name plus its shared key
/// (base64-encoded 256-bit key, from *Access keys* in the portal).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AzureCredentials {
    pub account: String,
    pub key: String,
}

impl AzureCredentials {
    pub fn new(account: impl Into<String>, key: impl Into<String>) -> Self {
        AzureCredentials {
            account: account.into(),
            key: key.into(),
        }
    }

    /// Read `AZURE_STORAGE_ACCOUNT` and `AZURE_STORAGE_KEY` (or the
    /// `AZURE_STORAGE_CONNECTION_STRING` `AccountName=`/`AccountKey=` pairs)
    /// from the environment.
    pub fn from_env() -> Result<Self> {
        if let Ok(conn) = std::env::var("AZURE_STORAGE_CONNECTION_STRING") {
            let mut account = None;
            let mut key = None;
            for part in conn.split(';') {
                let Some((k, v)) = part.split_once('=') else {
                    continue;
                };
                match k {
                    "AccountName" => account = Some(v.to_string()),
                    "AccountKey" => key = Some(v.to_string()),
                    _ => {}
                }
            }
            if let (Some(account), Some(key)) = (account, key) {
                return Ok(AzureCredentials { account, key });
            }
            return Err(Error::Cloud(
                "AZURE_STORAGE_CONNECTION_STRING has no AccountName/AccountKey".into(),
            ));
        }
        let account = std::env::var("AZURE_STORAGE_ACCOUNT")
            .map_err(|_| Error::Cloud("AZURE_STORAGE_ACCOUNT not set".into()))?;
        let key = std::env::var("AZURE_STORAGE_KEY")
            .map_err(|_| Error::Cloud("AZURE_STORAGE_KEY not set".into()))?;
        Ok(AzureCredentials { account, key })
    }

    fn decoded_key(&self) -> Result<Vec<u8>> {
        base64::engine::general_purpose::STANDARD
            .decode(&self.key)
            .map_err(|e| Error::Cloud(format!("azure: account key is not valid base64: {e}")))
    }
}

// ---------------------------------------------------------------------------
// Store
// ---------------------------------------------------------------------------

/// A signed client for one container of an Azure Blob Storage account.
#[derive(Clone)]
pub struct AzureBlobStore {
    account: String,
    container: String,
    /// `https://{account}.blob.core.windows.net` or the Azurite equivalent.
    base_url: String,
    decoded_key: Vec<u8>,
    agent: crate::httpclient::Agent,
}

impl std::fmt::Debug for AzureBlobStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AzureBlobStore")
            .field("account", &self.account)
            .field("container", &self.container)
            .field("base_url", &self.base_url)
            .finish()
    }
}

impl AzureBlobStore {
    /// Build a client for `{account_url}/{container}`. Example:
    /// `AzureBlobStore::new("https://myaccount.blob.core.windows.net", "lake", &creds)`.
    /// With the Azurite emulator use
    /// `http://127.0.0.1:10000/devstoreaccount1`.
    pub fn new(account_url: &str, container: &str, credentials: &AzureCredentials) -> Result<Self> {
        let base_url = account_url.trim_end_matches('/').to_string();
        let url = url::Url::parse(&base_url)
            .map_err(|e| Error::Cloud(format!("invalid account URL {account_url:?}: {e}")))?;
        // Real Azure: account is the first host label
        // (https://myaccount.blob.core.windows.net). Path-style URLs
        // (Azurite: http://127.0.0.1:10000/devstoreaccount1) carry it in the
        // path; anything else falls back to the credentials' account name.
        let path_account = url.path().trim_matches('/').split('/').next().unwrap_or("");
        let account = if !path_account.is_empty() {
            path_account.to_string()
        } else {
            url.host_str()
                .and_then(|host| host.split('.').next())
                .map(|h| h.to_string())
                .unwrap_or_else(|| credentials.account.clone())
        };
        Ok(AzureBlobStore {
            account,
            container: container.trim_matches('/').to_string(),
            base_url,
            decoded_key: credentials.decoded_key()?,
            agent: crate::httpclient::Agent::new(),
        })
    }

    pub fn container(&self) -> &str {
        &self.container
    }

    /// Sign a Blob service request (Shared Key, 2015+ scheme) and return the
    /// `Authorization` header value.
    ///
    /// `canonical_resource` is the decoded path *with* the leading account:
    /// `/{account}/{container}/{key}`; `query` holds sorted `(name, value)`
    /// query parameters.
    fn authorization(
        &self,
        method: &str,
        content_length: usize,
        canonical_resource: &str,
        query: &[(&str, &str)],
        x_ms_headers: &[(&str, &str)],
    ) -> String {
        // CanonicalizedHeaders: every x-ms-* header, lowercase, sorted.
        let mut ms: Vec<(&str, &str)> = x_ms_headers
            .iter()
            .filter(|(k, _)| k.starts_with("x-ms-"))
            .copied()
            .collect();
        ms.sort();
        let canonical_headers: String = ms.iter().map(|(k, v)| format!("{k}:{v}\n")).collect();

        // CanonicalizedResource: path plus sorted query params.
        let mut resource = format!("/{}{}", self.account, canonical_resource);
        let mut sorted: Vec<(&str, &str)> = query.to_vec();
        sorted.sort();
        for (k, v) in sorted {
            resource.push_str(&format!("\n{k}:{v}"));
        }

        let content_length_line = if content_length > 0 {
            content_length.to_string()
        } else {
            String::new()
        };

        let string_to_sign = format!(
            "{method}\n\n\n{content_length_line}\n\n\n\n\n\n\n\n\n{canonical_headers}{resource}"
        );

        let mut mac =
            Hmac::<Sha256>::new_from_slice(&self.decoded_key).expect("HMAC accepts any key length");
        mac.update(string_to_sign.as_bytes());
        let signature =
            base64::engine::general_purpose::STANDARD.encode(mac.finalize().into_bytes());
        format!("SharedKey {}:{}", self.account, signature)
    }

    /// One signed request line: URL, Authorization header, and the standard
    /// x-ms headers. `extra_ms` are additional x-ms-* headers that must be
    /// sent AND signed (e.g. `x-ms-blob-type`).
    fn request_parts(
        &self,
        method: &str,
        blob_path: &str,
        query: &[(&str, &str)],
        content_length: usize,
        extra_ms: &[(&'static str, &str)],
    ) -> (String, Vec<(&'static str, String)>) {
        let date = rfc1123_now();
        let mut x_ms: Vec<(&'static str, String)> = vec![
            ("x-ms-date", date),
            ("x-ms-version", API_VERSION.to_string()),
        ];
        for (k, v) in extra_ms {
            x_ms.push((k, (*v).to_string()));
        }
        let x_ms_for_signing: Vec<(&str, &str)> =
            x_ms.iter().map(|(k, v)| (*k, v.as_str())).collect();
        let auth = self.authorization(method, content_length, blob_path, query, &x_ms_for_signing);
        x_ms.push(("Authorization", auth));
        let url = format!("{}{}{}", self.base_url, blob_path, query_string(query));
        (url, x_ms)
    }

    /// `GET` a blob, returning a streaming reader over its body.
    pub fn read_object(&self, key: &str) -> Result<Box<dyn BufRead + Send>> {
        let blob_path = format!("/{}/{}", self.container, key);
        let (url, headers) = self.request_parts("GET", &blob_path, &[], 0, &[]);
        let mut request = self.agent.get(&url);
        for (k, v) in &headers {
            request = request.set(k, v);
        }
        let response = request
            .call()
            .map_err(|e| http_error("azure get", key, e))?;
        Ok(Box::new(std::io::BufReader::with_capacity(
            64 * 1024,
            response.into_reader(),
        )))
    }

    /// `PUT` an in-memory payload as one block blob.
    pub fn write_object(&self, key: &str, payload: Vec<u8>) -> Result<()> {
        let blob_path = format!("/{}/{}", self.container, key);
        let len = payload.len();
        let (url, headers) = self.request_parts(
            "PUT",
            &blob_path,
            &[],
            len,
            &[("x-ms-blob-type", "BlockBlob")],
        );
        let mut request = self.agent.put(&url);
        for (k, v) in &headers {
            request = request.set(k, v);
        }
        request
            .send_bytes(&payload)
            .map_err(|e| http_error("azure put", key, e))?;
        Ok(())
    }

    /// Upload one block (`?comp=block`).
    pub(crate) fn put_block(&self, key: &str, block_id: &str, data: Vec<u8>) -> Result<()> {
        let blob_path = format!("/{}/{}", self.container, key);
        let len = data.len();
        let query = [("comp", "block"), ("blockid", block_id)];
        let (url, headers) = self.request_parts("PUT", &blob_path, &query, len, &[]);
        let mut request = self.agent.put(&url);
        for (k, v) in &headers {
            request = request.set(k, v);
        }
        request
            .send_bytes(&data)
            .map_err(|e| http_error("azure put block", key, e))?;
        Ok(())
    }

    /// Commit a block upload (`?comp=blocklist`) in submission order.
    pub(crate) fn commit_block_upload(&self, key: &str, block_ids: &[String]) -> Result<()> {
        let blob_path = format!("/{}/{}", self.container, key);
        let mut body = String::from("<?xml version=\"1.0\" encoding=\"utf-8\"?><BlockList>");
        for id in block_ids {
            body.push_str(&format!("<Latest>{id}</Latest>"));
        }
        body.push_str("</BlockList>");
        let payload = body.into_bytes();
        let len = payload.len();
        let query = [("comp", "blocklist")];
        let (url, headers) = self.request_parts("PUT", &blob_path, &query, len, &[]);
        let mut request = self.agent.put(&url);
        for (k, v) in &headers {
            request = request.set(k, v);
        }
        request
            .send_bytes(&payload)
            .map_err(|e| http_error("azure commit blocklist", key, e))?;
        Ok(())
    }
}

/// Block ids must be identical in the `?blockid=` query and the block list
/// XML, so they use the URL-safe base64 alphabet (no percent-encoding).
fn block_id_at(index: usize) -> String {
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(format!("block-{index:08}"))
}

fn query_string(query: &[(&str, &str)]) -> String {
    if query.is_empty() {
        return String::new();
    }
    let joined: Vec<String> = query.iter().map(|(k, v)| format!("{k}={v}")).collect();
    format!("?{}", joined.join("&"))
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

/// RFC1123 date (`Tue, 15 Sep 2026 10:00:00 GMT`) as required by
/// `x-ms-date`, built without external date libraries.
fn rfc1123_now() -> String {
    let secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64;
    rfc1123_from_unix(secs)
}

fn rfc1123_from_unix(secs: i64) -> String {
    // Days-from-civil algorithm (Hinnant), weekday anchored at epoch
    // (1970-01-01 was a Thursday).
    const MONTHS: [&str; 12] = [
        "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
    ];
    const DAYS: [&str; 7] = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"];
    let days = secs.div_euclid(86_400);
    let secs_of_day = secs.rem_euclid(86_400);
    let weekday = ((days + 3).rem_euclid(7)) as usize; // 1970-01-01 (day 0) = Thursday

    // Civil-from-days.
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = if m <= 2 { y + 1 } else { y };
    let month = m as usize - 1;

    format!(
        "{}, {:02} {} {} {:02}:{:02}:{:02} GMT",
        DAYS[weekday],
        d,
        MONTHS[month],
        year,
        secs_of_day / 3600,
        (secs_of_day % 3600) / 60,
        secs_of_day % 60
    )
}

// ---------------------------------------------------------------------------
// Source
// ---------------------------------------------------------------------------

/// Streams an Azure blob as pipeline batches. Format comes from the key
/// extension (`.csv`, `.jsonl`/`.ndjson`, `.json`, `.tptcol`).
pub struct AzureBlobSource {
    reader: StreamingReader,
    pending: Option<std::result::Result<RecordBatch, Error>>,
    chunk_rows: usize,
}

impl AzureBlobSource {
    pub fn open(store: AzureBlobStore, key: impl Into<String>) -> Self {
        AzureBlobSource::open_with_chunk_size(store, key, crate::DEFAULT_CHUNK_ROWS)
    }

    pub fn open_with_chunk_size(
        store: AzureBlobStore,
        key: impl Into<String>,
        chunk_rows: usize,
    ) -> Self {
        let key = key.into();
        let format = CloudFormat::detect(&key);
        let (reader, handle) =
            StreamingReader::spawn(move |tx: &BatchTx| match store.read_object(&key) {
                Ok(body) => decode_object_stream(tx, body, format, chunk_rows, &Default::default()),
                Err(e) => {
                    let _ = tx.send(Err(e));
                }
            });
        let _ = handle;
        AzureBlobSource {
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

impl std::fmt::Debug for AzureBlobSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AzureBlobSource")
            .field("chunk_rows", &self.chunk_rows)
            .finish()
    }
}

#[async_trait::async_trait]
impl Source for AzureBlobSource {
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

/// Uploads pipeline batches to one block blob. Buffers up to `part_size`
/// bytes and stages blocks (`?comp=block`), committing the block list on
/// `finish()`; a payload that fits in one buffer is a single block-blob `PUT`.
pub struct AzureBlobSink {
    store: AzureBlobStore,
    key: String,
    format: CloudFormat,
    part_size: usize,
    encoder: BatchEncoder,
    buffer: Vec<u8>,
    blocks: Vec<String>,
    bytes_out: u64,
}

impl AzureBlobSink {
    pub fn new(store: AzureBlobStore, key: impl Into<String>) -> Self {
        let key = key.into();
        let format = CloudFormat::detect(&key);
        AzureBlobSink {
            store,
            key,
            format,
            part_size: 4 * 1024 * 1024,
            encoder: BatchEncoder::new(format),
            buffer: Vec::with_capacity(1024 * 1024),
            blocks: Vec::new(),
            bytes_out: 0,
        }
    }

    /// Buffer size before a block is staged (Azure blocks max 4000 MiB; a
    /// blob can hold up to 50,000 blocks).
    pub fn with_part_size(mut self, bytes: usize) -> Self {
        self.part_size = bytes.max(64 * 1024);
        self
    }

    pub fn bytes_out(&self) -> u64 {
        self.bytes_out
    }

    fn stage_block(&mut self) -> Result<()> {
        if self.buffer.is_empty() {
            return Ok(());
        }
        let block_id = block_id_at(self.blocks.len());
        let data = std::mem::take(&mut self.buffer);
        self.store.put_block(&self.key, &block_id, data)?;
        self.blocks.push(block_id);
        Ok(())
    }
}

impl std::fmt::Debug for AzureBlobSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AzureBlobSink")
            .field("key", &self.key)
            .field("format", &self.format)
            .field("part_size", &self.part_size)
            .field("bytes_out", &self.bytes_out)
            .finish()
    }
}

#[async_trait::async_trait]
impl crate::sink::Sink for AzureBlobSink {
    async fn write_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        let before = self.buffer.len();
        self.encoder.encode(&mut self.buffer, batch)?;
        self.bytes_out += (self.buffer.len() - before) as u64;
        if self.buffer.len() >= self.part_size {
            self.stage_block()?;
        }
        Ok(())
    }

    async fn finish(&mut self) -> Result<()> {
        let mut payload = std::mem::take(&mut self.buffer);
        self.encoder.finish(&mut payload);
        if self.blocks.is_empty() {
            // Small stream: one plain block-blob PUT.
            self.store.write_object(&self.key, payload)?;
        } else {
            if !payload.is_empty() {
                let block_id = block_id_at(self.blocks.len());
                self.store.put_block(&self.key, &block_id, payload)?;
                self.blocks.push(block_id);
            }
            let blocks = std::mem::take(&mut self.blocks);
            self.store.commit_block_upload(&self.key, &blocks)?;
        }
        self.buffer = Vec::with_capacity(self.part_size.min(1024 * 1024));
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rfc1123_format_matches_spec() {
        // 2026-09-14T00:00:00Z is a Monday; the next day is a Tuesday.
        assert_eq!(
            rfc1123_from_unix(1_789_344_000),
            "Mon, 14 Sep 2026 00:00:00 GMT"
        );
        assert_eq!(
            rfc1123_from_unix(1_789_430_400),
            "Tue, 15 Sep 2026 00:00:00 GMT"
        );
        // 1970-01-01T00:00:00Z is a Thursday.
        assert_eq!(rfc1123_from_unix(0), "Thu, 01 Jan 1970 00:00:00 GMT");
        // Leap-year date: 2024-02-29T12:30:59Z is a Thursday.
        assert_eq!(
            rfc1123_from_unix(1_709_209_859),
            "Thu, 29 Feb 2024 12:30:59 GMT"
        );
    }

    #[test]
    fn block_ids_are_unique_url_safe() {
        let a = block_id_at(0);
        let b = block_id_at(1);
        assert_ne!(a, b);
        assert!(!a.contains('+') && !a.contains('/') && !a.contains('='));
    }
}
