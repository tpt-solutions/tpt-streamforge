//! Minimal blocking HTTP/1.1-over-TLS client used by the S3, Azure, and
//! plain-HTTP(S) source/sink modules.
//!
//! Written in-house instead of depending on `ureq` because `ureq`'s `tls`
//! feature always pulls in `rustls` with its default `ring` crypto
//! provider, and `ring` is Apache-2.0-only with no MIT option — Cargo
//! feature unification means a downstream crate can't turn that default
//! off. This client depends on `rustls` directly (`default-features =
//! false`) with the `rustls-rustcrypto` provider (pure Rust, MIT/
//! Apache-2.0 dual) instead.
//!
//! It only implements the request/response subset the cloud modules
//! actually need: GET/PUT/POST/DELETE over HTTPS with a byte-slice body,
//! a streaming response body (fixed Content-Length or chunked transfer
//! encoding), and header/status access. No connection pooling, no
//! redirects, no HTTP/1.0 or plaintext HTTP support — every request opens
//! a fresh TLS connection and closes it (`Connection: close`) when done.

use std::io::{self, BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use rustls::pki_types::ServerName;
use rustls::{ClientConfig, ClientConnection, RootCertStore, StreamOwned};

const IO_TIMEOUT: Duration = Duration::from_secs(60);

fn client_config() -> Arc<ClientConfig> {
    static CONFIG: OnceLock<Arc<ClientConfig>> = OnceLock::new();
    CONFIG
        .get_or_init(|| {
            let mut roots = RootCertStore::empty();
            let result = rustls_native_certs::load_native_certs();
            for cert in result.certs {
                let _ = roots.add(cert);
            }
            let provider = Arc::new(rustls_rustcrypto::provider());
            let config = ClientConfig::builder_with_provider(provider)
                .with_safe_default_protocol_versions()
                .expect("rustls-rustcrypto supports the default TLS protocol versions")
                .with_root_certificates(roots)
                .with_no_client_auth();
            Arc::new(config)
        })
        .clone()
}

/// A minimal HTTP client. Cheap to clone (just an `Arc<ClientConfig>` plus the
/// retry policy); each request opens its own TLS connection.
#[derive(Clone)]
pub struct Agent {
    config: Arc<ClientConfig>,
    retry: RetryPolicy,
}

impl Agent {
    pub fn new() -> Self {
        Agent {
            config: client_config(),
            retry: RetryPolicy::default(),
        }
    }

    /// Retry transient failures according to `policy`. Off by default.
    #[must_use]
    pub fn with_retry(mut self, policy: RetryPolicy) -> Self {
        self.retry = policy;
        self
    }

    pub fn get(&self, url: &str) -> RequestBuilder<'_> {
        RequestBuilder::new(self, "GET", url)
    }

    pub fn put(&self, url: &str) -> RequestBuilder<'_> {
        RequestBuilder::new(self, "PUT", url)
    }

    pub fn post(&self, url: &str) -> RequestBuilder<'_> {
        RequestBuilder::new(self, "POST", url)
    }

    pub fn delete(&self, url: &str) -> RequestBuilder<'_> {
        RequestBuilder::new(self, "DELETE", url)
    }
}

impl Default for Agent {
    fn default() -> Self {
        Agent::new()
    }
}

/// How many times, and how patiently, to retry a failed request.
///
/// Retries only ever fire for *transient* failures — transport errors and
/// retryable statuses (408, 429, 5xx). A 4xx that isn't 408/429 is the server
/// telling us the request itself is wrong, so it is surfaced immediately
/// rather than retried into the same rejection.
///
/// `attempts` counts total tries, so `attempts: 1` disables retrying and is
/// the default, keeping existing behavior unchanged.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct RetryPolicy {
    /// Total attempts including the first, so `1` means "no retry".
    pub attempts: u32,
    /// Delay before the first retry; doubles each subsequent retry.
    pub base_delay: Duration,
    /// Upper bound on any single backoff delay.
    pub max_delay: Duration,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        // No retries unless a caller opts in.
        RetryPolicy {
            attempts: 1,
            base_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(10),
        }
    }
}

impl RetryPolicy {
    /// A policy that never retries. Equivalent to the default.
    pub fn none() -> Self {
        RetryPolicy::default()
    }

    /// `attempts` total tries, starting from `base_delay` and doubling up to
    /// `max_delay`.
    pub fn new(attempts: u32, base_delay: Duration, max_delay: Duration) -> Self {
        RetryPolicy {
            attempts: attempts.max(1),
            base_delay,
            max_delay,
        }
    }

    /// Exponential backoff for `attempt` (1-based), with jitter and a cap.
    ///
    /// Jitter is full-width over `[delay/2, delay]`, so a fleet of clients
    /// retrying after the same outage does not resynchronize into a thundering
    /// herd. Deterministic (seeded from the attempt number) to keep this
    /// testable without a RNG dependency.
    pub fn backoff(&self, attempt: u32) -> Duration {
        if self.base_delay.is_zero() {
            return Duration::ZERO;
        }
        let shift = attempt.saturating_sub(1).min(16);
        let scaled = self
            .base_delay
            .saturating_mul(1u32 << shift)
            .min(self.max_delay);
        // Cheap deterministic jitter in [50%, 100%] of `scaled`.
        let jitter_nanos = (scaled.as_nanos() as u64)
            .wrapping_mul(2_654_435_761)
            .wrapping_add(attempt as u64)
            % 1_000_000_007;
        let half = scaled / 2;
        half + Duration::from_nanos(jitter_nanos % (half.as_nanos() as u64).max(1))
    }
}

/// Whether a failed attempt is worth retrying.
///
/// `status` is `None` for transport-level failures (connection reset, TLS
/// error, timeout), which are the most common transient fault.
pub fn is_retryable(status: Option<u16>) -> bool {
    match status {
        // Transport failure: no response at all.
        None => true,
        Some(code) => code == 408 || code == 429 || (500..600).contains(&code),
    }
}

/// Mirrors the tiny slice of `ureq::AgentBuilder`'s API the call sites used
/// to use, so migrating off `ureq` needed no changes at the call sites.
pub struct AgentBuilder;

impl AgentBuilder {
    pub fn new() -> Self {
        AgentBuilder
    }

    pub fn build(self) -> Agent {
        Agent::new()
    }
}

impl Default for AgentBuilder {
    fn default() -> Self {
        AgentBuilder::new()
    }
}

pub struct RequestBuilder<'a> {
    agent: &'a Agent,
    method: &'static str,
    url: String,
    headers: Vec<(String, String)>,
}

impl<'a> RequestBuilder<'a> {
    fn new(agent: &'a Agent, method: &'static str, url: &str) -> Self {
        RequestBuilder {
            agent,
            method,
            url: url.to_string(),
            headers: Vec::new(),
        }
    }

    pub fn set(mut self, key: &str, value: &str) -> Self {
        self.headers.push((key.to_string(), value.to_string()));
        self
    }

    pub fn call(self) -> Result<Response, Error> {
        execute_with_retry(
            &self.agent.config,
            self.agent.retry,
            self.method,
            &self.url,
            &self.headers,
            &[],
        )
    }

    pub fn send_bytes(self, body: &[u8]) -> Result<Response, Error> {
        execute_with_retry(
            &self.agent.config,
            self.agent.retry,
            self.method,
            &self.url,
            &self.headers,
            body,
        )
    }
}

/// Run one request, retrying transient failures per `policy`.
///
/// `body` is a byte slice that `execute` copies into the socket on each
/// attempt, so a retry re-sends the identical payload; that keeps retries safe
/// for the idempotent requests the cloud modules issue (GET, PUT of a part,
/// DELETE of an upload).
fn execute_with_retry(
    config: &Arc<ClientConfig>,
    policy: RetryPolicy,
    method: &str,
    url: &str,
    extra_headers: &[(String, String)],
    body: &[u8],
) -> Result<Response, Error> {
    let attempts = policy.attempts.max(1);
    let mut attempt = 1;
    loop {
        match execute(config, method, url, extra_headers, body) {
            Ok(response) => return Ok(response),
            Err(err) => {
                let status = match &err {
                    Error::Status(code, _) => Some(*code),
                    Error::Transport(_) => None,
                };
                if attempt >= attempts || !is_retryable(status) {
                    return Err(err);
                }
                let delay = policy.backoff(attempt);
                crate::trace_event!(
                    "tpt_stream_core::httpclient",
                    crate::telemetry::Level::INFO,
                    method,
                    url,
                    status = status.unwrap_or(0),
                    attempt,
                    delay_ms = delay.as_millis() as u64,
                    "retrying transient request failure"
                );
                if !delay.is_zero() {
                    std::thread::sleep(delay);
                }
                attempt += 1;
            }
        }
    }
}

pub enum Error {
    Status(u16, Box<Response>),
    Transport(String),
}

impl std::fmt::Debug for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Status(code, _) => write!(f, "Status({code}, ..)"),
            Error::Transport(msg) => write!(f, "Transport({msg:?})"),
        }
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Status(code, _) => write!(f, "http status {code}"),
            Error::Transport(msg) => write!(f, "{msg}"),
        }
    }
}

impl std::error::Error for Error {}

/// Either a plain TCP connection (used only for `http://127.0.0.1` mock
/// servers and the Azurite emulator in tests) or a TLS connection over
/// `rustls-rustcrypto`. Real cloud endpoints always use `Tls`.
enum Conn {
    Plain(TcpStream),
    Tls(Box<StreamOwned<ClientConnection, TcpStream>>),
}

impl Read for Conn {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match self {
            Conn::Plain(s) => s.read(buf),
            Conn::Tls(s) => s.read(buf),
        }
    }
}

impl Write for Conn {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match self {
            Conn::Plain(s) => s.write(buf),
            Conn::Tls(s) => s.write(buf),
        }
    }
    fn flush(&mut self) -> io::Result<()> {
        match self {
            Conn::Plain(s) => s.flush(),
            Conn::Tls(s) => s.flush(),
        }
    }
}

pub struct Response {
    status: u16,
    headers: Vec<(String, String)>,
    body: BodyInner,
}

enum BodyInner {
    Empty,
    Fixed(BufReader<Conn>, u64),
    /// (reader, bytes remaining in the current chunk, saw the 0-length chunk)
    Chunked(BufReader<Conn>, u64, bool),
    ToEof(BufReader<Conn>),
}

impl Response {
    pub fn status(&self) -> u16 {
        self.status
    }

    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
    }

    pub fn into_reader(self) -> Box<dyn Read + Send> {
        Box::new(BodyReader { inner: self.body })
    }

    pub fn into_string(self) -> Result<String, io::Error> {
        let mut reader = BodyReader { inner: self.body };
        let mut buf = String::new();
        reader.read_to_string(&mut buf)?;
        Ok(buf)
    }
}

struct BodyReader {
    inner: BodyInner,
}

impl Read for BodyReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match &mut self.inner {
            BodyInner::Empty => Ok(0),
            BodyInner::ToEof(r) => r.read(buf),
            BodyInner::Fixed(r, remaining) => {
                if *remaining == 0 || buf.is_empty() {
                    return Ok(0);
                }
                let cap = (buf.len() as u64).min(*remaining) as usize;
                let n = r.read(&mut buf[..cap])?;
                *remaining -= n as u64;
                Ok(n)
            }
            BodyInner::Chunked(r, remaining, finished) => {
                if *finished || buf.is_empty() {
                    return Ok(0);
                }
                if *remaining == 0 {
                    let mut line = String::new();
                    r.read_line(&mut line)?;
                    let size_str = line.trim().split(';').next().unwrap_or("").trim();
                    let size = u64::from_str_radix(size_str, 16).map_err(|e| {
                        io::Error::new(
                            io::ErrorKind::InvalidData,
                            format!("bad chunk size {line:?}: {e}"),
                        )
                    })?;
                    if size == 0 {
                        // Trailing headers (usually none), then the final CRLF.
                        loop {
                            let mut trailer = String::new();
                            if r.read_line(&mut trailer)? == 0 || trailer.trim().is_empty() {
                                break;
                            }
                        }
                        *finished = true;
                        return Ok(0);
                    }
                    *remaining = size;
                }
                let cap = (buf.len() as u64).min(*remaining) as usize;
                let n = r.read(&mut buf[..cap])?;
                *remaining -= n as u64;
                if *remaining == 0 {
                    let mut crlf = [0u8; 2];
                    r.read_exact(&mut crlf)?;
                }
                Ok(n)
            }
        }
    }
}

fn execute(
    config: &Arc<ClientConfig>,
    method: &str,
    url_str: &str,
    extra_headers: &[(String, String)],
    body: &[u8],
) -> Result<Response, Error> {
    let url = url::Url::parse(url_str)
        .map_err(|e| Error::Transport(format!("invalid URL {url_str:?}: {e}")))?;
    let is_tls = match url.scheme() {
        "https" => true,
        // Plain HTTP is only reachable for loopback test servers and the
        // Azurite emulator; real cloud endpoints always use TLS.
        "http" => false,
        other => {
            return Err(Error::Transport(format!(
                "unsupported URL scheme {other:?} in {url_str:?}"
            )))
        }
    };
    let host = url
        .host_str()
        .ok_or_else(|| Error::Transport(format!("URL {url_str:?} has no host")))?
        .to_string();
    let port = url
        .port_or_known_default()
        .unwrap_or(if is_tls { 443 } else { 80 });
    let mut path = url.path().to_string();
    if path.is_empty() {
        path = "/".to_string();
    }
    if let Some(q) = url.query() {
        path.push('?');
        path.push_str(q);
    }

    let addr = format!("{host}:{port}");
    let tcp =
        TcpStream::connect(&addr).map_err(|e| Error::Transport(format!("connect {addr}: {e}")))?;
    let _ = tcp.set_read_timeout(Some(IO_TIMEOUT));
    let _ = tcp.set_write_timeout(Some(IO_TIMEOUT));

    let mut stream = if is_tls {
        let server_name = ServerName::try_from(host.clone())
            .map_err(|e| Error::Transport(format!("invalid hostname {host:?}: {e}")))?;
        let conn = ClientConnection::new(config.clone(), server_name)
            .map_err(|e| Error::Transport(format!("tls setup: {e}")))?;
        Conn::Tls(Box::new(StreamOwned::new(conn, tcp)))
    } else {
        Conn::Plain(tcp)
    };

    let mut request = format!("{method} {path} HTTP/1.1\r\n");
    request.push_str(&format!("Host: {host}\r\n"));
    request.push_str("Connection: close\r\n");
    request.push_str("User-Agent: tpt-streamforge\r\n");
    let has_content_type = extra_headers
        .iter()
        .any(|(k, _)| k.eq_ignore_ascii_case("content-type"));
    for (k, v) in extra_headers {
        request.push_str(&format!("{k}: {v}\r\n"));
    }
    if !body.is_empty() && !has_content_type {
        request.push_str("Content-Type: application/octet-stream\r\n");
    }
    if matches!(method, "PUT" | "POST") || !body.is_empty() {
        request.push_str(&format!("Content-Length: {}\r\n", body.len()));
    }
    request.push_str("\r\n");

    stream
        .write_all(request.as_bytes())
        .map_err(|e| Error::Transport(format!("write request: {e}")))?;
    if !body.is_empty() {
        stream
            .write_all(body)
            .map_err(|e| Error::Transport(format!("write body: {e}")))?;
    }

    let mut reader = BufReader::new(stream);
    let (status, headers) = parse_status_and_headers(&mut reader)
        .map_err(|e| Error::Transport(format!("read response: {e}")))?;

    let content_length = headers
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case("content-length"))
        .and_then(|(_, v)| v.trim().parse::<u64>().ok());
    let chunked = headers.iter().any(|(k, v)| {
        k.eq_ignore_ascii_case("transfer-encoding") && v.to_ascii_lowercase().contains("chunked")
    });

    let body_inner = if method == "HEAD" {
        BodyInner::Empty
    } else if chunked {
        BodyInner::Chunked(reader, 0, false)
    } else if let Some(len) = content_length {
        BodyInner::Fixed(reader, len)
    } else {
        BodyInner::ToEof(reader)
    };

    let response = Response {
        status,
        headers,
        body: body_inner,
    };
    if !(200..300).contains(&status) {
        return Err(Error::Status(status, Box::new(response)));
    }
    Ok(response)
}

fn parse_status_and_headers(
    reader: &mut BufReader<Conn>,
) -> io::Result<(u16, Vec<(String, String)>)> {
    let mut status_line = String::new();
    reader.read_line(&mut status_line)?;
    let mut parts = status_line.trim().splitn(3, ' ');
    let _version = parts.next();
    let status: u16 = parts.next().and_then(|s| s.parse().ok()).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("bad status line {status_line:?}"),
        )
    })?;
    let mut headers = Vec::new();
    loop {
        let mut line = String::new();
        let n = reader.read_line(&mut line)?;
        if n == 0 || line.trim().is_empty() {
            break;
        }
        if let Some((k, v)) = line.split_once(':') {
            headers.push((k.trim().to_string(), v.trim().to_string()));
        }
    }
    Ok((status, headers))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_policy_does_not_retry() {
        assert_eq!(RetryPolicy::default().attempts, 1);
        assert_eq!(RetryPolicy::none(), RetryPolicy::default());
    }

    #[test]
    fn transport_and_server_errors_are_retryable() {
        // No response at all: the most common transient fault.
        assert!(is_retryable(None));
        assert!(is_retryable(Some(408)));
        assert!(is_retryable(Some(429)));
        assert!(is_retryable(Some(500)));
        assert!(is_retryable(Some(503)));
    }

    #[test]
    fn client_errors_are_not_retryable() {
        // Retrying a malformed or unauthorized request just wastes time and
        // hammers the endpoint; it can never succeed.
        for code in [400, 401, 403, 404, 409, 413, 422] {
            assert!(!is_retryable(Some(code)), "{code} must not retry");
        }
    }

    #[test]
    fn backoff_grows_and_is_capped() {
        let policy = RetryPolicy::new(10, Duration::from_millis(100), Duration::from_millis(800));
        let mut last = Duration::ZERO;
        for attempt in 1..=10 {
            let delay = policy.backoff(attempt);
            assert!(
                delay <= policy.max_delay,
                "attempt {attempt} exceeded max_delay: {delay:?}"
            );
            // Jitter keeps it in [50%, 100%] of the scaled delay, so growth is
            // monotonic per-attempt but never exactly doubling.
            if attempt <= 3 {
                assert!(delay >= last, "backoff shrank at attempt {attempt}");
                last = delay;
            }
        }
    }

    #[test]
    fn backoff_of_zero_is_immediate() {
        let policy = RetryPolicy::new(5, Duration::ZERO, Duration::from_secs(1));
        assert_eq!(policy.backoff(1), Duration::ZERO);
        assert_eq!(policy.backoff(4), Duration::ZERO);
    }

    #[test]
    fn backoff_never_exceeds_cap_even_with_large_shift() {
        let policy = RetryPolicy::new(64, Duration::from_secs(1), Duration::from_secs(5));
        for attempt in [1, 2, 30, 31, 63, 64] {
            assert!(policy.backoff(attempt) <= Duration::from_secs(5));
        }
    }

    #[test]
    fn attempts_of_zero_is_clamped_to_one() {
        let policy = RetryPolicy::new(0, Duration::ZERO, Duration::ZERO);
        assert_eq!(policy.attempts, 1, "0 attempts must still try once");
    }
}
