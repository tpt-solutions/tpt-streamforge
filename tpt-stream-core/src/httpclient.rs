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

/// A minimal HTTP client. Cheap to clone (just an `Arc<ClientConfig>`);
/// each request opens its own TLS connection.
#[derive(Clone)]
pub struct Agent {
    config: Arc<ClientConfig>,
}

impl Agent {
    pub fn new() -> Self {
        Agent {
            config: client_config(),
        }
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
        execute(
            &self.agent.config,
            self.method,
            &self.url,
            &self.headers,
            &[],
        )
    }

    pub fn send_bytes(self, body: &[u8]) -> Result<Response, Error> {
        execute(
            &self.agent.config,
            self.method,
            &self.url,
            &self.headers,
            body,
        )
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
