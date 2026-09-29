//! Opt-in Prometheus exporter for a running pipeline.
//!
//! Serves `/metrics` in the Prometheus text exposition format, driven by the
//! engine's own telemetry stream, so an external scraper can watch a long ETL
//! run without the pipeline knowing about it.
//!
//! Deliberately built on `std::net::TcpListener` with no new dependency: the
//! project keeps its dependency tree minimal, and this is a single-threaded,
//! read-only, best-effort endpoint that does not justify a web framework. It
//! is off unless `--metrics` is passed.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::{IpAddr, SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tpt_stream_core::telemetry::{StageMetrics, TelemetryEvent};

/// Longest request line accepted, matching the conventional 8 KiB limit.
const MAX_REQUEST_LINE: usize = 8 * 1024;
/// Longest total header block accepted.
const MAX_HEADER_BYTES: usize = 16 * 1024;
/// Number of header lines accepted before the request is rejected.
const MAX_HEADERS: usize = 100;
/// Ceiling on how much of a rejected request we will read and throw away.
const MAX_DRAIN_BYTES: usize = 64 * 1024;
/// Deadline for reading one request, and for writing the response.
const IO_TIMEOUT: Duration = Duration::from_secs(2);
/// Ceiling on how long one connection may occupy the single accept thread.
const CONNECTION_DEADLINE: Duration = Duration::from_secs(5);

/// Serve `/metrics` on `addr` until `shutdown` is set.
///
/// Refuses a non-loopback bind address: this is an unauthenticated counter
/// endpoint, and publishing a job's row counts to the network should be a
/// deliberate act. Use [`serve_allow_remote`] to opt in.
///
/// Returns the bound address, so a caller that asked for port `0` can discover
/// what it got (the tests rely on this). Binding happens before this returns,
/// so a port conflict surfaces to the caller rather than on the scraper thread.
pub fn serve(
    addr: &str,
    pipeline: String,
    stats: Arc<Metrics>,
    shutdown: Arc<AtomicBool>,
) -> std::io::Result<SocketAddr> {
    start(addr, false, pipeline, stats, shutdown)
}

/// Like [`serve`] but permits a non-loopback (including `0.0.0.0`) bind address.
pub fn serve_allow_remote(
    addr: &str,
    pipeline: String,
    stats: Arc<Metrics>,
    shutdown: Arc<AtomicBool>,
) -> std::io::Result<SocketAddr> {
    start(addr, true, pipeline, stats, shutdown)
}

fn start(
    addr: &str,
    allow_remote: bool,
    pipeline: String,
    stats: Arc<Metrics>,
    shutdown: Arc<AtomicBool>,
) -> std::io::Result<SocketAddr> {
    let listener = TcpListener::bind(addr)?;
    let local = listener.local_addr()?;
    if !allow_remote && !is_loopback(&local) {
        return Err(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            format!(
                "refusing to expose the metrics endpoint on non-loopback address {local}; \
                 pass --metrics-allow-remote if you really mean it"
            ),
        ));
    }
    listener.set_nonblocking(true)?;

    std::thread::spawn(move || {
        while !shutdown.load(Ordering::Relaxed) {
            match listener.accept() {
                Ok((stream, _)) => {
                    // One connection at a time, but bounded by that
                    // connection's own deadline: a client that connects and
                    // then stalls must not wedge the endpoint for the run.
                    let _ = handle_connection(stream, &pipeline, &stats);
                }
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    std::thread::sleep(Duration::from_millis(25));
                }
                Err(_) => break,
            }
        }
    });
    Ok(local)
}

/// `0.0.0.0` / `::` are the "all interfaces" wildcards, not loopback; binding
/// one exposes the endpoint to the network just as surely as a routable
/// address does, so they are rejected by default.
fn is_loopback(addr: &SocketAddr) -> bool {
    match addr.ip() {
        IpAddr::V4(ip) => ip.is_loopback() && ip.octets() != [0, 0, 0, 0],
        IpAddr::V6(ip) => ip.is_loopback() && ip.segments() != [0, 0, 0, 0, 0, 0, 0, 0],
    }
}

/// Answer one request. Anything other than `GET /metrics` (or `/`) gets a 404;
/// the connection is always closed, since a scrape endpoint needs no
/// keep-alive.
///
/// Every read is bounded (request-line length, header count, total header
/// bytes) and the whole exchange has a deadline, so a hostile or merely broken
/// client can neither make this single-threaded server allocate without bound
/// nor hold it open for the length of a pipeline run.
fn handle_connection(
    mut stream: TcpStream,
    pipeline: &str,
    stats: &Metrics,
) -> std::io::Result<()> {
    let deadline = Instant::now() + CONNECTION_DEADLINE;
    stream.set_read_timeout(Some(IO_TIMEOUT))?;
    stream.set_write_timeout(Some(IO_TIMEOUT))?;
    let mut reader = BufReader::new(stream.try_clone()?);

    // An oversized request line is a protocol error, not something to try to
    // parse. Answer 431 and hang up — dropping the connection silently would
    // look like a network fault to the scraper.
    let request_line = match read_line_limited(&mut reader, MAX_REQUEST_LINE) {
        Ok(Some(line)) => line,
        // Nothing at all: the peer went away before speaking. Not an error.
        Ok(None) => return Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::InvalidData => {
            drain_remaining(&mut reader);
            return respond(
                &mut stream,
                "431 Request Header Fields Too Large",
                "# request line too large\n",
            );
        }
        Err(e) => return Err(e),
    };

    // Drain the header block. Two reasons, both load-bearing:
    //  - bounds how much a client can make us buffer, and
    //  - an *unread* request is what makes the server's close() arrive at the
    //    peer as an RST rather than a clean FIN, which is what makes a
    //    well-behaved client see ECONNRESET while reading a response it has
    //    already received in full.
    let mut header_bytes = 0usize;
    let mut headers = 0usize;
    while let Some(line) = read_line_limited(&mut reader, MAX_REQUEST_LINE)? {
        if line.trim().is_empty() {
            break;
        }
        headers += 1;
        header_bytes += line.len();
        if headers > MAX_HEADERS || header_bytes > MAX_HEADER_BYTES {
            drain_remaining(&mut reader);
            return respond(
                &mut stream,
                "431 Request Header Fields Too Large",
                "# header block too large\n",
            );
        }
        if Instant::now() >= deadline {
            return respond(&mut stream, "408 Request Timeout", "# timed out\n");
        }
    }

    let path = request_line.split_whitespace().nth(1).unwrap_or("");
    let (status, body) = if path == "/metrics" || path == "/" {
        ("200 OK", render(pipeline, stats))
    } else {
        ("404 Not Found", "# unknown path\n".to_string())
    };
    respond(&mut stream, status, &body)
}

/// Read one line of at most `max` bytes. `Ok(None)` means the peer closed
/// cleanly on a line boundary; an over-long line is an error rather than an
/// unbounded allocation.
fn read_line_limited(
    reader: &mut BufReader<TcpStream>,
    max: usize,
) -> std::io::Result<Option<String>> {
    let mut raw = Vec::new();
    let read = Read::by_ref(reader)
        .take(max as u64 + 1)
        .read_until(b'\n', &mut raw)?;
    if read == 0 {
        return Ok(None);
    }
    if read > max {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("request line exceeds {max} bytes"),
        ));
    }
    Ok(Some(String::from_utf8_lossy(&raw).into_owned()))
}

/// Read and discard whatever is left of a request we are about to reject,
/// bounded by [`MAX_DRAIN_BYTES`].
///
/// This is not politeness, it is correctness: closing a socket that still has
/// unread data in its receive queue makes Windows (and BSD) send an RST
/// instead of a FIN, and the peer then *discards data it already received* —
/// so the 431 we just wrote vanishes and the client sees a connection reset
/// with no explanation. Draining empties the queue so the close is graceful.
fn drain_remaining(reader: &mut BufReader<TcpStream>) {
    let _ = std::io::copy(
        &mut Read::by_ref(reader).take(MAX_DRAIN_BYTES as u64),
        &mut std::io::sink(),
    );
}

fn respond(stream: &mut TcpStream, status: &str, body: &str) -> std::io::Result<()> {
    write!(
        stream,
        "HTTP/1.1 {status}\r\n\
         Content-Type: text/plain; version=0.0.4; charset=utf-8\r\n\
         Content-Length: {}\r\n\
         Connection: close\r\n\
         \r\n{body}",
        body.len()
    )?;
    stream.flush()?;
    // Send the FIN explicitly while the socket is still open, rather than
    // leaving it to whichever of the two handles happens to be dropped last.
    let _ = stream.shutdown(std::net::Shutdown::Write);
    Ok(())
}

/// Render the current metrics in Prometheus text exposition format.
fn render(pipeline: &str, stats: &Metrics) -> String {
    let snapshot = stats.snapshot();
    let mut out = String::with_capacity(512 + snapshot.stages.len() * 192);
    let p = escape(pipeline);

    out.push_str("# HELP tptforge_rows Rows read from the source.\n");
    out.push_str("# TYPE tptforge_rows counter\n");
    out.push_str(&format!(
        "tptforge_rows{{pipeline=\"{p}\"}} {}\n",
        snapshot.rows
    ));

    out.push_str("# HELP tptforge_batches Batches produced by the source.\n");
    out.push_str("# TYPE tptforge_batches counter\n");
    out.push_str(&format!(
        "tptforge_batches{{pipeline=\"{p}\"}} {}\n",
        snapshot.batches
    ));

    out.push_str("# HELP tptforge_stage_rows Rows entering or leaving a stage.\n");
    out.push_str("# TYPE tptforge_stage_rows counter\n");
    for stage in &snapshot.stages {
        let s = escape(&stage.name);
        out.push_str(&format!(
            "tptforge_stage_rows{{pipeline=\"{p}\",stage=\"{s}\",dir=\"in\"}} {}\n",
            stage.rows_in
        ));
        out.push_str(&format!(
            "tptforge_stage_rows{{pipeline=\"{p}\",stage=\"{s}\",dir=\"out\"}} {}\n",
            stage.rows_out
        ));
    }

    out.push_str("# HELP tptforge_dead_letter_rows Rows captured by the dead-letter queue.\n");
    out.push_str("# TYPE tptforge_dead_letter_rows counter\n");
    out.push_str(&format!(
        "tptforge_dead_letter_rows{{pipeline=\"{p}\"}} {}\n",
        snapshot.dead_letter_rows
    ));

    out.push_str("# HELP tptforge_running Whether the pipeline is still running.\n");
    out.push_str("# TYPE tptforge_running gauge\n");
    out.push_str(&format!(
        "tptforge_running{{pipeline=\"{p}\"}} {}\n",
        u8::from(snapshot.running)
    ));

    out
}

/// Escape a label value per the Prometheus exposition rules.
fn escape(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
}

/// Live metrics, updated from the engine's telemetry stream as the run proceeds.
///
/// A poisoned lock is ignored rather than propagated: losing a metrics update
/// must never fail the ETL job it is observing.
#[derive(Default)]
pub struct Metrics {
    inner: Mutex<Snapshot>,
}

#[derive(Default, Clone)]
struct Snapshot {
    rows: u64,
    batches: u64,
    stages: Vec<StageMetrics>,
    dead_letter_rows: u64,
    running: bool,
}

impl Metrics {
    pub fn new() -> Self {
        Metrics {
            inner: Mutex::new(Snapshot {
                running: true,
                ..Snapshot::default()
            }),
        }
    }

    /// Record one event from the engine's telemetry stream.
    pub fn on_event(&self, event: &TelemetryEvent) {
        let Ok(mut s) = self.inner.lock() else {
            return;
        };
        match event {
            TelemetryEvent::SourceBatch {
                total_rows,
                batches,
                ..
            } => {
                s.rows = *total_rows;
                s.batches = *batches;
            }
            TelemetryEvent::StageBatch {
                stage,
                rows_in,
                rows_out,
            } => {
                let entry = match s.stages.iter_mut().find(|m| &m.name == stage) {
                    Some(m) => m,
                    None => {
                        s.stages.push(StageMetrics {
                            name: stage.clone(),
                            ..StageMetrics::default()
                        });
                        s.stages.last_mut().expect("just pushed")
                    }
                };
                entry.rows_in += rows_in;
                entry.rows_out += rows_out;
                entry.batches += 1;
            }
            TelemetryEvent::SinkBatch { .. } => {}
            TelemetryEvent::Done { .. } => s.running = false,
        }
    }

    /// Mark the run finished from the final stats (also used on the error path,
    /// so `tptforge_running` drops to 0 either way).
    pub fn finish(&self, rows: u64, batches: u64) {
        if let Ok(mut s) = self.inner.lock() {
            s.rows = rows;
            s.batches = batches;
            s.running = false;
        }
    }

    pub fn add_dead_letter(&self, rows: u64) {
        if let Ok(mut s) = self.inner.lock() {
            s.dead_letter_rows += rows;
        }
    }

    fn snapshot(&self) -> Snapshot {
        self.inner.lock().map(|s| s.clone()).unwrap_or_default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    // `TcpStream::read_to_string` needs the `Read` trait in scope; the
    // production code only writes.
    use std::io::Read;

    fn metrics_with_events() -> Metrics {
        let m = Metrics::new();
        m.on_event(&TelemetryEvent::SourceBatch {
            rows: 10,
            total_rows: 10,
            batches: 1,
        });
        m.on_event(&TelemetryEvent::StageBatch {
            stage: "filter".into(),
            rows_in: 10,
            rows_out: 4,
        });
        m.on_event(&TelemetryEvent::StageBatch {
            stage: "filter".into(),
            rows_in: 10,
            rows_out: 3,
        });
        m
    }

    #[test]
    fn render_emits_prometheus_text_format() {
        let m = metrics_with_events();
        let out = render("test", &m);
        assert!(out.contains("tptforge_rows{pipeline=\"test\"} 10"), "{out}");
        assert!(
            out.contains("tptforge_stage_rows{pipeline=\"test\",stage=\"filter\",dir=\"in\"} 20"),
            "{out}"
        );
        assert!(
            out.contains("tptforge_stage_rows{pipeline=\"test\",stage=\"filter\",dir=\"out\"} 7"),
            "{out}"
        );
        assert!(out.contains("# TYPE tptforge_rows counter"), "{out}");
    }

    #[test]
    fn stage_counters_accumulate_across_batches() {
        let m = metrics_with_events();
        let snap = m.snapshot();
        assert_eq!(snap.stages.len(), 1, "same stage must not duplicate");
        assert_eq!(snap.stages[0].rows_in, 20);
        assert_eq!(snap.stages[0].rows_out, 7);
        assert_eq!(snap.stages[0].batches, 2);
    }

    #[test]
    fn source_totals_take_the_latest_values() {
        let m = Metrics::new();
        m.on_event(&TelemetryEvent::SourceBatch {
            rows: 10,
            total_rows: 10,
            batches: 1,
        });
        m.on_event(&TelemetryEvent::SourceBatch {
            rows: 5,
            total_rows: 15,
            batches: 2,
        });
        let snap = m.snapshot();
        assert_eq!(snap.rows, 15, "must not double-count the running total");
        assert_eq!(snap.batches, 2);
    }

    #[test]
    fn done_marks_not_running() {
        let m = metrics_with_events();
        assert!(m.snapshot().running);
        m.on_event(&TelemetryEvent::Done {
            rows: 20,
            batches: 2,
            elapsed: Duration::from_millis(5),
        });
        assert!(!m.snapshot().running);
        assert!(render("p", &m).contains("tptforge_running{pipeline=\"p\"} 0"));
    }

    #[test]
    fn finish_marks_not_running() {
        let m = metrics_with_events();
        m.finish(42, 3);
        let snap = m.snapshot();
        assert!(!snap.running);
        assert_eq!(snap.rows, 42);
        assert_eq!(snap.batches, 3);
    }

    #[test]
    fn labels_are_escaped() {
        let m = Metrics::new();
        m.on_event(&TelemetryEvent::StageBatch {
            stage: "we\"ird".into(),
            rows_in: 1,
            rows_out: 1,
        });
        let out = render("p", &m);
        assert!(out.contains(r#"stage="we\"ird""#), "{out}");
    }

    #[test]
    fn dead_letter_rows_are_counted() {
        let m = metrics_with_events();
        m.add_dead_letter(3);
        assert!(render("p", &m).contains("tptforge_dead_letter_rows{pipeline=\"p\"} 3"));
    }

    #[test]
    fn every_line_is_a_valid_exposition_line() {
        // A malformed exposition file makes Prometheus drop the whole scrape.
        let m = metrics_with_events();
        for line in render("p", &m).lines() {
            assert!(
                line.starts_with('#') || line.split(' ').count() >= 2,
                "bad exposition line: {line:?}"
            );
            assert!(!line.contains('\r'), "CR in exposition: {line:?}");
        }
    }

    fn get(addr: SocketAddr, path: &str) -> String {
        get_raw(
            addr,
            &format!("GET {path} HTTP/1.1\r\nHost: localhost\r\n\r\n"),
        )
    }

    /// Send a raw request and read whatever comes back. A reset *after* a
    /// complete response is a normal end-of-connection on Windows, so it is
    /// reported as success with whatever was received; only a reset with no
    /// data at all is a failure.
    ///
    /// For a deliberately malformed request the peer may close without
    /// answering at all — Windows sends an RST (rather than a FIN) when a
    /// socket is closed with unread data in its receive queue, and a half-close
    /// from this side plus that RST is a legitimate "no reply" outcome. The
    /// reject-path tests therefore assert on the security property (never a
    /// 200, and the endpoint survives) rather than on which close flavour the
    /// OS picked.
    fn get_raw(addr: SocketAddr, request: &str) -> String {
        let mut stream = TcpStream::connect(addr).unwrap();
        stream.write_all(request.as_bytes()).unwrap();
        stream.flush().unwrap();
        // Nudge the server so it has the whole request before it answers.
        stream
            .shutdown(std::net::Shutdown::Write)
            .expect("half-close must be supported on loopback");
        let mut body = String::new();
        match stream.read_to_string(&mut body) {
            Ok(_) => body,
            Err(e)
                if matches!(
                    e.kind(),
                    std::io::ErrorKind::ConnectionReset
                        | std::io::ErrorKind::ConnectionAborted
                        | std::io::ErrorKind::BrokenPipe
                ) =>
            {
                assert!(!body.is_empty(), "reset before any response: {e}");
                body
            }
            Err(e) => panic!("metrics request failed: {e}"),
        }
    }

    /// Assert the request was refused and the endpoint is still serving.
    fn assert_refused_and_healthy(addr: SocketAddr, body: &str, what: &str) {
        assert!(
            !body.contains("HTTP/1.1 200 OK"),
            "{what} must not be served as 200: {body}"
        );
        // The single accept thread must not have been wedged or poisoned.
        let after = get(addr, "/metrics");
        assert!(
            after.starts_with("HTTP/1.1 200 OK"),
            "{what} wedged the server: {after}"
        );
    }

    #[test]
    fn serve_answers_metrics_over_http() {
        let shutdown = Arc::new(AtomicBool::new(false));
        let addr = serve(
            "127.0.0.1:0",
            "live".into(),
            Arc::new(metrics_with_events()),
            shutdown.clone(),
        )
        .unwrap();
        let body = get(addr, "/metrics");
        assert!(body.starts_with("HTTP/1.1 200 OK"), "{body}");
        assert!(
            body.contains("tptforge_rows{pipeline=\"live\"} 10"),
            "{body}"
        );
        shutdown.store(true, Ordering::Relaxed);
    }

    #[test]
    fn serve_404s_unknown_paths() {
        let shutdown = Arc::new(AtomicBool::new(false));
        let addr = serve(
            "127.0.0.1:0",
            "live".into(),
            Arc::new(metrics_with_events()),
            shutdown.clone(),
        )
        .unwrap();
        let body = get(addr, "/nope");
        assert!(body.starts_with("HTTP/1.1 404 Not Found"), "{body}");
        shutdown.store(true, Ordering::Relaxed);
    }

    #[test]
    fn serve_refuses_a_non_loopback_bind_without_an_opt_in() {
        let shutdown = Arc::new(AtomicBool::new(false));
        let err = serve(
            "0.0.0.0:0",
            "live".into(),
            Arc::new(metrics_with_events()),
            shutdown.clone(),
        )
        .expect_err("0.0.0.0 must not be reachable without --metrics-allow-remote");
        assert!(err.to_string().contains("non-loopback"), "{err}");
        // The same bind is fine when the caller opts in.
        let addr = serve_allow_remote(
            "0.0.0.0:0",
            "live".into(),
            Arc::new(metrics_with_events()),
            shutdown.clone(),
        )
        .unwrap();
        assert!(addr.ip().is_unspecified());
        shutdown.store(true, Ordering::Relaxed);
    }

    #[test]
    fn serve_rejects_an_oversized_request_line() {
        let shutdown = Arc::new(AtomicBool::new(false));
        let addr = serve(
            "127.0.0.1:0",
            "live".into(),
            Arc::new(metrics_with_events()),
            shutdown.clone(),
        )
        .unwrap();
        // 20 KiB of path: far past MAX_REQUEST_LINE. The server must refuse
        // rather than buffer it, and must still be answering afterwards.
        let mut request = String::from("GET /");
        request.push_str(&"a".repeat(20_000));
        request.push_str(" HTTP/1.1\r\nHost: localhost\r\n\r\n");
        let body = get_raw(addr, &request);
        assert_refused_and_healthy(addr, &body, "an oversized request line");
        shutdown.store(true, Ordering::Relaxed);
    }

    #[test]
    fn serve_rejects_too_many_headers() {
        let shutdown = Arc::new(AtomicBool::new(false));
        let addr = serve(
            "127.0.0.1:0",
            "live".into(),
            Arc::new(metrics_with_events()),
            shutdown.clone(),
        )
        .unwrap();
        let mut request = String::from("GET /metrics HTTP/1.1\r\n");
        for i in 0..(MAX_HEADERS + 10) {
            request.push_str(&format!("X-{i}: v\r\n"));
        }
        request.push_str("\r\n");
        let body = get_raw(addr, &request);
        assert_refused_and_healthy(addr, &body, "too many headers");
        shutdown.store(true, Ordering::Relaxed);
    }
}
