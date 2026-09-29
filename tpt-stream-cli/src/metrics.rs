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

use std::io::{BufRead, BufReader, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tpt_stream_core::telemetry::{StageMetrics, TelemetryEvent};

/// Serve `/metrics` on `addr` until `shutdown` is set.
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
    let listener = TcpListener::bind(addr)?;
    let local = listener.local_addr()?;
    listener.set_nonblocking(true)?;

    std::thread::spawn(move || {
        while !shutdown.load(Ordering::Relaxed) {
            match listener.accept() {
                Ok((stream, _)) => {
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

/// Answer one request. Anything other than `GET /metrics` (or `/`) gets a 404;
/// the connection is always closed, since a scrape endpoint needs no
/// keep-alive.
fn handle_connection(
    mut stream: TcpStream,
    pipeline: &str,
    stats: &Metrics,
) -> std::io::Result<()> {
    stream.set_read_timeout(Some(Duration::from_secs(2)))?;
    let mut reader = BufReader::new(stream.try_clone()?);
    let mut request_line = String::new();
    reader.read_line(&mut request_line)?;
    // Drain headers so the client sees a clean response.
    loop {
        let mut line = String::new();
        if reader.read_line(&mut line)? == 0 || line.trim().is_empty() {
            break;
        }
    }

    let path = request_line.split_whitespace().nth(1).unwrap_or("");
    let (status, body) = if path == "/metrics" || path == "/" {
        ("200 OK", render(pipeline, stats))
    } else {
        ("404 Not Found", "# unknown path\n".to_string())
    };

    write!(
        stream,
        "HTTP/1.1 {status}\r\n\
         Content-Type: text/plain; version=0.0.4; charset=utf-8\r\n\
         Content-Length: {}\r\n\
         Connection: close\r\n\
         \r\n{body}",
        body.len()
    )?;
    stream.flush()
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
        let mut stream = TcpStream::connect(addr).unwrap();
        stream
            .write_all(format!("GET {path} HTTP/1.1\r\nHost: localhost\r\n\r\n").as_bytes())
            .unwrap();
        let mut body = String::new();
        stream.read_to_string(&mut body).unwrap();
        body
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
}
