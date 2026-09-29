//! Telemetry hooks for pipeline execution: per-stage rows/elapsed, sink bytes,
//! and a progress callback fired as batches flow through the pipeline.
//!
//! Attach a hook with [`crate::Pipeline::on_progress`]; it receives a
//! [`TelemetryEvent`] for every source batch, stage output, sink write, and at
//! completion. Aggregate per-stage numbers are available after `execute()`
//! via [`crate::Pipeline::stage_stats`].

use std::sync::Arc;
use std::time::Duration;

/// Cumulative metrics for one pipeline stage, accumulated across all batches.
#[derive(Debug, Clone, Default)]
pub struct StageMetrics {
    /// Stage name (e.g. `"Filter"`, `"GroupByAgg"`).
    pub name: String,
    pub rows_in: u64,
    pub rows_out: u64,
    pub batches: u64,
    pub elapsed: Duration,
}

impl StageMetrics {
    /// Rows per second through this stage (0 when no time was spent).
    pub fn rows_per_sec(&self) -> f64 {
        let secs = self.elapsed.as_secs_f64();
        if secs > 0.0 {
            self.rows_in as f64 / secs
        } else {
            0.0
        }
    }
}

/// Emitted by the pipeline runner at each observable step of `execute()`.
#[derive(Debug, Clone)]
pub enum TelemetryEvent {
    /// A batch was read from the source.
    SourceBatch {
        rows: u64,
        total_rows: u64,
        batches: u64,
    },
    /// A stage finished processing one batch.
    StageBatch {
        stage: String,
        rows_in: u64,
        rows_out: u64,
    },
    /// A batch was written to the sink.
    SinkBatch { rows: u64, total_rows: u64 },
    /// The pipeline finished.
    Done {
        rows: u64,
        batches: u64,
        elapsed: Duration,
    },
}

/// Progress hook: a shared closure receiving each [`TelemetryEvent`].
pub type ProgressHook = Arc<dyn Fn(&TelemetryEvent) + Send + Sync>;

/// Verbosity of an instrumented event. A thin alias so call sites don't need
/// `cfg` blocks of their own: with the `tracing` feature on these are
/// `tracing`'s levels, and with it off they are inert placeholders that the
/// macro discards.
#[cfg(feature = "tracing")]
pub use tracing::Level;
#[cfg(not(feature = "tracing"))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Level {
    DEBUG,
    INFO,
}

#[macro_use]
#[path = "telemetry/macros.rs"]
mod trace_macros;

/// Mirror a [`TelemetryEvent`] into `tracing`.
///
/// Called from the pipeline runner next to the existing `ProgressHook`
/// dispatch, so a subscriber sees the same events whether it hooks the
/// callback or installs a `tracing` subscriber — they are independent and can
/// be used together.
pub(crate) fn emit_tracing(event: &TelemetryEvent) {
    match event {
        TelemetryEvent::SourceBatch {
            rows,
            total_rows,
            batches,
        } => {
            // With the `tracing` feature off the field values are never read,
            // so the macro discards them; the leading-underscore locals keep
            // the match arms identical either way.
            let (_rows, _total, _batches) = (*rows, *total_rows, *batches);
            crate::trace_event!(
                "tpt_stream_core::pipeline",
                crate::telemetry::Level::DEBUG,
                rows,
                total_rows,
                batches,
                "source batch"
            );
        }
        TelemetryEvent::StageBatch {
            stage,
            rows_in,
            rows_out,
        } => {
            let (_stage, _in, _out) = (stage, rows_in, rows_out);
            crate::trace_event!(
                "tpt_stream_core::pipeline",
                crate::telemetry::Level::DEBUG,
                stage,
                rows_in,
                rows_out,
                "stage batch"
            );
        }
        TelemetryEvent::SinkBatch { rows, total_rows } => {
            let (_rows, _total) = (rows, total_rows);
            crate::trace_event!(
                "tpt_stream_core::pipeline",
                crate::telemetry::Level::DEBUG,
                rows,
                total_rows,
                "sink batch"
            );
        }
        TelemetryEvent::Done {
            rows,
            batches,
            elapsed,
        } => {
            let (_rows, _batches, _elapsed) = (rows, batches, elapsed);
            crate::trace_event!(
                "tpt_stream_core::pipeline",
                crate::telemetry::Level::INFO,
                rows,
                batches,
                elapsed_ms = elapsed.as_millis() as u64,
                "pipeline finished"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rows_per_sec_handles_zero_elapsed() {
        let m = StageMetrics::default();
        assert_eq!(m.rows_per_sec(), 0.0);
    }
}
