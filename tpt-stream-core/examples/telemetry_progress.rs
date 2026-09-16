//! Watch pipeline telemetry: progress events and per-stage stats.
//!
//! Run: `cargo run -p tpt-stream-core --example telemetry_progress --features async`

use std::io::Write;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use tpt_stream_core::{Pipeline, Result, TelemetryEvent};

fn main() -> Result<()> {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(run())
}

async fn run() -> Result<()> {
    let dir = std::env::temp_dir().join("tpt-example-telemetry");
    std::fs::create_dir_all(&dir).unwrap();

    let input = dir.join("clicks.csv");
    let output = dir.join("clicks-out.csv");
    {
        let mut f = std::io::BufWriter::new(std::fs::File::create(&input).unwrap());
        writeln!(f, "user,ms").unwrap();
        for i in 0..100_000 {
            writeln!(f, "user{},{}", i % 500, i % 1_000).unwrap();
        }
    }

    let batches_seen = Arc::new(AtomicU64::new(0));
    let counter = batches_seen.clone();

    let mut pipeline = Pipeline::new();
    pipeline
        .read_csv(input.to_string_lossy())
        .on_progress(Arc::new(move |event: &TelemetryEvent| match event {
            TelemetryEvent::SourceBatch { total_rows, .. } => {
                let _ = counter.fetch_add(1, Ordering::Relaxed);
                eprintln!("\r[progress] {total_rows} rows read...");
            }
            TelemetryEvent::Done { rows, elapsed, .. } => {
                eprintln!("\n[done] {rows} rows in {elapsed:.1?}");
            }
            _ => {}
        }))
        .filter_expr("ms > 100")
        .aggregate(&["user"], &[tpt_stream_core::AggSpec::sum("ms")])
        .write_csv(output.to_string_lossy());
    let stats = pipeline.execute().await?;

    eprintln!(
        "\nsource batches seen: {}",
        batches_seen.load(Ordering::Relaxed)
    );
    for (i, stage) in pipeline.stage_stats().iter().enumerate() {
        eprintln!(
            "stage {i} ({}): {} in / {} out in {:.1?}",
            stage.name, stage.rows_in, stage.rows_out, stage.elapsed
        );
    }
    eprintln!("output -> {} ({} rows)", output.display(), stats.rows);
    Ok(())
}
