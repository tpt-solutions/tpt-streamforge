//! Deduplicate, sort, and aggregate a messy CSV in one streaming pipeline.
//!
//! Run: `cargo run -p tpt-stream-core --example dedup_sort_pipeline --features async`

use std::io::Write;

use tpt_stream_core::{AggSpec, Pipeline, Result};

fn main() -> Result<()> {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(run())
}

async fn run() -> Result<()> {
    let dir = std::env::temp_dir().join("tpt-example-dedup-sort");
    std::fs::create_dir_all(&dir).unwrap();

    let input = dir.join("messy.csv");
    let output = dir.join("summary.csv");

    {
        let mut f = std::io::BufWriter::new(std::fs::File::create(&input).unwrap());
        writeln!(f, "event,user,minutes").unwrap();
        for i in 0..5_000 {
            let user = format!("user{}", i % 200);
            // duplicates: every 7th row repeats exactly
            if i % 7 == 0 {
                writeln!(f, "login,{user},{}", i % 60).unwrap();
            }
            writeln!(f, "login,{user},{}", i % 60).unwrap();
        }
    }

    let mut pipeline = Pipeline::new();
    pipeline
        .read_csv(input.to_string_lossy())
        .dedup(&["event", "user", "minutes"])
        .aggregate(
            &["user"],
            &[AggSpec::sum("minutes"), AggSpec::count_all("events")],
        )
        .sort_by_desc(&["sum_minutes"])
        .write_csv(output.to_string_lossy());
    let stats = pipeline.execute().await?;

    println!(
        "{} unique events aggregated into {} users -> {}",
        stats.rows,
        stats.rows,
        output.display()
    );
    Ok(())
}
