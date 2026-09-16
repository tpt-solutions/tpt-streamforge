//! Convert a CSV file to newline-delimited JSON.
//!
//! Run: `cargo run -p tpt-stream-core --example csv_to_jsonl --features "async"`

use std::io::Write;

use tpt_stream_core::Pipeline;

fn main() -> tpt_stream_core::Result<()> {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(run())
}

async fn run() -> tpt_stream_core::Result<()> {
    let dir = std::env::temp_dir().join("tpt-example-csv-to-jsonl");
    std::fs::create_dir_all(&dir).unwrap();

    let csv_path = dir.join("events.csv");
    let jsonl_path = dir.join("events.jsonl");

    {
        let mut f = std::io::BufWriter::new(std::fs::File::create(&csv_path).unwrap());
        writeln!(f, "id,region,amount").unwrap();
        for i in 0..10_000 {
            writeln!(f, "{},region{},{}", i, i % 5, i * 7 % 100).unwrap();
        }
    }

    let mut pipeline = Pipeline::new();
    pipeline
        .read_csv(csv_path.to_string_lossy())
        .filter_expr("amount > 50")
        .write_jsonl(jsonl_path.to_string_lossy());
    let stats = pipeline.execute().await?;

    println!("wrote {} rows to {}", stats.rows, jsonl_path.display());
    Ok(())
}
