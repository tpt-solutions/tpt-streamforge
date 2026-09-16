//! Inner-join two CSV files and write the result.
//!
//! Run: `cargo run -p tpt-stream-core --example join_two_files --features async`

use std::io::Write;

use tpt_stream_core::{Pipeline, Result};

fn main() -> Result<()> {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(run())
}

async fn run() -> Result<()> {
    let dir = std::env::temp_dir().join("tpt-example-join");
    std::fs::create_dir_all(&dir).unwrap();

    let orders = dir.join("orders.csv");
    let customers = dir.join("customers.csv");
    let out = dir.join("enriched.csv");

    {
        let mut f = std::io::BufWriter::new(std::fs::File::create(&orders).unwrap());
        writeln!(f, "customer_id,amount").unwrap();
        for i in 0..1_000 {
            writeln!(f, "{},{}", i % 50, i * 3 % 90).unwrap();
        }
    }
    {
        let mut f = std::io::BufWriter::new(std::fs::File::create(&customers).unwrap());
        writeln!(f, "customer_id,name").unwrap();
        for i in 0..50 {
            writeln!(f, "{i},customer{i}").unwrap();
        }
    }

    let mut pipeline = Pipeline::new();
    pipeline
        .read_csv(orders.to_string_lossy())
        .filter_expr("amount > 10")
        .join_csv(
            customers.to_string_lossy().as_ref(),
            &["customer_id"],
            &["customer_id"],
            tpt_stream_core::JoinType::Inner,
        )?
        .write_csv(out.to_string_lossy());
    let stats = pipeline.execute().await?;

    println!("{} enriched orders -> {}", stats.rows, out.display());
    Ok(())
}
