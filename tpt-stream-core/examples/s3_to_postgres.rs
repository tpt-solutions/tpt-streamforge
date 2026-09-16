//! End-to-end cloud ETL: stream an S3 object, transform, bulk-load Postgres.
//!
//! Set the environment before running:
//! - `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` (or `AWS_SESSION_TOKEN`)
//! - `TPT_EXAMPLE_S3_BUCKET_URL`, e.g. `https://s3.us-east-1.amazonaws.com/my-bucket`
//! - `TPT_EXAMPLE_POSTGRES_URL`, e.g. `postgres://postgres:postgres@localhost:5432/postgres`
//!
//! Run: `cargo run -p tpt-stream-core --example s3_to_postgres --features "s3,postgres"`

use tpt_stream_core::{CloudCredentials, Pipeline, Result};

fn main() -> Result<()> {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(run())
}

async fn run() -> Result<()> {
    let Ok(bucket_url) = std::env::var("TPT_EXAMPLE_S3_BUCKET_URL") else {
        eprintln!("set TPT_EXAMPLE_S3_BUCKET_URL and TPT_EXAMPLE_POSTGRES_URL to run this example");
        return Ok(());
    };
    let Ok(postgres_url) = std::env::var("TPT_EXAMPLE_POSTGRES_URL") else {
        eprintln!("set TPT_EXAMPLE_POSTGRES_URL to run this example");
        return Ok(());
    };
    let creds = CloudCredentials::from_env()?;

    let mut pipeline = Pipeline::new();
    pipeline
        .read_s3(&bucket_url, "incoming/events.csv", &creds)?
        .filter_expr("amount > 0")
        .aggregate(&["region"], &[tpt_stream_core::AggSpec::sum("amount")])
        .write_postgres(&postgres_url, "region_totals");
    let stats = pipeline.execute().await?;

    println!(
        "{} source rows aggregated into `region_totals` ({} bytes) in {:.1?}",
        stats.rows, stats.bytes_out, stats.elapsed
    );
    Ok(())
}
