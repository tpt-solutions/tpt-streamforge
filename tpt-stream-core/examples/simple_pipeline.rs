use std::env;
use tpt_stream_core::Value;

fn usage() -> ! {
    eprintln!("usage: simple_pipeline <input.csv> <output.csv>");
    std::process::exit(2);
}

#[tokio::main]
async fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() != 3 {
        usage();
    }
    let input = &args[1];
    let output = &args[2];

    let mut pipeline = tpt_stream_core::Pipeline::new();
    pipeline
        .read_csv(input)
        .filter(|row| {
            let score = match row.get("score") {
                Some(Value::Float64(v)) => v,
                _ => 0.0,
            };
            score > 0.0
        })
        .write_csv(output);
    let stats = pipeline.execute().await.expect("pipeline failed");
    eprintln!("processed {} rows in {} batches", stats.rows, stats.batches);
}
