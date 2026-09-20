# tpt-stream-core

The streaming ETL engine behind tpt-streamforge. Push columnar
[`RecordBatch`]es through a pipeline of `filter` / `map` / `sort` / `dedup`
/ `aggregate` / `join` stages without loading whole datasets into memory.

Dual-licensed MIT / Apache-2.0.

## Features

- **Streaming sources**: CSV, JSONL, JSON arrays, native `.tptcol`
  columnar files, gzip (`.gz`), plain HTTP(S) URLs, SQLite, PostgreSQL,
  S3 / GCS / Azure Blob.
- **Stages**: `filter`, `map`, `select`, `sort` (external merge sort with
  spill), `dedup` (bloom filter + exact set), `aggregate` (hash GROUP BY
  with spill), `join` (hash join with build/probe), `expect` (data-quality
  checks), `limit`.
- **Expression language** for filters and maps: comparisons, Kleene
  three-valued `and`/`or`/`not`, arithmetic, and `coalesce`, `abs`, `sqrt`,
  `min`, `max`, `upper`, `lower`, `length`, `trim`, `if` functions. Date
  and timestamp columns compare against ISO strings directly.
- **Types**: `int32`, `int64`, `float32`, `float64`, `bool`, `string`,
  `date` (ISO `YYYY-MM-DD`), `timestamp` (microseconds, ISO 8601).
- **Telemetry**: progress callbacks and per-stage row/elapsed statistics.
- **Error policies**: `strict` (fail with line numbers), `skip`, or
  `quarantine:<path>` to capture malformed rows.
- **Async by default** (tokio + rayon); `--no-default-features` builds a
  sync-only core used by the WASM target.

Feature flags: `async` (default), `zstd`, `sqlite`, `postgres`, `s3`,
`gcs`, `azure`, `gzip`, `http`.

## Quick start

```rust
use tpt_stream_core::{AggSpec, Pipeline};

#[tokio::main]
async fn main() -> tpt_stream_core::Result<()> {
    let mut pipeline = Pipeline::new();
    pipeline
        .read_csv("in.csv")
        .filter_expr("amount > 0")
        .aggregate(&["region"], &[AggSpec::sum("amount")])
        .write_csv("out.csv");
    let stats = pipeline.execute().await?;
    println!("{} rows in {:?}", stats.rows, stats.elapsed);
    Ok(())
}
```

More in the [workspace README](https://github.com/tpt-solutions/tpt-streamforge)
and the runnable examples in `examples/`.

## Tests

```sh
cargo test -p tpt-stream-core --all-features
```
