# tpt-streamforge

A SQL-lite-free streaming transformation engine for tabular data, written in
Rust with bindings for Python and JavaScript/WASM. Push columnar batches through
a pipeline of `filter` / `map` / `sort` / `dedup` / `aggregate` stages without
loading whole datasets into memory.

## What it does

- **Streaming pipelines** on CSV/JSONL/JSON-array input
  (`tpt-stream-core`) — external merge sort, bloom+exact-set dedup, hash-group
  aggregation, join, zstd compression, and an expression language for
  filters/maps.
- **PostgreSQL** (`postgres` feature) — `COPY`-based bulk-load sink and a
  chunked SELECT source.
- **SQLite** (`sqlite` feature) — bulk-insert sink and a chunked SELECT source.
- **Cloud object storage** — S3-compatible endpoints (`s3` feature), Google
  Cloud Storage via the S3 XML API + HMAC keys (`gcs`), and Azure Blob with
  Shared Key auth (`azure`); multipart/block streaming uploads, no cloud SDK
  dependency.
- **Telemetry** — progress callbacks and per-stage row/elapsed stats in Rust,
  Python (`on_progress`/`stage_stats`), and JavaScript (`onProgress`).
- **Python** (`tpt-stream-py`) — a `Pipeline` object via PyO3, published to
  PyPI as `tpt-streamforge`.
- **JavaScript / WASM** (`tpt-stream-wasm`) — an in-memory `Engine` and fluent
  wrappers for Node (`tpt-streamforge-node`) and browsers
  (`tpt-streamforge-browser`), compiled with wasm-bindgen/wasm-pack.
- **C FFI** (`tpt-stream-ffi`) — the C ABI used by the Python package.

## Package layout

| Crate / package | Language | Notes |
| --- | --- | --- |
| `tpt-stream-core` | Rust | streaming engine; async by default, sync-only build for wasm |
| `tpt-stream-columnar` | Rust | native columnar format (`.tptcol`) + optional zstd |
| `tpt-stream-ffi` | Rust/C | C ABI over the core |
| `tpt-stream-py` | Python | `pip install tpt-streamforge` |
| `tpt-stream-wasm/node` | Node.js | `npm install tpt-streamforge-node` |
| `tpt-stream-wasm/browser` | Browser | `npm install tpt-streamforge-browser` |

## Quick start

### Python

```python
from tpt_streamforge import Pipeline

stats = (
    Pipeline()
    .read_csv("in.csv")
    .filter("amount > 10")
    .map({"upper": "upper(name)", "total": "amount * 2"})
    .group_by(["region"]).agg({"amount": "sum"})
    .write_csv("out.csv")
    .execute()
)
print(stats["rows"])
```

### Node.js

```js
const { readCSV } = require('tpt-streamforge-node');

readCSV('in.csv')
  .filter('amount > 10')
  .map(['total'], ['amount * 2'])
  .sort(['total'], true)
  .writeFile('out.csv');
```

### Browser

```js
import { Pipeline } from 'tpt-streamforge-browser';
const p = await Pipeline.fromFile(file);
const csv = p.filter('amount > 10').toCSV();
```

### Rust

```rust
use tpt_stream_core::{AggSpec, Pipeline};

#[tokio::main]
async fn main() -> tpt_stream_core::Result<()> {
    let mut pipeline = Pipeline::new();
    pipeline
        .read_csv("in.csv")
        .filter_expr("amount > 10")
        .map_expr(&[("total", "amount * 2")])
        .aggregate(&["region"], &[AggSpec::sum("amount")])
        .write_csv("out.csv");
    let stats = pipeline.execute().await?;
    println!("{} rows in {:?}", stats.rows, stats.elapsed);
    Ok(())
}
```

## Expression language

Comparisons (`==`, `!=`, `<`, `<=`, `>`, `>=`), `and`/`or`/`not` (Kleene
three-valued logic), parens, arithmetic (`+ - * / %`), and functions:
`coalesce`, `abs`, `sqrt`, `min`, `max`, `upper`, `lower`, `length`, `trim`,
`if`. Number types widen automatically (`int + float -> float`); nulls
propagate through comparisons.

## Performance

Per-stage throughput from the criterion suite (`cargo bench --bench stages`,
`tpt-stream-core`), on a 1M-row / 33 MB CSV with 5 columns:

| stage | time (1M rows) | throughput |
| --- | --- | --- |
| CSV source (read only) | 115 ms | ≈ 8.7M rows/s |
| `filter` | 122 ms | ≈ 8.2M rows/s |
| `map` (new column) | 123 ms | ≈ 8.1M rows/s |
| `select` (3 of 5 columns) | 121 ms | ≈ 8.3M rows/s |
| `aggregate` (GROUP BY 50 keys, SUM+AVG+COUNT) | 403 ms | ≈ 2.5M rows/s |
| `dedup` (unique key, bloom + exact) | 356 ms | ≈ 2.8M rows/s |
| hash `join` (inner, 50-key build side) | 723 ms | ≈ 1.4M rows/s |
| CSV → CSV copy | 832 ms | ≈ 1.2M rows/s |
| `sort` (external merge sort) | 1.32 s | ≈ 0.76M rows/s |

### Comparison with pandas and DuckDB

Same machine (Intel i5-13500, Windows 11, warm file cache), same 1M-row CSV;
every number includes reading the input CSV from disk, because that is what a
streaming pipeline run pays. pandas 3.0.5, DuckDB 1.5.5, Python 3.13. DuckDB
timings measure query execution only (no Python-side row materialization).
Reproduce with `scripts/compare_pandas_duckdb.py`.

| operation | tpt-streamforge | pandas | DuckDB |
| --- | --- | --- | --- |
| read 1M-row CSV | **0.12 s** | 0.41 s | 0.03 s |
| CSV → CSV copy | 0.83 s | 1.31 s | **0.15 s** |
| filter | 0.12 s | 0.43 s | **0.03 s** |
| map (new column) | 0.12 s | 0.42 s | **0.03 s** |
| project 3 of 5 columns | 0.12 s | 0.42 s | **0.03 s** |
| GROUP BY + SUM/AVG/COUNT | 0.40 s | 0.44 s | **0.06 s** |
| sort by score | 1.32 s | **0.52 s** | 0.10 s |
| dedup by unique key | 0.36 s | 0.52 s | **0.10 s** |

How to read this: pandas pays a full read + DataFrame build on every
operation, and tpt-streamforge streams that read 3–4× faster while applying
the transform, staying within a fixed chunk buffer. DuckDB's vectorized
engine is the fastest on single-pass analytics — but it materializes the
whole relation to execute a query, whereas tpt-streamforge's memory stays
bounded by the chunk size (65,536 rows by default) with spill-to-disk for
sort/aggregate/join working sets, which is the trade it makes for the sort
and CSV→CSV numbers above.

## Development

See [CONTRIBUTING.md](CONTRIBUTING.md). Phases and status live in
[todo.md](todo.md).

## License

MIT