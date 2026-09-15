# tpt-streamforge — Python

A streaming ETL engine in Rust with a Python wrapper (PyO3). Process CSV files
row-by-row with constant memory: filter, map, aggregate, sort, join and dedup
before writing the result, without ever loading the whole dataset.

## Install

```bash
pip install tpt-streamforge
```

Wheels are published for Linux (manylinux), macOS, and Windows, Python 3.9+
(abi3).

To build locally in an editable env:

```bash
python -m pip install maturin
maturin develop --manifest-path tpt-stream-py/Cargo.toml
```

## Quick start

```python
from tpt_streamforge import Pipeline

stats = (
    Pipeline()
    .read_csv("transactions.csv")          # chunk_size defaults to 65,536 rows
    .filter("amount > 100 and status == 'ok'")
    .map({
        "net": "amount - fee",
        "who": "upper(customer)",
    })
    .write_csv("big_transactions.csv")
    .execute()
)
print(stats)  # {'rows': ..., 'batches': ..., 'bytes_in': ..., 'bytes_out': ...}
```

Every stage method returns the same `Pipeline`, so calls chain naturally.
`execute()` runs the pipeline and returns a stats dict (`rows`, `batches`,
`bytes_in`, `bytes_out`).

## API

### `Pipeline()`
Create an empty pipeline.

### `.read_csv(path, chunk_size=None)`
Read a CSV with a header row. `chunk_size` rows are buffered per batch
(0/`None` → engine default 65,536).

### `.filter(expr)`
Keep rows where the expression is boolean true. Grammar: comparisons
(`==`, `!=`, `<`, `<=`, `>`, `>=`), `and`, `or`, `not`, parens, arithmetic
(`+ - * / %`), and functions `coalesce`, `abs`, `sqrt`, `min`, `max`,
`upper`, `lower`, `length`, `trim`, `if`. Nulls propagate (a comparison with
a null is null → row dropped by `filter`). Number types widen automatically
(int + float → float).

### `.map({output_column: expr})`
Project rows to the given computed columns; the row schema is replaced with
exactly the mapped columns (existing columns are not preserved).

### `.group_by(columns).agg({column: fn})`
Aggregate with `fn` in `sum`, `avg`, `count`, `count_all`, `min`, `max`.
Output columns are named `{fn}_{column}` (except `count_all`).

```python
Pipeline().read_csv("in.csv").group_by(["country"]).agg({"amount": "sum"})
```

### `.sort(columns, descending=False)`
Stable external merge sort (spills to disk under the system temp dir when the
data exceeds memory).

### `.dedup(columns)`
Bloom-filter + exact-set dedup.

### `.write_csv(path)`
Write the streamed result to a CSV file.

### `.on_progress(callback)`
Register a progress callback invoked while the pipeline runs. The callable
receives a dict per event; `event["event"]` is one of:

- `"source_batch"`: a batch was read — keys `rows`, `total_rows`, `batches`
- `"stage_batch"`: a stage finished a batch — keys `stage`, `rows_in`, `rows_out`
- `"sink_batch"`: a batch was written — keys `rows`, `total_rows`
- `"done"`: the pipeline finished — keys `rows`, `batches`, `elapsed_ms`

Exceptions raised inside the callback are reported on stderr and do not abort
the pipeline.

```python
events = []
Pipeline().read_csv("in.csv").on_progress(events.append).write_csv("out.csv").execute()
kinds = [e["event"] for e in events]  # ["source_batch", "stage_batch", "sink_batch", ..., "done"]
```

### `.stage_stats()`
Cumulative per-stage metrics from the last `execute()` run: a list of dicts
with `name`, `rows_in`, `rows_out`, `batches`, `elapsed_ms`, and
`rows_per_sec` (empty before the first run).

### `.execute()`
Run the pipeline (blocking). Raises `TptError` on expression parse errors,
schema mismatches, or I/O failures.

### `.num_stages()`
Number of transformation stages attached so far (for debugging).

## Memory behaviour

Both input and output stream in fixed-size batches. Aggregate spill, external
merge sort, and hash join spill to `.tptcol` files under the temp directory
when working sets exceed in-memory limits, so a 100 GB CSV can be processed
on a laptop.

## Performance

Rough benchmarks (engine core, Rust): CSV reading ≈ 7M rows/s; 10M-row
`GROUP BY SUM` ≈ 3.5 s.

## Development

```bash
cargo test --workspace --all-features
python -m pip install maturin pytest
maturin develop --manifest-path tpt-stream-py/Cargo.toml
python -m pytest tpt-stream-py/python/tests
```