# tpt-streamforge Python examples

```sh
# from the repo root, with the package built:
maturin develop -m tpt-stream-py/Cargo.toml
python tpt-stream-py/examples/sales_report.py
```

- `sales_report.py` — filter → map → `expect` → group-by → sort → CSV, with
  `explain()`, `on_progress`, `stage_stats()`, and `preview()` printed.

The Python API mirrors the Rust engine: `read_csv` / `read_jsonl` /
`read_json` / `read_columnar` / `read_sqlite` / `read_postgres` /
`read_s3` / `read_gcs` / `read_azure` / `read_http`, the `filter` / `map` /
`select` / `dedup` / `sort` / `join_csv` stages, `group_by(...).agg({...})`,
the matching `write_*` sinks, plus `execute()`, `collect()`, `to_pandas()`,
`preview(n)`, `explain()`, `on_error()`, and `expect()`.
