# tptforge — the tpt-streamforge CLI

`tptforge` runs streaming ETL pipelines described in a single YAML file — no
Rust, Python, or JavaScript required. Under the hood it drives the same
`tpt-stream-core` engine as the language bindings.

## Install

```sh
cargo install --path tpt-stream-cli
# or use the Docker image (build locally; no registry image is published yet):
docker build -t tptforge .
```

## Running a pipeline

```yaml
# pipeline.yaml
source:
  csv: { path: in.csv }
error_policy: strict
stages:
  - filter: "amount > 0"
  - map: { total: "amount * 2" }
  - aggregate: { group_by: [region], aggs: { total: sum, "*": count_all } }
  - sort: { columns: [sum_total], descending: true }
  - expect: { rows_at_least: 1 }
sink:
  csv: out.csv
```

```sh
tptforge run pipeline.yaml
# 1000 rows in 16 batch(es), 2048 bytes out, in 41.2ms
```

### Sources

| key | options |
| --- | --- |
| `csv` | `path`, `chunk_rows?` — `.gz` files decompress automatically |
| `jsonl` | `path`, `chunk_rows?` — `.gz` supported |
| `json` | `path`, `chunk_rows?` — a top-level JSON array |
| `columnar` | `path` — native `.tptcol` format |
| `http` | `path` — an http(s) URL; format from the extension |
| `sqlite` | `path`, `query`, `chunk_rows?` |
| `postgres` | `connection`, `query` |
| `s3` | `bucket_url`, `key` — creds from `AWS_*` env vars |
| `gcs` | `bucket`, `key` — HMAC key in `AWS_*` env vars |
| `azure` | `account_url`, `container`, `key` — creds from `AZURE_*` env vars |

### Stages

- `filter: "<expr>"` — keep rows where the expression is true
- `map: { out_col: "<expr>", ... }` — replace the schema with computed columns
- `select: [col, ...]` — project columns
- `aggregate: { group_by: [...], aggs: { col: fn, "*": count_all } }` — fns:
  `sum`, `avg`, `count`, `count_all`, `min`, `max`
- `sort: { columns: [...], descending: false }`
- `dedup: [col, ...]`
- `join: { right: file.csv, left_keys: [...], right_keys: [...], type: inner|left|right }`
- `expect: { rows_at_least: n, rows_at_most: n, no_nulls: [...], unique: [...] }`

### Sinks

`csv`, `jsonl`, `json` (`pretty: true` optional), `columnar` (`use_zstd`),
`sqlite` (`path`, `table`), `postgres` (`connection`, `table`), `s3`, `gcs`,
`azure` — same options as the sources above.

### Error policies

`error_policy` selects how malformed input rows are handled: `strict`
(default: fail with a line number), `skip`, or `quarantine:<path>` (drop the
row and capture it).

## SQL

```sh
tptforge sql "SELECT region, SUM(amount) AS total FROM 'in.csv'   WHERE amount > 0 GROUP BY region ORDER BY total DESC LIMIT 10"
tptforge sql "SELECT day, amount * 2 AS doubled FROM 'events.csv.gz'   WHERE day >= '2024-01-01'" --out totals.csv
```

Supported: single-table `SELECT` with column/arithmetic projections
(aliases via `AS`), `WHERE` (comparisons, `AND`/`OR`/`NOT`, `IS [NOT] NULL`,
arithmetic), `GROUP BY` with `SUM`/`AVG`/`COUNT`/`MIN`/`MAX`, `ORDER BY ...
[ASC|DESC]`, and `LIMIT`. `FROM` accepts any file the sources support
(CSV/JSONL/JSON/`.tptcol`, `.gz`, http(s) URLs). Unsupported SQL fails with
an explicit error. Results print as CSV; `--out FILE` writes them instead.

## Inspecting data

```sh
tptforge schema data.csv.gz        # name<TAB>type per column
tptforge preview data.csv -n 20    # first 20 rows as CSV
tptforge preview https://example.com/data.jsonl
```

## Tests

```sh
cargo test -p tpt-stream-cli
```
