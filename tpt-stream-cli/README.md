# tptforge — the tpt-streamforge CLI

`tptforge` runs streaming ETL pipelines described in a single TOML file — no
Rust, Python, or JavaScript required. Under the hood it drives the same
`tpt-stream-core` engine as the language bindings.

## Install

```sh
cargo install tpt-stream-cli          # from crates.io (builds the `tptforge` binary)
cargo binstall tpt-stream-cli         # prebuilt binary via cargo-binstall
cargo install --path tpt-stream-cli   # from a checkout
# or use the published container image (runs as a non-root user):
docker pull ghcr.io/tpt-solutions/tptforge:latest
```

Prebuilt binaries for Linux, macOS and Windows are attached to each GitHub
release together with `SHA256SUMS` and a build-provenance attestation. The
installers verify the checksum before installing:

```sh
curl -fsSLO https://github.com/tpt-solutions/tpt-streamforge/releases/latest/download/install.sh
sh install.sh                      # -> ~/.local/bin/tptforge
```

```powershell
irm https://github.com/tpt-solutions/tpt-streamforge/releases/latest/download/install.ps1 -OutFile install.ps1
.\install.ps1                      # -> %LOCALAPPDATA%\tptforge\bin
```

Verify a download with `gh attestation verify <file> --repo tpt-solutions/tpt-streamforge`.
Scoop and Homebrew manifest templates live in `packaging/`.

## Running a pipeline

```toml
# pipeline.toml
error_policy = "strict"

[source]
csv = { path = "in.csv" }

[[stages]]
filter = "amount > 0"

[[stages]]
# map replaces the schema with the columns you list, so keep `region` for the
# group-by below.
map = { region = "region", total = "amount * 2" }

[[stages]]
aggregate = { group_by = ["region"], aggs = { total = "sum", "*" = "count_all" } }

[[stages]]
sort = { columns = ["sum_total"], descending = true }

[[stages]]
expect = { rows_at_least = 1 }

[sink]
csv = "out.csv"
```

```sh
tptforge run pipeline.toml
# 1000 rows in 16 batch(es), 2048 bytes out, in 41.2ms
```

Every `key = value` above also accepts the table form, which is easier to
read for long option lists:

```toml
[source.csv]
path = "in.csv"
chunk_rows = 65536

[[stages]]
[stages.aggregate]
group_by = ["region"]

[stages.aggregate.aggs]
total = "sum"
"*" = "count_all"

[sink.csv]
path = "out.csv"
```

Top-level keys are `source` (required), `stages` (array of tables),
`error_policy`, `dead_letter`, and `sink`; unknown keys anywhere are errors. A file named `*.yaml`/`*.yml` is rejected with a
pointer to the TOML layout — pipeline files moved from YAML to TOML so the
CLI no longer depends on an Apache-2.0-only TOML/YAML transitive crate
(`ryu`).

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

- `filter = "<expr>"` — keep rows where the expression is true
- `map = { out_col = "<expr>" }` — replace the schema with computed columns
- `select = [col, ...]` — project columns
- `aggregate = { group_by = [...], aggs = { col = fn, "*" = "count_all" } }` —
  fns: `sum`, `avg`, `count`, `count_all`, `min`, `max`. Output columns are
  named `{fn}_{col}`, except `count_all`: with the `"*"` key it is named
  `count_all`, and with a column key `col = "count_all"` it is named
  `count_col`. The `"*"` form is the usual way to get a row count.
- `limit = n` — keep only the first `n` rows
- `sort = { columns = [...], descending = false }`
- `dedup = [col, ...]`
- `join = { right = "file.csv", left_keys = [...], right_keys = [...], type = "inner"|"left"|"right" }`
- `expect = { rows_at_least = n, rows_at_most = n, no_nulls = [...], unique = [...] }`
  Data contracts (also under `expect`): `ranges = { age = { min = 0, max = 120 } }`,
  `one_of = { region = ["north", "south"] }`, `types = { age = "int32" }`.
- `sample = { fraction = 0.1, key = ["id"], seed = 0 }` — deterministic keyed sample; rows sharing a key are kept or dropped together.

### Sinks

`csv`, `jsonl`, `json` (`pretty = true` optional), `columnar` (`use_zstd`),
`sqlite` (`path`, `table`), `postgres` (`connection`, `table`), `s3`, `gcs`,
`azure` — same options as the sources above. Path-only sinks also accept the
scalar shorthand (`csv = "out.csv"`).

### Error policies

`error_policy` selects how malformed input rows are handled: `"strict"`
(default: fail with a line number), `"skip"`, or `"quarantine:<path>"` (drop
the row and capture it).

### Environment variables

String values may reference the environment, so passwords stay out of files:

```toml
[source.postgres]
connection = "host=db user=etl password=${DB_PASSWORD}"
query = "SELECT * FROM orders"

[sink.csv]
path = "${OUT_DIR:-out}/orders.csv"   # default when OUT_DIR is unset or empty
```

- `${VAR}` is replaced by the variable's value; if it is **unset** the command
  fails, naming the variable and the line (set-but-empty is allowed).
- `${VAR:-default}` uses `default` when `VAR` is unset or empty.
- `$${` is an escape: it produces a literal `${` (e.g. `$${HOME}`).
- A lone `$` is left alone. Substitution applies to string *values*, never keys,
  and runs after parsing, so a value containing quotes cannot break the TOML.
- `--manifest` hashes the file as written, before substitution.

## Authoring tools

```sh
tptforge validate pipeline.toml     # syntax, options, expressions; non-zero on error
tptforge validate pipeline.toml --no-env   # unset ${VAR} become <VAR> (CI without secrets)
tptforge explain pipeline.toml      # numbered plan: source, stage 1..n, sink
tptforge run pipeline.toml --dry-run       # validate + plan, also checks inputs exist
tptforge init data.csv --out pipeline.toml # commented starter with expect checks
tptforge run pipeline.toml --watch         # rerun when the file or a local input changes
tptforge doctor [pipeline.toml]     # temp dir, credentials, pipeline sanity
```

Errors point at the spot and suggest fixes:

```
tptforge: parsing pipeline TOML p.toml: line 11, column 1: stage #2: invalid stage "sort":
unknown field `colums`, expected `columns` or `descending` -- did you mean `columns`?
```

`validate` and `--dry-run` do not read column values, so they cannot catch a
misspelled *column name*; that still surfaces at run time (or use `preview`).
`--watch` polls modification time and size every 500 ms with `std` only, keeps
watching after a failed run, and cannot be combined with `--metrics`.

### Editor autocomplete

`tpt-stream-cli/pipeline.schema.json` is a JSON Schema (draft-07) for pipeline
files; `tptforge schema-json` prints the same document. With Taplo / "Even
Better TOML" add `#:schema ./pipeline.schema.json` as the first line, or point
`evenBetterToml.schema.associations` at it. The file is generated from
`src/schema_json.rs` and a test fails if it goes stale
(`TPT_UPDATE_SCHEMA=1 cargo test -p tpt-stream-cli schema` regenerates it).

### Provenance manifest

```sh
tptforge run pipeline.toml --manifest run.json
```

writes JSON with the tool version, start/finish time, `spec_sha256`, every local
input and output (`path`, `bytes`, `sha256`), run totals, and per-stage
rows in/out and timing. Inputs are hashed before the run. A failed run still
writes a manifest, with `"status": "failed"` and the error.

## Metrics

`--metrics` serves a Prometheus endpoint for the duration of the run:

```sh
tptforge run pipeline.toml --metrics 127.0.0.1:9464
curl http://127.0.0.1:9464/metrics
```

```
tptforge_rows{pipeline="tptforge"} 1048576
tptforge_batches{pipeline="tptforge"} 16
tptforge_stage_rows{pipeline="tptforge",stage="filter",dir="in"} 1048576
tptforge_stage_rows{pipeline="tptforge",stage="filter",dir="out"} 812345
tptforge_dead_letter_rows{pipeline="tptforge"} 0
tptforge_running{pipeline="tptforge"} 1
```

Counters come from the engine's telemetry stream, so they match
`Pipeline::stage_stats()`. `--metrics-name` sets the `pipeline` label
(default `tptforge`). The address is bound *before* the pipeline starts, so a
port conflict fails the command rather than silently doing nothing. Combine
freely with `--quiet`; the two drive one telemetry hook.

The endpoint is **loopback-only by default**: `--metrics 0.0.0.0:9464` is
refused, because it is unauthenticated and binding all interfaces would publish
a run's row counts to the network. Pass `--metrics-allow-remote` to opt in. The
request line and header block are capped (8 KiB / 100 headers / 16 KiB, answered
with `431`), and each connection has a 5 s deadline, so a stalled or hostile
client cannot wedge the single-threaded server for the length of the run.

Off unless the flag is passed, and it adds no dependency — the endpoint is
`std::net::TcpListener` and a small text-format renderer. OpenTelemetry is not
offered: an OTLP exporter would add a dependency this project deliberately
avoids.

## SQL

```sh
tptforge sql "SELECT region, SUM(amount) AS total FROM 'in.csv'   WHERE amount > 0 GROUP BY region ORDER BY total DESC LIMIT 10"
tptforge sql "SELECT day, amount * 2 AS doubled FROM 'events.csv.gz'   WHERE day >= '2024-01-01'" --out totals.csv
```

Supported: single-table `SELECT` with column/arithmetic projections
(aliases via `AS`), `DISTINCT`, `WHERE` (comparisons, `AND`/`OR`/`NOT`,
`IS [NOT] NULL`, arithmetic), `GROUP BY` with `SUM`/`AVG`/`COUNT`/`MIN`/`MAX`,
`ORDER BY ... [ASC|DESC]`, and `LIMIT`. `FROM` accepts any file the sources
support (CSV/JSONL/JSON/`.tptcol`, `.gz`, http(s) URLs). Unsupported SQL fails
with an explicit error. Results print as CSV; `--out FILE` writes them instead.

`DISTINCT` dedups on the *projected* columns, so `SELECT DISTINCT region,
product` keeps two `north` rows that differ in `product`; `SELECT DISTINCT *`
compares whole rows. It runs after `WHERE` and before `ORDER BY`/`LIMIT`, and
it is skipped when `GROUP BY` is present (which already collapses the keys).
`DISTINCT ON (...)` (Postgres-style) is not supported.

## Inspecting data

```sh
tptforge schema data.csv.gz        # name<TAB>type per column
tptforge preview data.csv -n 20    # first 20 rows as CSV
tptforge preview https://example.com/data.jsonl
tptforge convert data.csv data.tptcol --zstd   # csv | jsonl | ndjson | json | tptcol, by extension
```

### Schema drift

```sh
tptforge schema data.csv --save schema.json      # record today's schema
tptforge schema data.csv --against schema.json   # later: exit status 2 on drift
```

Drift means a column was added, removed, retyped, or reordered; the report lists
each one (`- column removed: ...`, `+ column added: ...`,
`~ column type changed: x: int64 -> string`). Types come from the sampled rows
(`--rows`, default 1000), the same inference as `schema`.

### Comparing two files

```sh
tptforge diff old.csv new.csv --key id [--out changes.csv] [--exit-code]
```

Both files are sorted by the key with the engine's external sort (spilling to
disk) and compared in one merge pass. Output is CSV with a leading `_diff`
column: `-` for a row only in A (or A's version of a changed row), `+` for a row
only in B (or B's version). Keys must be unique in each file and have the same
type in both; columns present on one side only are ignored (and noted on
stderr). Counts go to stderr; `--exit-code` exits 1 when the files differ.
Comparison of the sorted streams is bounded-memory, but the sort stage itself
currently hands its merged output back in one piece, so peak memory is one
sorted copy of each input.

## Shell completions and man page

```sh
tptforge completions bash > /etc/bash_completion.d/tptforge   # bash|zsh|fish|powershell|elvish
tptforge completions powershell | Out-String | Invoke-Expression
tptforge man > tptforge.1                 # main page
tptforge man --out-dir man/              # tptforge.1 + tptforge-<command>.1
```

## Tests

```sh
cargo test -p tpt-stream-cli
```
