# Why tpt-streamforge

## The problem

Data engineers moving CSV/JSON/JSONL data between systems are stuck choosing
between three imperfect options:

- **pandas / Python DataFrames** — easy to use, but load the entire file into
  RAM. A file bigger than available memory simply crashes the process, and
  every operation pays a full DataFrame-build cost even for a single filter.
- **Node.js streams** — memory-efficient, but implementing joins,
  aggregations, external sorts, and dedup by hand on top of raw streams is
  slow to write and slow to run.
- **Polars / DuckDB** — fast, well-built analytical engines, but they
  materialize the working relation to execute a query and bring large,
  fast-moving dependency trees (SIMD kernels, their own execution engines,
  Arrow) that are harder to audit and don't build for tiny targets like WASM
  or embedded C ABIs.

None of them is a good fit for "run a bounded-memory ETL pipeline that also
needs to run identically in a Python job, a browser tab, and a Rust service."

## What tpt-streamforge does differently

**Constant memory, not best-effort memory.** The engine processes data in
fixed-size chunks (65,536 rows by default) end to end — source, filter, map,
aggregate, sort, join, sink. Memory usage is bounded by chunk size, not file
size, and sort/aggregate/join spill to disk (`.tptcol`) when their working
set grows. A CSV far larger than RAM runs the same way a small one does.

**One engine, four runtimes, same semantics.** The core is pure Rust
(`tpt-stream-core`), and the same pipeline logic is exposed through PyO3
(`pip install tpt-streamforge`), wasm-bindgen for Node and browsers
(`tpt-streamforge-node` / `tpt-streamforge-browser`), a C ABI
(`tpt-stream-ffi`), and a standalone CLI (`tptforge`). There's no second
implementation to keep in sync — Python, JS, and the CLI all call into the
same Rust stages, so a pipeline behaves the same whether it runs in a
Jupyter notebook, a browser tab (fully client-side via WASM — data never
leaves the machine), or a server binary.

**A minimal, audited dependency tree by policy, not accident.** The project
explicitly rejects heavy dependencies — no `parquet`, no `wasmtime`, no
Arrow — even when they're permissively licensed, and builds narrower
replacements instead (a custom `.tptcol` columnar format, no WASM plugin
runtime). `cargo deny` enforces this in CI: any copyleft-licensed dependency
(GPL/LGPL/AGPL/SSPL, and even weak-copyleft MPL-2.0) fails the build. That
matters for anyone embedding this in a commercial product who can't take on
license risk or an unauditable dependency graph the way you can with
Polars's or DuckDB's much larger surface area.

**No cloud SDKs.** S3, GCS, and Azure Blob support is implemented as bare
HTTP + request signing, not via the official (and heavy) cloud SDKs. This
keeps the dependency tree and binary size small and keeps behavior
consistent across the Rust/Python/CLI targets that use it.

**Built-in data quality and introspection, not bolted on.** `explain()` and
`preview(n)` let you see a pipeline's stage plan and its first N output rows
without running it to completion — useful for debugging a pipeline before
committing to a multi-hour run. An `Expect` stage checks row-count bounds,
null constraints, and uniqueness inline, with per-source error policies
(`strict` / `skip` / `quarantine:file.csv`) for malformed rows, so bad data
is a pipeline-level concern instead of something every caller has to
re-implement.

## Where it actually sits versus the alternatives

Benchmarks in [README.md](README.md#performance) (1M-row CSV, same machine)
are honest about the trade-off rather than claiming to beat everyone at
everything:

- **vs. pandas**: 3–4x faster on read-heavy operations because pandas pays a
  full read + DataFrame build on every call; tpt-streamforge streams the
  read while applying the transform.
- **vs. DuckDB**: DuckDB's vectorized engine wins on single-pass analytics
  throughput — it's simply faster per row once data is loaded. What
  tpt-streamforge trades for that is bounded memory: DuckDB materializes the
  relation to execute a query, tpt-streamforge never does, which is the
  point when the input is larger than RAM or memory ceilings are fixed (a
  browser tab, a small container, a laptop).

The pitch isn't "fastest engine" — it's "the one option that gives you
bounded memory, one execution engine across four language runtimes, and a
dependency tree small enough to actually read."

## Who this is for

- Teams that need ETL pipelines to run **identically** in a Python backend
  and a browser-based tool (or a Rust service) without maintaining two
  implementations.
- Anyone processing files that don't reliably fit in memory and can't take
  on a heavyweight query engine to do it.
- Projects with real license-compliance constraints (embedded/commercial
  distribution) that need every dependency, direct and transitive, to be
  MIT/Apache-2.0-compatible and enforced in CI rather than checked by hand.
