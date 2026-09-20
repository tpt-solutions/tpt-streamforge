# tptforge examples

Two ways to see the CLI in action:

```sh
# 1. Run the annotated pipeline (input.csv -> filter/map/expect/aggregate/sort/select -> report.csv)
#    Run from tpt-stream-cli/ so the relative paths in pipeline.toml resolve:
cd tpt-stream-cli
cargo run -- run examples/pipeline.toml
cat examples/report.csv

# 2. Same library API from Rust: print the plan, run it, print per-stage stats
#    (works from anywhere; it reads the bundled examples/pipeline.toml)
cargo run --example inspect_pipeline

# 3. Inspect data without a pipeline file
cargo run -- schema examples/input.csv
cargo run -- preview examples/input.csv -n 3
cargo run -- sql \
  "SELECT region, SUM(amount) AS total FROM 'examples/input.csv' WHERE amount > 0 GROUP BY region ORDER BY total DESC"
```

## Files

- `pipeline.toml` — annotated source → stages → sink definition
- `input.csv` — 10 fabricated orders
- `inspect_pipeline.rs` — embeds the spec + engine in Rust (`explain()` + `stage_stats()`)
- `report.csv` — written by the pipeline (created on first run)
