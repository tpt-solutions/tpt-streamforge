# Changelog — tptforge (tpt-stream-cli)

Per-crate history. The workspace-wide log lives in the
[root CHANGELOG](../CHANGELOG.md).

## [Unreleased]

### Added
- `expect` data contracts (`ranges`, `one_of`, `types`) and the `sample` stage in pipeline TOML, `explain`/`validate` and `pipeline.schema.json`.
- **Pipeline authoring** — `${VAR}` / `${VAR:-default}` environment substitution
  in string values (`$${` for a literal `${`; an unset variable is an error that
  names it and its line). Errors report `line L, column C`, the stage index
  (`stage #2`), and a did-you-mean hint; all spec structs use
  `deny_unknown_fields`. New commands: `validate`, `explain`, `run --dry-run`
  (also checks input files exist), `schema-json`, `completions <shell>`,
  `man [--out-dir]`, `init <data-file>`, `doctor [pipeline]`, `convert`,
  `diff A B --key ...`, and `run --watch` (mtime/size polling, std only).
  `pipeline.schema.json` is checked in and a test fails if it drifts from the
  spec structs.
- **Schema drift** — `schema --save FILE` and `schema --against FILE` (exit
  status 2 on added/removed/retyped/reordered columns).
- **Provenance** — `run --manifest FILE` writes counts, per-stage stats,
  and SHA-256 of the spec (pre-substitution, so no secrets) and local
  inputs/outputs; also written, with `status: failed`, when a run fails.
- New dependencies: `clap_complete`, `clap_mangen` (+ `roff`), `sha2`,
  `serde_json`, `tempfile` — all MIT OR Apache-2.0; `cargo deny` stays green.
- **Prometheus metrics endpoint** — `tptforge run --metrics 127.0.0.1:9464`
  serves `/metrics` in the Prometheus text exposition format while the pipeline
  runs, fed by the engine's telemetry stream (rows, batches, per-stage
  rows-in/out, dead-letter rows, and a `running` gauge).
  `--metrics-name` sets the `pipeline` label (default `tptforge`). Off unless
  the flag is passed; built on `std::net::TcpListener` with no new dependency.
  The address is bound before the run starts, so a port conflict fails the
  command rather than silently.
- `tptforge sql "SELECT ..."` — a SQL frontend over the pipeline engine:
  single-table `SELECT` with `WHERE`, `GROUP BY` + `SUM`/`AVG`/`COUNT`/
  `MIN`/`MAX`, aliases, `ORDER BY ... [DESC]`, and `LIMIT`, reading
  CSV/JSONL/JSON (`.gz` supported) from paths or http(s) URLs. Results go
  to stdout as CSV or to `--out FILE`.
- Dockerfile + CI image-build job; `templates/pipeline-starter`.

## [0.1.0] - 2026
Initial CLI: `tptforge run pipeline.yaml` (sources/sinks/stages as in the
README), `tptforge schema`, `tptforge preview`, indicatif progress,
`--quiet`.
