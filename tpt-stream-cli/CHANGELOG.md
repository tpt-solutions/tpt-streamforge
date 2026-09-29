# Changelog — tptforge (tpt-stream-cli)

Per-crate history. The workspace-wide log lives in the
[root CHANGELOG](../CHANGELOG.md).

## [Unreleased]

### Added
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
