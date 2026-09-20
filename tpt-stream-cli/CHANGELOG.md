# Changelog — tptforge (tpt-stream-cli)

Per-crate history. The workspace-wide log lives in the
[root CHANGELOG](../CHANGELOG.md).

## [Unreleased]

### Added
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
