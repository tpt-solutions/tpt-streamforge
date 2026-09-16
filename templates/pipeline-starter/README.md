# tpt-streamforge pipeline starter

A ready-to-run skeleton for a `tptforge` ETL pipeline.

## Use it

1. Copy this folder (or `cargo generate` / `cookiecutter` it) into a new repo.
2. Drop your input at `input.csv` or point `pipeline.yaml` at a URL/database.
3. Edit the stages (see the annotated `pipeline.yaml`).
4. Run:

```sh
tptforge run pipeline.yaml          # cargo install --path tpt-stream-cli
# or without installing anything:
docker build -t tptforge .. && docker run --rm -v "$PWD:/data" tptforge run pipeline.yaml
```

## Validate data before it ships

The `expect` stage is your data-quality gate: a failed check aborts the run
with a non-zero exit code, which makes this pattern CI-safe:

```sh
tptforge run pipeline.yaml && tptforge schema output.csv
```

## Files

- `pipeline.yaml` — annotated pipeline definition (source → stages → sink)
- `input.csv` — placeholder input; replace with your data or a source URL
