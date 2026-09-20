# tpt-streamforge pipeline starter

A ready-to-run skeleton for a `tptforge` ETL pipeline.

## Use it

1. Copy this folder (or `cargo generate` / `cookiecutter` it) into a new repo.
2. Drop your input at `input.csv` or point `pipeline.toml` at a URL/database.
3. Edit the stages (see the annotated `pipeline.toml`).
4. Run:

```sh
tptforge run pipeline.toml          # cargo install --path tpt-stream-cli
# or without installing anything:
docker build -t tptforge .. && docker run --rm -v "$PWD:/data" tptforge run pipeline.toml
```

## Validate data before it ships

The `expect` stage is your data-quality gate: a failed check aborts the run
with a non-zero exit code, which makes this pattern CI-safe:

```sh
tptforge run pipeline.toml && tptforge schema output.csv
```

## Files

- `pipeline.toml` — annotated pipeline definition (source → stages → sink)
- `input.csv` — placeholder input; replace with your data or a source URL
