# AGENTS.md — tpt-streamforge

## Quick commands

Use the `justfile` for everything:

```sh
just --list          # all recipes
just ci              # fmt + clippy + deny + test (the CI gates)
just test            # cargo test --workspace --all-features --exclude tpt-streamforge
just clippy          # cargo clippy --workspace --all-features -- -D warnings
just deny            # cargo deny check licenses bans sources
just bench           # criterion benches in tpt-stream-core
just py              # maturin develop + pytest (Windows path: .venv/Scripts/...)
just jstest          # wasm-pack build both targets, then npm test both
```

### Python

```sh
cd tpt-stream-py
maturin develop --manifest-path Cargo.toml
python -m pytest python/tests -q
```

### WASM / JS

`pkg/` is **git-ignored**; rebuild before testing or publishing.

```sh
cd tpt-stream-wasm
wasm-pack build --target nodejs  --out-dir node/pkg
wasm-pack build --target bundler --out-dir browser/pkg
cd node        && npm test
cd browser     && npm ci && npm test
```

On **Windows**: `node --test tests/*.test.js` (bare directory fails).

### Browser playground

```sh
cd tpt-stream-wasm/browser
npm run build:playground   # webpack bundle into playground/dist
npm run serve:playground   # serve it locally
```

## Feature flags

- `tpt-stream-core` default features = `async` (tokio + rayon + async-trait).
- `tpt-stream-wasm` builds with `default-features = false` — sync-only, std-only.
- `tpt-stream-py` and `tpt-stream-cli` enable all optional features:
  `sqlite`, `postgres`, `s3`, `gcs`, `azure`, `gzip`, `http`, `zstd`.

## Tests that skip without env vars

- **Postgres**: `TPT_TEST_POSTGRES_URL` required, e.g. `postgres://postgres:postgres@localhost:5432/postgres`. In CI it uses the service container default.
- **Cloud**: `TPT_TEST_S3_ENDPOINT` and `TPT_TEST_AZURE_ENDPOINT` gate real-cloud runs. Offline mock server runs always.

## Architecture

```
tpt-stream-core/       Rust async engine (Pipeline + stages)
tpt-stream-columnar/   .tptcol format + zstd
tpt-stream-ffi/        C ABI (cdylib + staticlib), cbindgen header
tpt-stream-py/         PyO3 via maturin → pip install tpt-streamforge
tpt-stream-wasm/       wasm-bindgen (sync-only)
  node/                tpt-streamforge-node (CJS)
  browser/             tpt-streamforge-browser (ESM + webpack)
tpt-stream-cli/        tptforge binary (clap + indicatif progress)
  src/sql.rs           `tptforge sql` — sqlparser -> filter/aggregate/sort/limit stages
tpt-stream-wasm/browser/playground/  in-browser pipeline builder (webpack, wasm-only)
```

Default chunk size: **65,536 rows**. Spill-to-disk uses `.tptcol` under the system temp dir.

Each crate carries its own `README.md` and `CHANGELOG.md` (crates.io `readme`/`categories`/`keywords` metadata); update the relevant crate's `CHANGELOG.md` alongside the root one when a change is scoped to that crate.

## Constraints

- Dual-licensed MIT / Apache-2.0. `cargo deny` must stay green (enforced in release workflow).
- No cloud SDKs: S3/GCS/Azure use bare HTTP + signing.
- `cbindgen` (MPL-2.0) is a build-time tool only; its code never ships — covered by `deny.toml` exception.

## Release

1. Update `CHANGELOG.md`, bump versions, commit.
2. `git tag v0.1.0 && git push origin v0.1.0`
3. GitHub Actions release workflow builds CPython wheels (trusted publishing) + npm packages. Requires `NPM_TOKEN` secret for npm; PyPI uses OIDC.
