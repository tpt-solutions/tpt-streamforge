# Contributing

## Prerequisites

- Rust **stable** (1.85+)
- `wasm32-unknown-unknown` target: `rustup target add wasm32-unknown-unknown`
- [wasm-pack](https://rustwasm.github.io/wasm-pack/) 0.13+ (0.15 verified)
- Node.js 22+ / npm 11+
- Python 3.13+ with [maturin](https://www.maturin.rs/) 1.15+ and `pytest`

## Workspace layout

```
tpt-csv/               dependency-free streaming CSV reader/writer
tpt-stream-core/       Rust engine (default-features build is async)
tpt-stream-columnar/   native .tptcol columnar format, optional zstd
tpt-stream-ffi/        C ABI over the core
tpt-stream-py/         PyO3 Python package  (maturin)
tpt-stream-wasm/       wasm-bindgen crate
  node/                tpt-streamforge-node (CJS wrapper + node:test suite)
  browser/             tpt-streamforge-browser (ESM wrapper + webpack harness)
tpt-stream-cli/        tptforge CLI (TOML pipelines + SQL frontend)
templates/             copy-paste pipeline starters
```

## Rust checks

```sh
cargo fmt --all --check
cargo clippy --workspace --all-features -- -D warnings
cargo test --workspace --all-features
cargo check -p tpt-stream-wasm --target wasm32-unknown-unknown
# deny/licenses must stay green (used by the release workflow):
cargo install cargo-deny && cargo deny check
```

## Python

```sh
cd tpt-stream-py
maturin develop        # or: maturin build --release --out dist
python -m pytest python/tests -q
```

## WASM / JS

```sh
cd tpt-stream-wasm
wasm-pack build --target nodejs   --out-dir node/pkg    # then: cd node && npm test
wasm-pack build --target bundler  --out-dir browser/pkg # then: cd browser && npm ci && npm test
```

The `pkg/` folders are git-ignored; fresh checkouts must rebuild before running
the JS tests. Note on Windows: run `node --test tests/pipeline.test.js` or
`npm test` — passing a bare directory to `node --test` fails there.

## CI

`.github/workflows/ci.yml` runs the Rust checks plus the JS job (builds both
wasm targets and runs both test suites) on every push/PR.

## Release process

1. Update `CHANGELOG.md`, bump crate/package versions, and commit.
2. Tag the release:

   ```sh
   git tag v0.1.0
   git push origin v0.1.0
   ```

3. `.github/workflows/release.yml` then:
   - builds CPython wheels on ubuntu/macos/windows and publishes to PyPI
     (trusted publishing),
   - runs `cargo deny check` (quality gate),
   - builds + tests + publishes both npm packages (`tpt-streamforge-node`,
     `tpt-streamforge-browser`).

The npm job authenticates with an `NPM_TOKEN` secret (an npm access token with
publish scope for the org). PyPI uses OIDC trusted publishing.