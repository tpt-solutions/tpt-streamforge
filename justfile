# tpt-streamforge development recipes. Run with `just <recipe>` (https://github.com/casey/just)

default:
    @just --list

# Format all Rust code
fmt:
    cargo fmt --all

# Run every Rust test with all features (tpt-stream-py is excluded: its
# pyo3 "extension-module" feature only links correctly via maturin)
test:
    cargo test --workspace --all-features --exclude tpt-streamforge

# Lint with clippy (CI gate)
clippy:
    cargo clippy --workspace --all-features --all-targets -- -D warnings

# License/advisory checks (CI gate)
deny:
    cargo deny check licenses bans sources

# Run the per-stage criterion benchmarks
bench:
    cargo bench -p tpt-stream-core --bench stages --features zstd

# Build the Python wheel into the local virtualenv
py:
    .venv/Scripts/maturin.exe develop -m tpt-stream-py/Cargo.toml

# Run the Python test suite
pytest: py
    .venv/Scripts/python.exe -m pytest tpt-stream-py/python/tests -q

# Build the Node and browser wasm packages
wasm:
    cd tpt-stream-wasm && wasm-pack build --target nodejs --out-dir node/pkg --release
    cd tpt-stream-wasm && wasm-pack build --target bundler --out-dir browser/pkg --release

# Run both JS test suites
jstest: wasm
    cd tpt-stream-wasm/node && npm test
    cd tpt-stream-wasm/browser && npm ci && npm test

# Everything CI checks, in one command
ci: fmt clippy deny test
    @echo "CI gates passed"
