# tpt-stream-wasm

In-memory ETL for JavaScript, compiled to WebAssembly with
[wasm-bindgen](https://rustwasm.github.io/wasm-bindgen/). The `Engine` holds
a whole dataset as columnar batches and runs filter / map / sort / dedup /
aggregate synchronously — no filesystem, no async, no tokio (the crate is
built with `tpt-stream-core` `default-features = false`).

The compiled artifact is ~250 KB, far under the 10 MB budget.

Two npm packages wrap this crate:

- **Node.js**: [`node/`](node/README.md) — `tpt-streamforge-node` (CJS, file
  I/O via `fs`, plus JSON/JSONL file readers)
- **Browsers**: [`browser/`](browser/README.md) —
  `tpt-streamforge-browser` (ESM, `ArrayBuffer` / `File` inputs, webpack
  test harness). `browser/playground/` hosts an in-browser pipeline
  playground built on the same engine.

## Engine API (raw wasm-bindgen)

```js
import { Engine } from './pkg/tpt_stream_wasm.js';

const engine = Engine.from_csv('id,name\n1,alice\n2,bob\n', 0);
engine.filter('id > 1');
engine.sort(['name'], false);
engine.aggregate([], ['count_all'], ['']);
const csv = engine.to_csv();
```

Column types mirror the core engine (`int32` … `bool`, `date`, `timestamp`);
dates render and parse as ISO strings.

## Building from source

```sh
cd tpt-stream-wasm
wasm-pack build --target nodejs   --out-dir node/pkg
wasm-pack build --target bundler --out-dir browser/pkg
```

`pkg/` output is git-ignored; the release workflow rebuilds and publishes
both packages.

## Tests

```sh
cd node    && npm test   # node:test, includes telemetry + JSON reader tests
cd browser && npm ci && npm test   # webpack-bundled harness
```
