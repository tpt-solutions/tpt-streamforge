# tpt-streamforge-browser

In-memory streaming ETL for the browser, backed by the tpt-streamforge WASM engine. No
server round-trips: CSV text is parsed, filtered, mapped, sorted, deduplicated, and
aggregated entirely in WebAssembly.

```js
import { Pipeline } from 'tpt-streamforge-browser';

// From a <input type="file"> File/Blob:
const p = await Pipeline.fromFile(file)
  .filter('amount > 10')
  .sort(['region'])
  .aggregate(['region'], ['sum'], ['amount']);

console.log(p.toJSON());
```

## API

- `new Pipeline(csv, chunkRows?)` / `Pipeline.fromCsv(...)` — build from a string.
- `Pipeline.fromArrayBuffer(buf, chunkRows?)` — accept UTF-8 bytes.
- `Pipeline.fromFile(file, chunkRows?)` — from a `File` or `Blob` (async).
- Fluent transforms (each returns `this`): `filter(expr)`, `map(columns, exprs)`,
  `sort(columns, descending?)`, `dedup(columns)`,
  `aggregate(groupBy, fns, columns)`.
- Results: `toCSV()`, `toJSON()`, `toArrayBuffer()`, `numRows()`, `columnNames()`,
  `execute()` → `{ rows, csv }`.
- Telemetry: `onProgress(callback)` / `clearProgress()`. The callback fires
  once per batch for `filter`/`map`/`dedup` and once at completion of every
  operation with `{ op, done, total, rowsIn, rowsOut, elapsedMs }`; a raising
  callback is reported to `console.error` and never aborts the operation.

## Bundlers

This package ships ESM (`type: module`) and works with Vite, webpack, esbuild, and
Rollup. The WebAssembly is bundled in under `pkg/`.

## Building from source

```sh
cd tpt-stream-wasm
wasm-pack build --target bundler --out-dir browser/pkg
```