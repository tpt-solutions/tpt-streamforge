# tpt-stream-wasm examples

## Node.js (runnable now)

```sh
cd tpt-stream-wasm
wasm-pack build --target nodejs --out-dir node/pkg   # once: pkg/ is git-ignored
node examples/node-report.js
```

`node-report.js` builds a 2,000-row CSV in memory, filters/maps/aggregates/sorts
it, writes the report to the temp dir, reads it back, and prints the telemetry
`execute()` returns.

## Browser

The `bundler`-target wasm glue imports its `.wasm` as a module, so it has to go
through a bundler (webpack/Vite/Rollup) with async WebAssembly enabled —
`browser-report.mjs` is written for exactly that: import it from your app, or
bundle it yourself.

For a zero-setup demo, use the playground, which is the browser example wired
into a page:

```sh
cd tpt-stream-wasm/browser
npm ci
npm run build:playground
npm run serve:playground      # http://localhost:3000
```

Both examples use the same engine surface as the npm packages: `fromCsv` /
`fromArrayBuffer` / `fromFile`, `filter`, `map`, `sort`, `dedup`,
`aggregate`, `toCSV` / `toJSON` / `toArrayBuffer`, `numRows`, `columnNames`,
and the `onProgress` / `on_progress` telemetry hook.
