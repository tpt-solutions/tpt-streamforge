// Browser example (ESM): the same engine as `node-report.js`, but fed from an
// `ArrayBuffer` / `File` and returning bytes for a download.
//
// This file is meant to be bundled by your app's bundler (webpack, Vite,
// Rollup, ...) with async WebAssembly enabled — the `bundler`-target wasm glue
// imports the `.wasm` as a module, so it cannot be loaded from plain HTML.
//
// Zero-setup browser demo: use the playground instead.
//   cd tpt-stream-wasm/browser && npm ci && npm run build:playground && npm run serve:playground

import { Pipeline, Engine } from '../browser/index.js';

/** Build a per-region report from CSV text and return it as CSV text. */
export function regionReport(csvText) {
  return Pipeline.fromCsv(csvText)
    .filter('amount > 0')
    .map(['region', 'gross'], ['region', 'amount * 1.2'])
    .aggregate(['region'], ['sum', 'count_all'], ['gross', ''])
    .sort(['sum_gross'], true)
    .toCSV();
}

/** Feed a `File`/`Blob` picked in the browser straight into the engine. */
export async function reportFromFile(file) {
  const pipeline = await Pipeline.fromFile(file);
  return {
    rows: pipeline.numRows(),
    columns: pipeline.columnNames(),
    csv: pipeline.sort(['amount'], true).toCSV(),
  };
}

/**
 * Telegram-style progress: one event per batch while filtering.
 * Event shape: `{ op, done, total, rowsIn, rowsOut, elapsedMs }`.
 */
export function reportWithProgress(csvText, onEvent) {
  const engine = Engine.from_csv(csvText, 0);
  engine.on_progress(onEvent);
  engine.filter('amount > 0');
  engine.clear_progress();
  return engine.to_csv();
}

/** Bytes ready for a `Blob` download or a `.write()` to an OPFS handle. */
export function reportBytes(csvText) {
  return new TextEncoder().encode(regionReport(csvText));
}
