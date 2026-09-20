'use strict';

// Runnable Node example: build a per-region report from an in-memory CSV and
// read one back from disk.
//
//   cd tpt-stream-wasm
//   wasm-pack build --target nodejs --out-dir node/pkg     # once
//   node examples/node-report.js
//
// The engine is in-memory and synchronous: no tokio, no filesystem inside wasm.
// `readFile`/`writeFile` here are thin `fs` shims in the Node wrapper.

const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');

const { Pipeline, readCSV } = require('../node/index.js');

function sampleCsv(rows) {
  const regions = ['emea', 'amer', 'apac', 'latam'];
  const lines = ['order_id,region,amount'];
  for (let i = 1; i <= rows; i += 1) {
    const region = regions[i % regions.length];
    const amount = i % 50 === 0 ? 0 : ((i * 7) % 400) / 4;
    lines.push(`${i},${region},${amount}`);
  }
  return lines.join('\n') + '\n';
}

const out = path.join(os.tmpdir(), 'tpt-streamforge-node-report.csv');

const pipeline = Pipeline.fromCsv(sampleCsv(2000))
  .filter('amount > 0')
  .map(['region', 'gross'], ['region', 'amount * 1.2'])
  .aggregate(['region'], ['sum', 'count_all'], ['gross', ''])
  .sort(['sum_gross'], true)
  .writeFile(out);

const { rows, csv } = pipeline.execute();
console.log(`streamed ${rows} rows, wrote ${out}`);
console.log(csv);

// Reading it back is just as cheap: `readCSV` is `Pipeline.readFile`.
const reread = readCSV(out).filter('count_all > 100').toJSON();
console.log('regions with more than 100 orders:', reread);

// Column names and row counts are available without materializing rows.
console.log('columns:', pipeline.columnNames(), 'rows:', pipeline.numRows());
console.log('bytes on disk:', fs.statSync(out).size);
