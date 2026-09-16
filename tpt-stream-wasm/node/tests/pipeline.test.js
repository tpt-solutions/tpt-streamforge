'use strict';

const { test } = require('node:test');
const assert = require('node:assert');
const os = require('node:os');
const fs = require('node:fs');
const path = require('node:path');

const { Pipeline, createPipeline, readCSV, readJsonFile, readJsonLinesFile } = require('../index.js');

const CSV = [
  'id,name,amount,region',
  '1,alice,10,north',
  '2,bob,20,south',
  '3,carol,10,north',
  '4,dave,,south',
].join('\n') + '\n';

test('fromCsv + numRows + columnNames', () => {
  const p = createPipeline(CSV);
  assert.strictEqual(p.numRows(), 4);
  assert.deepStrictEqual(p.columnNames(), ['id', 'name', 'amount', 'region']);
});

test('filter keeps matching rows', () => {
  const p = new Pipeline(CSV).filter('amount > 10');
  assert.strictEqual(p.numRows(), 1);
  assert.strictEqual(p.toCSV(), 'id,name,amount,region\n2,bob,20,south\n');
});

test('filter with compound expression', () => {
  const p = new Pipeline(CSV).filter('region == "north" and amount >= 10');
  assert.strictEqual(p.numRows(), 2);
});

test('map replaces the row schema', () => {
  const p = new Pipeline(CSV).map(
    ['upper_name', 'amount', 'total'],
    ['upper(name)', 'amount', 'amount * 2'],
  );
  assert.deepStrictEqual(
    p.toJSON(),
    [
      { upper_name: 'ALICE', amount: 10, total: 20 },
      { upper_name: 'BOB', amount: 20, total: 40 },
      { upper_name: 'CAROL', amount: 10, total: 20 },
      { upper_name: 'DAVE', amount: null, total: null },
    ],
  );
});

test('sort ascending puts nulls first, descending last', () => {
  const asc = new Pipeline(CSV).sort(['amount']);
  assert.strictEqual(asc.numRows(), 4);
  const ascNames = asc.toJSON().map((r) => r.name);
  assert.deepStrictEqual(ascNames, ['dave', 'alice', 'carol', 'bob']);

  const desc = new Pipeline(CSV).sort(['amount'], true);
  const descNames = desc.toJSON().map((r) => r.name);
  assert.deepStrictEqual(descNames, ['bob', 'alice', 'carol', 'dave']);
});

test('dedup removes repeating keys', () => {
  const csv = 'k,v\n1,a\n2,b\n1,c\n3,d\n2,e\n';
  const p = new Pipeline(csv).dedup(['k']);
  assert.strictEqual(p.numRows(), 3);
  assert.deepStrictEqual(p.toJSON().map((r) => r.v), ['a', 'b', 'd']);
});

test('aggregate groups deterministically', () => {
  const p = new Pipeline(CSV).aggregate(
    ['region'],
    ['sum', 'count', 'avg', 'min', 'max', 'count_all'],
    ['amount', 'amount', 'amount', 'amount', 'amount', ''],
  );
  const rows = p.toJSON();
  assert.deepStrictEqual(rows, [
    {
      region: 'north',
      sum_amount: 20,
      count_amount: 2,
      avg_amount: 10,
      min_amount: 10,
      max_amount: 10,
      count_all: 2,
    },
    {
      region: 'south',
      sum_amount: 20,
      count_amount: 1,
      avg_amount: 20,
      min_amount: 20,
      max_amount: 20,
      count_all: 2,
    },
  ]);
});

test('full chain: filter -> map -> sort -> aggregate', () => {
  const p = new Pipeline(CSV)
    .filter('region == "south" or amount > 10')
    .map(['label', 'price'], ['name', 'amount'])
    .sort(['price'], true)
    .aggregate(['label'], ['count_all'], ['']);
  const rows = p.toJSON();
  // bob (amount 20) and dave (south) survive the filter.
  assert.strictEqual(rows.length, 2);
  assert.deepStrictEqual(rows, [
    { label: 'bob', count_all: 1 },
    { label: 'dave', count_all: 1 },
  ]);
});

test('toCSV roundtrip through readCSV file', () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'tpt-node-'));
  const file = path.join(dir, 'in.csv');
  fs.writeFileSync(file, CSV);
  try {
    const p = readCSV(file);
    assert.strictEqual(p.toCSV(), CSV);
    const out = path.join(dir, 'out.csv');
    p.filter('amount > 10').writeFile(out);
    assert.strictEqual(fs.readFileSync(out, 'utf8'), 'id,name,amount,region\n2,bob,20,south\n');
  } finally {
    fs.rmSync(dir, { recursive: true, force: true });
  }
});

test('expression errors throw from wasm', () => {
  assert.throws(() => new Pipeline(CSV).filter('amount +'), /parse|expression|unexpected|expected/i);
});

test('readJsonFile and readJsonLinesFile build pipelines from JSON', () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'tpt-json-'));
  try {
    const jsonPath = path.join(dir, 'in.json');
    fs.writeFileSync(jsonPath, JSON.stringify([{ id: 1, name: 'a' }, { id: 2, name: 'b' }]));
    const p = readJsonFile(jsonPath);
    assert.strictEqual(p.numRows(), 2);
    assert.strictEqual(p.toCSV(), 'id,name\n1,a\n2,b\n');

    const jsonlPath = path.join(dir, 'in.jsonl');
    fs.writeFileSync(jsonlPath, '{"id":1,"name":"a"}\n{"id":2,"name":"b"}\n');
    const p2 = readJsonLinesFile(jsonlPath);
    assert.strictEqual(p2.numRows(), 2);
    assert.strictEqual(p2.filter('id > 1').toCSV(), 'id,name\n2,b\n');
  } finally {
    fs.rmSync(dir, { recursive: true, force: true });
  }
});

test('onProgress fires per batch for filter and reports cumulative rows', () => {
  const events = [];
  new Pipeline(CSV, 2) // 4 rows / chunk 2 -> 2 batches
    .onProgress((e) => events.push(e))
    .filter('amount >= 10');
  const filterEvents = events.filter((e) => e.op === 'filter');
  assert.strictEqual(filterEvents.length, 2);
  assert.deepStrictEqual(filterEvents.map((e) => e.done), [1, 2]);
  assert.strictEqual(filterEvents[0].total, 2);
  assert.strictEqual(filterEvents[1].rowsIn, 4);
  assert.strictEqual(filterEvents[1].rowsOut, 3); // one null amount dropped
  assert.strictEqual(typeof filterEvents[1].elapsedMs, 'number');
});

test('onProgress reports aggregate completion with group count', () => {
  const events = [];
  new Pipeline(CSV)
    .onProgress((e) => events.push(e))
    .aggregate(['region'], ['count'], ['id']);
  const last = events[events.length - 1];
  assert.strictEqual(last.op, 'aggregate');
  assert.strictEqual(last.done, 1);
  assert.strictEqual(last.total, 1);
  assert.strictEqual(last.rowsIn, 4);
  assert.strictEqual(last.rowsOut, 2); // north + south
});

test('clearProgress stops telemetry events', () => {
  const events = [];
  const p = new Pipeline(CSV).onProgress((e) => events.push(e));
  p.clearProgress();
  p.filter('amount > 0');
  assert.strictEqual(events.length, 0);
});