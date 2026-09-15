import assert from 'node:assert/strict';
import { Pipeline, createPipeline } from '../index.js';

const CSV = [
  'id,name,amount,region',
  '1,alice,10,north',
  '2,bob,20,south',
  '3,carol,10,north',
  '4,dave,,south',
].join('\n') + '\n';

let passed = 0;
let failed = 0;

function ok(name, fn) {
  try {
    fn();
    passed += 1;
  } catch (err) {
    failed += 1;
    console.error(`FAIL ${name}: ${err.message}`);
  }
}

ok('fromCsv + numRows + columnNames', () => {
  const p = createPipeline(CSV);
  assert.equal(p.numRows(), 4);
  assert.deepEqual(p.columnNames(), ['id', 'name', 'amount', 'region']);
});

ok('filter keeps matching rows', () => {
  const p = new Pipeline(CSV).filter('amount > 10');
  assert.equal(p.numRows(), 1);
  assert.equal(p.toCSV(), 'id,name,amount,region\n2,bob,20,south\n');
});

ok('filter with three-valued or null', () => {
  const p = new Pipeline(CSV).filter('region == "south" or amount > 10');
  assert.equal(p.numRows(), 2);
});

ok('map replaces the row schema', () => {
  const p = new Pipeline(CSV).map(
    ['upper_name', 'total'],
    ['upper(name)', 'amount * 2'],
  );
  assert.deepEqual(p.toJSON(), [
    { upper_name: 'ALICE', total: 20 },
    { upper_name: 'BOB', total: 40 },
    { upper_name: 'CAROL', total: 20 },
    { upper_name: 'DAVE', total: null },
  ]);
});

ok('sort ascending puts nulls first', () => {
  const asc = new Pipeline(CSV).sort(['amount']);
  assert.deepEqual(asc.toJSON().map((r) => r.name), ['dave', 'alice', 'carol', 'bob']);
});

ok('dedup removes repeating keys', () => {
  const p = new Pipeline('k,v\n1,a\n2,b\n1,c\n').dedup(['k']);
  assert.deepEqual(p.toJSON().map((r) => r.v), ['a', 'b']);
});

ok('aggregate groups deterministically', () => {
  const p = new Pipeline(CSV).aggregate(
    ['region'],
    ['sum', 'count', 'count_all'],
    ['amount', 'amount', ''],
  );
  assert.deepEqual(p.toJSON(), [
    { region: 'north', sum_amount: 20, count_amount: 2, count_all: 2 },
    { region: 'south', sum_amount: 20, count_amount: 1, count_all: 2 },
  ]);
});

ok('toArrayBuffer round-trips through fromArrayBuffer', () => {
  const p = new Pipeline(CSV).filter('amount > 10');
  const restored = Pipeline.fromArrayBuffer(p.toArrayBuffer());
  assert.equal(restored.toCSV(), 'id,name,amount,region\n2,bob,20,south\n');
});

ok('execute returns rows + csv', () => {
  const result = new Pipeline(CSV).filter('amount > 10').execute();
  assert.equal(result.rows, 1);
  assert.ok(result.csv.includes('bob'));
});

ok('expression errors throw', () => {
  assert.throws(() => new Pipeline(CSV).filter('amount +'));
});

ok('onProgress fires per batch for filter and reports cumulative rows', () => {
  const events = [];
  new Pipeline(CSV, 2) // 4 rows / chunk 2 -> 2 batches
    .onProgress((e) => events.push(e))
    .filter('amount >= 10');
  const filterEvents = events.filter((e) => e.op === 'filter');
  assert.equal(filterEvents.length, 2);
  assert.deepEqual(filterEvents.map((e) => e.done), [1, 2]);
  assert.equal(filterEvents[1].rowsIn, 4);
  assert.equal(filterEvents[1].rowsOut, 3); // one null amount dropped
  assert.equal(typeof filterEvents[1].elapsedMs, 'number');
});

ok('onProgress reports aggregate completion with group count', () => {
  const events = [];
  new Pipeline(CSV)
    .onProgress((e) => events.push(e))
    .aggregate(['region'], ['count'], ['id']);
  const last = events[events.length - 1];
  assert.equal(last.op, 'aggregate');
  assert.equal(last.rowsIn, 4);
  assert.equal(last.rowsOut, 2); // north + south
});

ok('clearProgress stops telemetry events', () => {
  const events = [];
  const p = new Pipeline(CSV).onProgress((e) => events.push(e));
  p.clearProgress();
  p.filter('amount > 0');
  assert.equal(events.length, 0);
});

console.log(`browser-package tests: ${passed} passed, ${failed} failed`);
process.exitCode = failed === 0 ? 0 : 1;