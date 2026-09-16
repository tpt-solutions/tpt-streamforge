'use strict';

const fs = require('node:fs');
const { Engine } = require('./pkg/tpt_stream_wasm.js');

function fresh(arr) {
  return Array.from(arr, (s) => String(s));
}

/** Serialize an array of flat objects to CSV text (first-seen key order). */
function jsonRowsToCsv(rows) {
  const keys = [];
  const seen = new Set();
  for (const row of rows) {
    for (const k of Object.keys(row)) {
      if (!seen.has(k)) {
        seen.add(k);
        keys.push(k);
      }
    }
  }
  const esc = (v) => {
    const s = v === null || v === undefined ? '' : String(v);
    return /[",\n\r]/.test(s) ? '"' + s.replace(/"/g, '""') + '"' : s;
  };
  const lines = [keys.map(esc).join(',')];
  for (const row of rows) {
    lines.push(keys.map((k) => esc(row[k])).join(','));
  }
  return lines.join('\n') + '\n';
}

class Pipeline {
  constructor(input, chunkRows = 0) {
    if (input instanceof Engine) {
      this._engine = input;
      return;
    }
    this._engine = Engine.from_csv(String(input), chunkRows);
  }

  static fromCsv(csv, chunkRows = 0) {
    return new Pipeline(csv, chunkRows);
  }

  static readFile(path, chunkRows = 0) {
    return new Pipeline(fs.readFileSync(path, 'utf8'), chunkRows);
  }

  /** Read a JSON file (array of objects or a single object). */
  static readJsonFile(path, chunkRows = 0) {
    const data = JSON.parse(fs.readFileSync(path, 'utf8'));
    const rows = Array.isArray(data) ? data : [data];
    return new Pipeline(jsonRowsToCsv(rows), chunkRows);
  }

  /** Read a newline-delimited JSON (JSONL) file. */
  static readJsonLinesFile(path, chunkRows = 0) {
    const rows = fs
      .readFileSync(path, 'utf8')
      .split('\n')
      .filter((line) => line.trim() !== '')
      .map((line) => JSON.parse(line));
    return new Pipeline(jsonRowsToCsv(rows), chunkRows);
  }

  filter(expr) {
    this._engine.filter(String(expr));
    return this;
  }

  map(columns, exprs) {
    this._engine.map(fresh(columns), fresh(exprs));
    return this;
  }

  sort(columns, descending = false) {
    this._engine.sort(fresh(columns), !!descending);
    return this;
  }

  dedup(columns) {
    this._engine.dedup(fresh(columns));
    return this;
  }

  aggregate(groupBy, fns, columns) {
    this._engine.aggregate(fresh(groupBy), fresh(fns), fresh(columns));
    return this;
  }

  /**
   * Register a progress callback: `callback(event)` fires once per batch for
   * filter/map/dedup and once at completion of every operation. The event is
   * `{op, done, total, rowsIn, rowsOut, elapsedMs}`.
   */
  onProgress(callback) {
    this._engine.on_progress(callback);
    return this;
  }

  /** Remove a previously registered progress callback. */
  clearProgress() {
    this._engine.clear_progress();
    return this;
  }

  columnNames() {
    return this._engine.column_names();
  }

  numRows() {
    return this._engine.num_rows();
  }

  toCSV() {
    return this._engine.to_csv();
  }

  toJSON() {
    return JSON.parse(this._engine.to_json_text());
  }

  writeFile(path) {
    fs.writeFileSync(path, this.toCSV());
    return this;
  }

  execute() {
    return { rows: this.numRows(), csv: this.toCSV() };
  }

  get engine() {
    return this._engine;
  }
}

const createPipeline = (csv, chunkRows) => new Pipeline(csv, chunkRows);

module.exports = {
  Pipeline,
  Engine,
  createPipeline,
  readCSV: Pipeline.readFile,
  readJsonFile: Pipeline.readJsonFile,
  readJsonLinesFile: Pipeline.readJsonLinesFile,
};