'use strict';

const fs = require('node:fs');
const { Engine } = require('./pkg/tpt_stream_wasm.js');

function fresh(arr) {
  return Array.from(arr, (s) => String(s));
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

module.exports = { Pipeline, Engine, createPipeline, readCSV: Pipeline.readFile };