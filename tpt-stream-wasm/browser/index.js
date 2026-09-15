import { Engine } from './pkg/tpt_stream_wasm.js';

const utf8Decoder = new TextDecoder('utf-8');
const utf8Encoder = new TextEncoder();

function fresh(arr) {
  return Array.from(arr, (s) => String(s));
}

export class Pipeline {
  constructor(csv, chunkRows = 0) {
    this._engine = Engine.from_csv(String(csv), chunkRows);
  }

  static fromCsv(csv, chunkRows = 0) {
    return new Pipeline(csv, chunkRows);
  }

  static fromArrayBuffer(buffer, chunkRows = 0) {
    return new Pipeline(utf8Decoder.decode(new Uint8Array(buffer)), chunkRows);
  }

  static async fromFile(file, chunkRows = 0) {
    const buffer = await file.arrayBuffer();
    return Pipeline.fromArrayBuffer(buffer, chunkRows);
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

  toArrayBuffer() {
    return utf8Encoder.encode(this.toCSV()).buffer;
  }

  toJSON() {
    return JSON.parse(this._engine.to_json_text());
  }

  execute() {
    return { rows: this.numRows(), csv: this.toCSV() };
  }

  get engine() {
    return this._engine;
  }
}

export function createPipeline(csv, chunkRows) {
  return new Pipeline(csv, chunkRows);
}

export { Engine };