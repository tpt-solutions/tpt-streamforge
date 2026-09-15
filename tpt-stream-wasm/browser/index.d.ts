export interface ExecuteResult {
  rows: number;
  csv: string;
}

/**
 * Fluent, in-memory ETL pipeline over CSV text for the browser. Every
 * transform mutates the pipeline and returns `this` for chaining.
 */
export declare class Pipeline {
  constructor(csv: string, chunkRows?: number);
  static fromCsv(csv: string, chunkRows?: number): Pipeline;
  /** Decode UTF-8 bytes (e.g. from an ArrayBuffer) into a pipeline. */
  static fromArrayBuffer(buffer: ArrayBuffer, chunkRows?: number): Pipeline;
  /** Read a browser `File`/`Blob` and build a pipeline from its contents. */
  static fromFile(file: File | Blob, chunkRows?: number): Promise<Pipeline>;
  /** Keep only rows where the expression evaluates to true. */
  filter(expr: string): this;
  /** Replace the schema: `columns[i]` is filled with `exprs[i]` per row. */
  map(columns: string[], exprs: string[]): this;
  /** Sort by columns; `descending` applies to every column. */
  sort(columns: string[], descending?: boolean): this;
  /** Drop rows whose key columns repeat an earlier row (keeps first). */
  dedup(columns: string[]): this;
  /**
   * GROUP BY with fns in {sum, avg, count, count_all, min, max} per column.
   * `count_all` ignores its column. Output columns are named `{fn}_{col}`.
   */
  aggregate(groupBy: string[], fns: string[], columns: string[]): this;
  /**
   * Register a progress callback: `callback(event)` fires once per batch for
   * filter/map/dedup and once at completion of every operation. The event is
   * `{op, done, total, rowsIn, rowsOut, elapsedMs}`.
   */
  onProgress(callback: (event: TelemetryEvent) => void): this;
  /** Remove a previously registered progress callback. */
  clearProgress(): this;
  columnNames(): string[];
  numRows(): number;
  /** Serialize the current result to CSV text. */
  toCSV(): string;
  /** Serialize the current result to CSV bytes for download or upload. */
  toArrayBuffer(): ArrayBuffer;
  /** Serialize the current result to a JSON array of objects. */
  toJSON(): unknown[];
  execute(): ExecuteResult;
  readonly engine: Engine;
}

export declare function createPipeline(csv: string, chunkRows?: number): Pipeline;

export { Engine } from './pkg/tpt_stream_wasm.js';