export interface TelemetryEvent {
  op: string;
  done: number;
  total: number;
  rowsIn: number;
  rowsOut: number;
  elapsedMs: number;
}

declare class Engine {
  static from_csv(csv: string, chunk_rows: number): Engine;
  filter(expr_text: string): void;
  map(columns: string[], exprs: string[]): void;
  sort(columns: string[], descending: boolean): void;
  dedup(columns: string[]): void;
  aggregate(group_by: string[], fns: string[], columns: string[]): void;
  column_names(): string[];
  num_rows(): number;
  to_csv(): string;
  to_json_text(): string;
  on_progress(callback: (event: TelemetryEvent) => void): void;
  clear_progress(): void;
  free(): void;
}

export interface ExecuteResult {
  rows: number;
  csv: string;
}

/**
 * Fluent, in-memory ETL pipeline over CSV text. Every transform mutates the
 * pipeline and returns `this` for chaining.
 */
export class Pipeline {
  constructor(input: string | Engine, chunkRows?: number);
  static fromCsv(csv: string, chunkRows?: number): Pipeline;
  /** Read a CSV file from disk. */
  static readFile(path: string, chunkRows?: number): Pipeline;
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
  /** Serialize the current result to a JSON array of objects. */
  toJSON(): unknown[];
  /** Write CSV to disk. */
  writeFile(path: string): this;
  execute(): ExecuteResult;
  readonly engine: Engine;
}

export { Engine };
export declare function createPipeline(csv: string, chunkRows?: number): Pipeline;
export declare const readCSV: typeof Pipeline.readFile;