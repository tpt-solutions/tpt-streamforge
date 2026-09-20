//! tpt-stream-wasm: in-memory ETL exposed to JavaScript/WASM (browser and
//! Node.js). Reuses the engine's expression parser and columnar types; all
//! data lives in memory, so there is no filesystem involvement.
//!
//! The JavaScript wrappers (`node/`, `browser/`) wrap the generated
//! wasm-bindgen glue with file- and ArrayBuffer-based I/O adapters.

use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};

use serde_json::json;
use tpt_stream_core::expr;
use tpt_stream_core::row::Row;
use tpt_stream_core::source::{batches_to_csv, csv_to_batches};
use tpt_stream_core::table::RecordBatch;
use tpt_stream_core::{Column, DataType, Value};
use wasm_bindgen::prelude::*;

const DEFAULT_CHUNK_ROWS: usize = 65_536;

fn js_err(msg: impl std::fmt::Display) -> JsValue {
    JsValue::from_str(&msg.to_string())
}

fn core_err(err: tpt_stream_core::Error) -> JsValue {
    js_err(err.to_string())
}

/// Value at `(row, col_index)`, clamping out-of-range to Null.
fn batch_cell(batch: &RecordBatch, row: usize, col: usize) -> Value {
    batch
        .column_by_index(col)
        .and_then(|c| c.get(row))
        .unwrap_or(Value::Null)
}

/// A whole-dataset in-memory transform engine.
#[wasm_bindgen]
pub struct Engine {
    batches: Vec<RecordBatch>,
    progress: Option<js_sys::Function>,
}

/// Monotonic wall-clock milliseconds for telemetry timings.
fn now_ms() -> f64 {
    js_sys::Date::now()
}

/// Invoke the progress callback with
/// `{op, done, total, rowsIn, rowsOut, elapsedMs}`. Callback errors are
/// reported to the console and never abort the operation.
fn emit_progress(
    progress: Option<&js_sys::Function>,
    op: &str,
    done: usize,
    total: usize,
    rows_in: usize,
    rows_out: usize,
    started_ms: f64,
) {
    let Some(callback) = progress else {
        return;
    };
    let event = js_sys::Object::new();
    let set = |key: &str, value: JsValue| {
        let _ = js_sys::Reflect::set(&event, &JsValue::from_str(key), &value);
    };
    set("op", JsValue::from_str(op));
    set("done", JsValue::from_f64(done as f64));
    set("total", JsValue::from_f64(total as f64));
    set("rowsIn", JsValue::from_f64(rows_in as f64));
    set("rowsOut", JsValue::from_f64(rows_out as f64));
    set("elapsedMs", JsValue::from_f64(now_ms() - started_ms));
    if let Err(err) = callback.call1(&JsValue::NULL, &event.into()) {
        web_sys_console_error(&err);
    }
}

fn web_sys_console_error(err: &JsValue) {
    // Reach the global `console.error` through Reflect: avoids a web-sys
    // dependency (needed for the console bindings) in this crate.
    let global = js_sys::global();
    let Ok(console) = js_sys::Reflect::get(&global, &JsValue::from_str("console")) else {
        return;
    };
    let Ok(error) = js_sys::Reflect::get(&console, &JsValue::from_str("error")) else {
        return;
    };
    let error = js_sys::Function::from(error);
    let _ = error.call2(
        &console,
        &JsValue::from_str("tpt-streamforge: progress callback failed:"),
        err,
    );
}

#[wasm_bindgen]
impl Engine {
    /// Parse a CSV string into an in-memory engine. `chunk_rows` controls the
    /// internal batch size (0 picks the engine default).
    ///
    /// Throws on malformed CSV or a missing header row.
    #[wasm_bindgen(js_name = from_csv)]
    pub fn from_csv(csv: &str, chunk_rows: usize) -> Result<Engine, JsValue> {
        let started = now_ms();
        let rows = if chunk_rows == 0 {
            DEFAULT_CHUNK_ROWS
        } else {
            chunk_rows
        };
        let batches = csv_to_batches(csv, rows).map_err(core_err)?;
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        let engine = Engine {
            batches,
            progress: None,
        };
        emit_progress(
            engine.progress.as_ref(),
            "from_csv",
            1,
            1,
            total_rows,
            total_rows,
            started,
        );
        Ok(engine)
    }

    /// Register a progress callback invoked as `callback(event)` where event
    /// is `{op, done, total, rowsIn, rowsOut, elapsedMs}`. Emitted once per
    /// batch by `filter`/`map`/`dedup` and once at completion by every
    /// operation (`from_csv`, `sort`, `aggregate`, `toCSV`, `toJSON`).
    #[wasm_bindgen(js_name = on_progress)]
    pub fn on_progress(&mut self, callback: js_sys::Function) {
        self.progress = Some(callback);
    }

    /// Remove a previously registered progress callback.
    #[wasm_bindgen(js_name = clear_progress)]
    pub fn clear_progress(&mut self) {
        self.progress = None;
    }

    /// Number of data rows across all batches.
    pub fn num_rows(&self) -> usize {
        self.batches.iter().map(|b| b.num_rows()).sum()
    }

    /// Column names of the (first) batch; empty for an empty engine.
    pub fn column_names(&self) -> Vec<String> {
        self.batches
            .first()
            .map(|b| {
                b.column_names()
                    .into_iter()
                    .map(|s| s.to_string())
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Keep only rows where `expr` evaluates to boolean true. Mutates this
    /// engine (fluent usage).
    pub fn filter(&mut self, expr_text: &str) -> Result<(), JsValue> {
        let started = now_ms();
        let parsed =
            expr::parse(expr_text).map_err(|e| js_err(format!("filter expression: {e}")))?;
        let total = self.batches.len();
        let mut rows_in = 0usize;
        let mut rows_out = 0usize;
        for (done, batch) in self.batches.iter_mut().enumerate() {
            let num_rows = batch.num_rows();
            rows_in += num_rows;
            let mut keep = Vec::with_capacity(num_rows);
            let mut any_dropped = false;
            for i in 0..num_rows {
                let row = Row::new(batch, i);
                let keep_row = parsed.matches(&row);
                keep.push(keep_row);
                if !keep_row {
                    any_dropped = true;
                }
            }
            let kept = if !any_dropped {
                num_rows
            } else {
                keep.iter().filter(|k| **k).count()
            };
            rows_out += kept;
            emit_progress(
                self.progress.as_ref(),
                "filter",
                done + 1,
                total,
                rows_in,
                rows_out,
                started,
            );
            if num_rows > 0 && !any_dropped {
                continue;
            }
            for column in batch.columns_mut() {
                column.retain_rows(&keep);
            }
            batch.recompute_row_count();
        }
        Ok(())
    }

    /// Replace the schema with columns `columns[i] = exprs[i]` evaluated per
    /// row (expression engine grammar). Array lengths must match.
    pub fn map(&mut self, columns: Vec<String>, exprs: Vec<String>) -> Result<(), JsValue> {
        if columns.len() != exprs.len() {
            return Err(js_err("map: columns and exprs must have the same length"));
        }
        let mut parsed: Vec<(String, expr::Expr)> = Vec::with_capacity(columns.len());
        for (name, text) in columns.iter().zip(exprs.iter()) {
            let e =
                expr::parse(text).map_err(|e| js_err(format!("map expression {name:?}: {e}")))?;
            parsed.push((name.clone(), e));
        }

        let started = now_ms();
        let batches = std::mem::take(&mut self.batches);
        let total = batches.len();
        let mut out = Vec::with_capacity(batches.len());
        let mut rows_in = 0usize;
        let mut rows_out = 0usize;
        for (done, batch) in batches.into_iter().enumerate() {
            let mut rows: Vec<Vec<Value>> = Vec::with_capacity(batch.num_rows());
            for i in 0..batch.num_rows() {
                let row = Row::new(&batch, i);
                rows.push(parsed.iter().map(|(_, e)| e.eval(&row)).collect());
            }
            rows_in += batch.num_rows();
            rows_out += rows.len();
            out.push(build_batch_from_rows(&parsed, &rows));
            emit_progress(
                self.progress.as_ref(),
                "map",
                done + 1,
                total,
                rows_in,
                rows_out,
                started,
            );
        }
        self.batches = out;
        Ok(())
    }

    /// Sort all rows by `columns` (ascending unless `descending`). Stable for
    /// equal keys.
    pub fn sort(&mut self, columns: Vec<String>, descending: bool) -> Result<(), JsValue> {
        let started = now_ms();
        let merged = merge_batches(&self.batches);
        if merged.num_rows() == 0 {
            self.batches = vec![merged];
            return Ok(());
        }
        let col_index: Vec<usize> = columns
            .iter()
            .map(|c| {
                merged
                    .column_names()
                    .iter()
                    .position(|n| n == c)
                    .ok_or_else(|| {
                        js_err(format!(
                            "sort: unknown column {c:?} (have {:?})",
                            merged.column_names()
                        ))
                    })
            })
            .collect::<Result<_, JsValue>>()?;

        let mut order: Vec<usize> = (0..merged.num_rows()).collect();
        order.sort_by(|&a, &b| {
            for &ci in &col_index {
                let va = batch_cell(&merged, a, ci);
                let vb = batch_cell(&merged, b, ci);
                let ord = compare_values(&va, &vb);
                if ord != Ordering::Equal {
                    return if descending { ord.reverse() } else { ord };
                }
            }
            Ordering::Equal
        });

        let names: Vec<String> = merged
            .column_names()
            .into_iter()
            .map(|s| s.to_string())
            .collect();
        let mut new_cols: Vec<Column> = Vec::with_capacity(names.len());
        for (ci, name) in names.iter().enumerate() {
            let data_type = merged
                .column_by_index(ci)
                .map(|c| c.data_type())
                .unwrap_or(DataType::Utf8);
            let mut col = Column::new(name.clone(), data_type, order.len());
            for &row in &order {
                col.push(batch_cell(&merged, row, ci));
            }
            new_cols.push(col);
        }
        self.batches = vec![RecordBatch::new(new_cols)];
        emit_progress(
            self.progress.as_ref(),
            "sort",
            1,
            1,
            order.len(),
            order.len(),
            started,
        );
        Ok(())
    }

    /// Drop rows whose key columns repeat an earlier row.
    pub fn dedup(&mut self, columns: Vec<String>) -> Result<(), JsValue> {
        let started = now_ms();
        let col_index: Vec<usize> = columns
            .iter()
            .map(|c| {
                self.column_names()
                    .iter()
                    .position(|n| n == c)
                    .ok_or_else(|| js_err(format!("dedup: unknown column {c:?}")))
            })
            .collect::<Result<_, JsValue>>()?;

        let total = self.batches.len();
        let mut rows_in = 0usize;
        let mut rows_out = 0usize;
        let mut seen: HashSet<String> = HashSet::new();
        for (done, batch) in self.batches.iter_mut().enumerate() {
            let num_rows = batch.num_rows();
            rows_in += num_rows;
            let mut keep = vec![false; num_rows];
            let mut any_dropped = false;
            for (i, keep_flag) in keep.iter_mut().enumerate() {
                let key = col_index
                    .iter()
                    .map(|&ci| key_component(&batch_cell(batch, i, ci)))
                    .collect::<Vec<_>>()
                    .join("\u{1f}");
                if seen.insert(key) {
                    *keep_flag = true;
                } else {
                    any_dropped = true;
                }
            }
            let kept = if !any_dropped {
                num_rows
            } else {
                keep.iter().filter(|k| **k).count()
            };
            rows_out += kept;
            emit_progress(
                self.progress.as_ref(),
                "dedup",
                done + 1,
                total,
                rows_in,
                rows_out,
                started,
            );
            if num_rows > 0 && !any_dropped {
                continue;
            }
            for column in batch.columns_mut() {
                column.retain_rows(&keep);
            }
            batch.recompute_row_count();
        }
        Ok(())
    }

    /// GROUP BY `group_by` with aggregate specs. `fns[i]` applies to
    /// `columns[i]`; the set of `fns` is: sum, avg, count, count_all (column
    /// may be empty), min, max. Output columns are named `{fn}_{column}`.
    pub fn aggregate(
        &mut self,
        group_by: Vec<String>,
        fns: Vec<String>,
        columns: Vec<String>,
    ) -> Result<(), JsValue> {
        if fns.len() != columns.len() {
            return Err(js_err(
                "aggregate: fns and columns must have the same length",
            ));
        }
        let g_index: Vec<usize> = group_by
            .iter()
            .map(|c| {
                self.column_names()
                    .iter()
                    .position(|n| n == c)
                    .ok_or_else(|| js_err(format!("aggregate: unknown group column {c:?}")))
            })
            .collect::<Result<_, JsValue>>()?;
        let mut spec_index: Vec<Option<usize>> = Vec::with_capacity(fns.len());
        for (i, col) in columns.iter().enumerate() {
            spec_index.push(if fns[i] == "count_all" {
                None
            } else {
                Some(match self.column_names().iter().position(|n| n == col) {
                    Some(ix) => ix,
                    None => return Err(js_err(format!("aggregate: unknown column {col:?}"))),
                })
            });
        }

        let started = now_ms();
        let merged = merge_batches(&self.batches);
        let mut groups: HashMap<String, (Vec<Value>, Vec<Acc>)> = HashMap::new();
        for i in 0..merged.num_rows() {
            let key = g_index
                .iter()
                .map(|&ci| key_component(&batch_cell(&merged, i, ci)))
                .collect::<Vec<_>>()
                .join("\u{1f}");
            let spec_values: Vec<Value> = spec_index
                .iter()
                .map(|ci| match ci {
                    Some(idx) => batch_cell(&merged, i, *idx),
                    None => Value::Null,
                })
                .collect();
            match groups.get_mut(&key) {
                Some((_, accs)) => {
                    for (k, f) in fns.iter().enumerate() {
                        accs[k].add(f, &spec_values[k]);
                    }
                }
                None => {
                    let keys: Vec<Value> = g_index
                        .iter()
                        .map(|&ci| batch_cell(&merged, i, ci))
                        .collect();
                    let accs: Vec<Acc> = fns
                        .iter()
                        .enumerate()
                        .map(|(k, f)| Acc::new(f, &spec_values[k]))
                        .collect();
                    groups.insert(key, (keys, accs));
                }
            }
        }

        // Deterministic output: sort groups by key values.
        let mut entries: Vec<GroupEntry> = groups.iter().collect();
        entries.sort_by(|a, b| {
            for (va, vb) in a.1 .0.iter().zip(b.1 .0.iter()) {
                let ord = compare_values(va, vb);
                if ord != Ordering::Equal {
                    return ord;
                }
            }
            Ordering::Equal
        });

        let mut cols: Vec<Column> = Vec::new();
        for (ci, name) in group_by.iter().enumerate() {
            let vals: Vec<Value> = entries.iter().map(|(_, g)| g.0[ci].clone()).collect();
            cols.push(build_column(name.clone(), &vals));
        }
        for (k, f) in fns.iter().enumerate() {
            let name = aggregate_output_name(f, &columns[k]);
            let vals: Vec<Value> = entries.iter().map(|(_, g)| g.1[k].finish()).collect();
            cols.push(build_column(name, &vals));
        }
        self.batches = vec![RecordBatch::new(cols)];
        emit_progress(
            self.progress.as_ref(),
            "aggregate",
            1,
            1,
            merged.num_rows(),
            entries.len(),
            started,
        );
        Ok(())
    }

    /// Serialize all rows back to CSV text (header + data, posix newlines).
    pub fn to_csv(&self) -> String {
        let started = now_ms();
        let text = batches_to_csv(&self.batches);
        let rows = self.num_rows();
        emit_progress(self.progress.as_ref(), "to_csv", 1, 1, rows, rows, started);
        text
    }

    /// Serialize all rows to a JSON array of objects.
    pub fn to_json_text(&self) -> Result<String, JsValue> {
        let started = now_ms();
        let mut rows: Vec<serde_json::Value> = Vec::with_capacity(self.num_rows());
        for batch in &self.batches {
            let names: Vec<String> = batch
                .column_names()
                .into_iter()
                .map(|s| s.to_string())
                .collect();
            for i in 0..batch.num_rows() {
                let mut obj = serde_json::Map::new();
                for (ci, name) in names.iter().enumerate() {
                    obj.insert(name.clone(), value_to_json(&batch_cell(batch, i, ci)));
                }
                rows.push(serde_json::Value::Object(obj));
            }
        }
        let out =
            serde_json::to_string(&json!(rows)).map_err(|e| js_err(format!("json encode: {e}")))?;
        emit_progress(
            self.progress.as_ref(),
            "to_json",
            1,
            1,
            rows.len(),
            rows.len(),
            started,
        );
        Ok(out)
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn aggregate_output_name(f: &str, col: &str) -> String {
    if f == "count_all" {
        "count_all".to_string()
    } else {
        format!("{f}_{col}")
    }
}

fn key_component(v: &Value) -> String {
    match v {
        Value::Null => "N".to_string(),
        Value::Int32(x) => format!("i{x}"),
        Value::Int64(x) => format!("i{x}"),
        Value::Float32(x) => format!("f{x}"),
        Value::Float64(x) => format!("f{x}"),
        Value::Bool(b) => format!("b{b}"),
        Value::Date(d) => format!("D{d}"),
        Value::Timestamp(t) => format!("T{t}"),
        Value::Utf8(s) => format!("s{s}"),
    }
}

fn value_to_json(v: &Value) -> serde_json::Value {
    match v {
        Value::Int32(x) => json!(x),
        Value::Int64(x) => json!(x),
        Value::Float32(x) => serde_json::Number::from_f64(*x as f64)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        Value::Float64(x) => serde_json::Number::from_f64(*x)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        Value::Utf8(s) => json!(s),
        Value::Bool(b) => json!(b),
        // Dates/timestamps serialize as ISO strings.
        Value::Date(_) | Value::Timestamp(_) => json!(v.to_string()),
        Value::Null => serde_json::Value::Null,
    }
}

/// Concatenate all batches into one, preserving the (shared) schema. Batches
/// produced by this engine always share a schema.
fn merge_batches(batches: &[RecordBatch]) -> RecordBatch {
    let Some(first) = batches.first() else {
        return RecordBatch::new(Vec::new());
    };
    let names: Vec<String> = first
        .column_names()
        .into_iter()
        .map(|s| s.to_string())
        .collect();
    let mut cols: Vec<Column> = Vec::with_capacity(names.len());
    for name in names {
        let data_type = first.column(&name).unwrap().data_type();
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        let mut col = Column::new(name.clone(), data_type, total);
        for batch in batches {
            let src = batch.column(&name).expect("merged schema consistent");
            for i in 0..src.len() {
                col.push(src.get(i).unwrap_or(Value::Null));
            }
        }
        cols.push(col);
    }
    RecordBatch::new(cols)
}

/// Coerce `v` into `target`, widening numbers and stringifying as a last
/// resort so `Column::push` never sees a type mismatch. Null propagates.
fn coerce(v: &Value, target: DataType) -> Value {
    use DataType::{Bool, Float64, Int32, Int64, Utf8};
    match v {
        Value::Null => Value::Null,
        Value::Bool(b) if target == Bool => Value::Bool(*b),
        Value::Int32(x) => match target {
            Int32 => Value::Int32(*x),
            Int64 => Value::Int64(*x as i64),
            Float64 => Value::Float64(*x as f64),
            _ => Value::Utf8(x.to_string()),
        },
        Value::Int64(x) => match target {
            Int64 | Int32 => Value::Int64(*x),
            Float64 => Value::Float64(*x as f64),
            _ => Value::Utf8(x.to_string()),
        },
        Value::Float32(x) => match target {
            Float64 => Value::Float64(*x as f64),
            _ => Value::Utf8(x.to_string()),
        },
        Value::Float64(x) => match target {
            Float64 => Value::Float64(*x),
            _ => Value::Utf8(x.to_string()),
        },
        Value::Utf8(s) if target == Utf8 => Value::Utf8(s.clone()),
        Value::Date(d) if target == DataType::Date => Value::Date(*d),
        Value::Timestamp(t) if target == DataType::Timestamp => Value::Timestamp(*t),
        value => Value::Utf8(value.to_string()),
    }
}

/// Rank a value's type for the widening ladder:
/// Null/Bool < Int32 < Int64 < Float64 < Utf8.
fn rank_value(v: &Value) -> u8 {
    match v {
        Value::Null => 0,
        Value::Bool(_) => 1,
        Value::Date(_) => 2,
        Value::Timestamp(_) => 3,
        Value::Int32(_) => 4,
        Value::Int64(_) => 5,
        Value::Float32(_) | Value::Float64(_) => 6,
        Value::Utf8(_) => 7,
    }
}

/// Widest common type of `values` (nulls are skipped). An all-null or all-bool
/// set yields Bool; numeric columns widen Int32 -> Int64 -> Float64.
fn unify_type(values: &[Value]) -> DataType {
    let mut rank = 0u8;
    for v in values {
        rank = rank.max(rank_value(v));
    }
    match rank {
        0 | 1 => DataType::Bool,
        2 => DataType::Date,
        3 => DataType::Timestamp,
        4 => DataType::Int32,
        5 => DataType::Int64,
        6 => DataType::Float64,
        _ => DataType::Utf8,
    }
}

/// Ordering across the Value model: nulls first, booleans, numerics compared
/// as f64, then strings (falling back to display order).
fn compare_values(a: &Value, b: &Value) -> Ordering {
    let (ra, rb) = (rank_value(a), rank_value(b));
    if ra != rb {
        return ra.cmp(&rb);
    }
    match (a, b) {
        (Value::Null, Value::Null) => Ordering::Equal,
        (Value::Bool(x), Value::Bool(y)) => x.cmp(y),
        (Value::Date(x), Value::Date(y)) => x.cmp(y),
        (Value::Timestamp(x), Value::Timestamp(y)) => x.cmp(y),
        // Lenient ISO-literal comparisons for date columns.
        (Value::Date(x), Value::Utf8(y)) => {
            tpt_stream_core::value::parse_date(y).map_or(Ordering::Equal, |o| x.cmp(&o))
        }
        (Value::Utf8(x), Value::Date(y)) => {
            tpt_stream_core::value::parse_date(x).map_or(Ordering::Equal, |o| o.cmp(y))
        }
        (Value::Timestamp(x), Value::Utf8(y)) => {
            tpt_stream_core::value::parse_timestamp(y).map_or(Ordering::Equal, |o| x.cmp(&o))
        }
        (Value::Utf8(x), Value::Timestamp(y)) => {
            tpt_stream_core::value::parse_timestamp(x).map_or(Ordering::Equal, |o| o.cmp(y))
        }
        (Value::Utf8(x), Value::Utf8(y)) => x.cmp(y),
        (x, y) if is_numeric(x) && is_numeric(y) => {
            to_f64(x).partial_cmp(&to_f64(y)).unwrap_or(Ordering::Equal)
        }
        _ => a.to_string().cmp(&b.to_string()),
    }
}

fn is_numeric(v: &Value) -> bool {
    matches!(
        v,
        Value::Int32(_) | Value::Int64(_) | Value::Float32(_) | Value::Float64(_)
    )
}

fn to_f64(v: &Value) -> f64 {
    match v {
        Value::Int32(x) => *x as f64,
        Value::Int64(x) => *x as f64,
        Value::Float32(x) => *x as f64,
        Value::Float64(x) => *x,
        _ => 0.0,
    }
}

/// Build a batch from evaluated rows: infer each output column's type, then
/// push coerced values.
fn build_batch_from_rows(spec: &[(String, expr::Expr)], rows: &[Vec<Value>]) -> RecordBatch {
    let mut cols: Vec<Column> = Vec::with_capacity(spec.len());
    for (ci, (name, _)) in spec.iter().enumerate() {
        let values: Vec<Value> = rows.iter().map(|r| r[ci].clone()).collect();
        cols.push(build_column(name.clone(), &values));
    }
    RecordBatch::new(cols)
}

fn build_column(name: String, values: &[Value]) -> Column {
    let data_type = unify_type(values);
    let mut col = Column::new(name, data_type, values.len());
    for v in values {
        col.push(coerce(v, data_type));
    }
    col
}

/// Partial accumulator for one aggregate spec.
#[derive(Debug, Clone)]
enum Acc {
    SumInt(i64),
    SumFloat(f64),
    Avg { sum: f64, count: u64 },
    Count(u64),
    Min(Value),
    Max(Value),
}

type GroupEntry<'a> = (&'a String, &'a (Vec<Value>, Vec<Acc>));

impl Acc {
    fn new(f: &str, first: &Value) -> Acc {
        match f {
            "sum" => match first {
                Value::Null => Acc::SumInt(0),
                Value::Int32(x) => Acc::SumInt(*x as i64),
                Value::Int64(x) => Acc::SumInt(*x),
                _ => Acc::SumFloat(avg_add(first).0),
            },
            "avg" => {
                let (sum, count) = avg_add(first);
                Acc::Avg { sum, count }
            }
            "count" => Acc::Count(if first == &Value::Null { 0 } else { 1 }),
            "count_all" => Acc::Count(1),
            "min" => Acc::Min(first.clone()),
            "max" => Acc::Max(first.clone()),
            _ => Acc::Count(0),
        }
    }

    fn add(&mut self, f: &str, v: &Value) {
        match self {
            Acc::SumInt(s) => match v {
                Value::Null => {}
                Value::Int32(x) => *s += *x as i64,
                Value::Int64(x) => *s += *x,
                x => {
                    let total = *s as f64 + avg_add(x).0;
                    *self = Acc::SumFloat(total);
                }
            },
            Acc::SumFloat(s) => {
                if v != &Value::Null {
                    *s += avg_add(v).0;
                }
            }
            Acc::Avg { sum, count } => {
                let (a, c) = avg_add(v);
                *sum += a;
                *count += c;
            }
            Acc::Count(c) => {
                if f == "count_all" || v != &Value::Null {
                    *c += 1;
                }
            }
            Acc::Min(m) => {
                if v != &Value::Null && compare_values(v, m) == Ordering::Less {
                    *m = v.clone();
                }
            }
            Acc::Max(m) => {
                if v != &Value::Null && compare_values(v, m) == Ordering::Greater {
                    *m = v.clone();
                }
            }
        }
    }

    fn finish(&self) -> Value {
        match self {
            Acc::SumInt(s) => Value::Int64(*s),
            Acc::SumFloat(s) => Value::Float64(*s),
            Acc::Avg { sum, count } => {
                if *count == 0 {
                    Value::Null
                } else {
                    Value::Float64(sum / *count as f64)
                }
            }
            Acc::Count(c) => Value::Int64(*c as i64),
            Acc::Min(m) | Acc::Max(m) => m.clone(),
        }
    }
}

fn avg_add(v: &Value) -> (f64, u64) {
    match v {
        Value::Null => (0.0, 0),
        Value::Int32(x) => (*x as f64, 1),
        Value::Int64(x) => (*x as f64, 1),
        Value::Float32(x) => (*x as f64, 1),
        Value::Float64(x) => (*x, 1),
        Value::Bool(b) => (if *b { 1.0 } else { 0.0 }, 1),
        Value::Date(d) => (*d as f64, 1),
        Value::Timestamp(t) => (*t as f64, 1),
        Value::Utf8(s) => (s.parse::<f64>().unwrap_or(0.0), 1),
    }
}
