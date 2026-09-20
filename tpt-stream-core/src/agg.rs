use crate::column::Column;
use crate::pipeline::{Error, Result};
use crate::table::RecordBatch;
use crate::value::{DataType, Value};
use std::collections::HashMap;
use std::io::BufWriter;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::OnceLock;
use tpt_stream_columnar::format::{ChunkedReader, ChunkedWriter};

static SPILL_COUNTER: AtomicUsize = AtomicUsize::new(0);

fn unique_id() -> u64 {
    static BASE: OnceLock<u64> = OnceLock::new();
    let base = *BASE.get_or_init(|| std::process::id() as u64);
    (base << 32) | SPILL_COUNTER.fetch_add(1, Ordering::Relaxed) as u64
}

/// Keys hash to one partition, so each key's partial aggregates always land in
/// the same file and merge locally at finish.
const SPILL_PARTITIONS: usize = 16;

/// Flush the group map once it exceeds this many groups.
const DEFAULT_GROUP_LIMIT: usize = 65536;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggFn {
    Sum,
    Avg,
    Count,
    Min,
    Max,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AggSpec {
    pub input: Option<String>,
    pub function: AggFn,
    pub output: String,
    strict: bool,
}

impl AggSpec {
    fn new(function: AggFn, input: Option<String>, output: String) -> Self {
        AggSpec {
            input,
            function,
            output,
            strict: false,
        }
    }

    pub fn sum(input: impl Into<String>) -> Self {
        let input = input.into();
        Self::new(AggFn::Sum, Some(input.clone()), format!("sum_{input}"))
    }

    pub fn avg(input: impl Into<String>) -> Self {
        let input = input.into();
        Self::new(AggFn::Avg, Some(input.clone()), format!("avg_{input}"))
    }

    pub fn count(input: impl Into<String>) -> Self {
        let input = input.into();
        Self::new(AggFn::Count, Some(input.clone()), format!("count_{input}"))
    }

    pub fn count_all(output: impl Into<String>) -> Self {
        Self::new(AggFn::Count, None, output.into())
    }

    pub fn min(input: impl Into<String>) -> Self {
        let input = input.into();
        Self::new(AggFn::Min, Some(input.clone()), format!("min_{input}"))
    }

    pub fn max(input: impl Into<String>) -> Self {
        let input = input.into();
        Self::new(AggFn::Max, Some(input.clone()), format!("max_{input}"))
    }

    /// Override the default output column name.
    pub fn with_output(mut self, output: impl Into<String>) -> Self {
        self.output = output.into();
        self
    }
}

/// Partial accumulator for one (column, function) pair.
#[derive(Debug, Clone)]
enum Acc {
    SumInt(i64),
    SumFloat(f64),
    Avg { sum: f64, count: u64 },
    Count(u64),
    Min(Value),
    Max(Value),
}

struct Group {
    key: Vec<Value>,
    accs: Vec<Acc>,
}

struct SpillState {
    id: u64,
    written: Vec<bool>,
}

pub struct GroupByAgg {
    keys: Vec<String>,
    specs: Vec<AggSpec>,
    map: HashMap<String, Group>,
    group_limit: usize,
    spilled: Option<SpillState>,
    /// First-batch-resolved key column types and per-spec input types.
    key_types: Vec<DataType>,
    out_types: Vec<Option<DataType>>,
    resolved: bool,
}

impl GroupByAgg {
    pub fn new(keys: Vec<String>, specs: Vec<AggSpec>) -> Self {
        let spec_len = specs.len();
        GroupByAgg {
            keys,
            specs,
            map: HashMap::new(),
            group_limit: DEFAULT_GROUP_LIMIT,
            spilled: None,
            key_types: Vec::new(),
            out_types: vec![None; spec_len],
            resolved: false,
        }
    }

    pub fn with_group_limit(mut self, limit: usize) -> Self {
        self.group_limit = limit;
        self
    }

    fn spill_path(&self, partition: usize) -> std::path::PathBuf {
        std::env::temp_dir().join(format!(
            "tpt-streamforge-agg-{}-p{partition}.tptcol",
            self.spilled.as_ref().expect("spill state").id
        ))
    }

    /// Number of spill columns held by one spec, plus their types.
    fn spill_width(&self, i: usize) -> (usize, Vec<(String, DataType)>) {
        let resolved = self.out_types[i];
        let spec = &self.specs[i];
        match spec.function {
            AggFn::Avg => (
                2,
                vec![
                    (format!("__a{i}_sum"), DataType::Float64),
                    (format!("__a{i}_cnt"), DataType::Int64),
                ],
            ),
            _ => (
                1,
                vec![(format!("__a{i}"), acc_output_type(spec, resolved))],
            ),
        }
    }

    fn spill_schema(&self) -> Vec<(String, DataType)> {
        let mut schema: Vec<(String, DataType)> = self
            .keys
            .iter()
            .enumerate()
            .map(|(i, _)| (format!("__k{i}"), self.key_types[i]))
            .collect();
        for i in 0..self.specs.len() {
            schema.extend(self.spill_width(i).1);
        }
        schema
    }

    fn group_to_row(&self, group: &Group) -> Vec<Value> {
        let mut row = group.key.clone();
        for (i, acc) in group.accs.iter().enumerate() {
            row.extend(acc_to_spill(acc, &self.specs[i], self.out_types[i]));
        }
        row
    }

    fn partial_row_batch(&self, rows: Vec<Vec<Value>>) -> Result<RecordBatch> {
        let schema = self.spill_schema();
        let mut columns = Vec::with_capacity(schema.len());
        for (ci, (name, dtype)) in schema.iter().enumerate() {
            let mut col = Column::new(name.clone(), *dtype, rows.len());
            for row in &rows {
                col.push(row.get(ci).cloned().unwrap_or(Value::Null));
            }
            columns.push(col);
        }
        Ok(RecordBatch::new(columns))
    }

    fn flush_map(&mut self) -> Result<()> {
        if self.spilled.is_none() {
            self.spilled = Some(SpillState {
                id: unique_id(),
                written: vec![false; SPILL_PARTITIONS],
            });
        }
        let mut per_partition: Vec<Vec<Vec<Value>>> = vec![Vec::new(); SPILL_PARTITIONS];
        for group in self.map.values() {
            let p = hash_key(&group.key) as usize % SPILL_PARTITIONS;
            per_partition[p].push(self.group_to_row(group));
        }
        self.map.clear();
        for (p, rows) in per_partition.into_iter().enumerate() {
            if rows.is_empty() {
                continue;
            }
            let path = self.spill_path(p);
            // Append: a partition file accumulates partials across many flushes.
            let file = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&path)?;
            let mut writer = ChunkedWriter::new(BufWriter::with_capacity(1 << 16, file), false);
            writer.write_batch(&self.partial_row_batch(rows)?)?;
            writer.finish()?;
            self.spilled.as_mut().unwrap().written[p] = true;
        }
        Ok(())
    }

    fn fold_partial_row(&mut self, row: &RecordBatch, ri: usize) -> Result<()> {
        let key: Vec<Value> = self
            .keys
            .iter()
            .enumerate()
            .map(|(ci, _)| row.cell(ri, &format!("__k{ci}")).unwrap_or(Value::Null))
            .collect();
        let canonical = encode_key(&key);
        let placeholder: Vec<Acc> = self.specs.iter().map(|_| Acc::Count(0)).collect();
        let specs = self.specs.clone();
        let widths: Vec<usize> = specs
            .iter()
            .enumerate()
            .map(|(i, _)| self.spill_width(i).0)
            .collect();
        let out_types = self.out_types.clone();
        let group = self.map.entry(canonical).or_insert_with(|| Group {
            key,
            accs: placeholder,
        });
        for (i, acc) in group.accs.iter_mut().enumerate() {
            let width = widths[i];
            let mut vals = Vec::with_capacity(width);
            for w in 0..width {
                let name = match width {
                    1 => format!("__a{i}"),
                    2 if w == 0 => format!("__a{i}_sum"),
                    _ => format!("__a{i}_cnt"),
                };
                vals.push(row.cell(ri, &name).unwrap_or(Value::Null));
            }
            let partial = acc_from_spill(&specs[i], out_types[i], vals);
            fold_acc(acc, &partial);
        }
        Ok(())
    }

    fn emit_final(&mut self) -> Result<Vec<RecordBatch>> {
        // Fold any spilled partials back into the map before emitting.
        let (id, written) = match &self.spilled {
            Some(s) => (s.id, s.written.clone()),
            None => (0, Vec::new()),
        };
        for p in 0..SPILL_PARTITIONS {
            if written.is_empty() || !written[p] {
                continue;
            }
            let path = std::env::temp_dir().join(format!("tpt-streamforge-agg-{id}-p{p}.tptcol"));
            let file = std::fs::File::open(&path)?;
            let mut reader = ChunkedReader::new(file);
            while let Some(batch) = reader.next_batch()? {
                for ri in 0..batch.num_rows() {
                    self.fold_partial_row(&batch, ri)?;
                }
            }
            std::fs::remove_file(path)?;
        }
        self.spilled = None;

        let mut out = Vec::new();
        if self.map.is_empty() {
            return Ok(out);
        }
        let mut columns: Vec<Column> = Vec::with_capacity(self.keys.len() + self.specs.len());
        for (ci, k) in self.keys.iter().enumerate() {
            columns.push(Column::new(k.clone(), self.key_types[ci], self.map.len()));
        }
        for (i, spec) in self.specs.iter().enumerate() {
            let t = acc_output_type(spec, self.out_types[i]);
            columns.push(Column::new(spec.output.clone(), t, self.map.len()));
        }
        for group in self.map.drain().map(|(_, g)| g) {
            for (ci, k) in group.key.iter().enumerate() {
                columns[ci].push(k.clone());
            }
            for (i, acc) in group.accs.iter().enumerate() {
                columns[self.keys.len() + i].push(acc_to_value(acc, self.out_types[i]));
            }
        }
        out.push(RecordBatch::new(columns));
        Ok(out)
    }

    fn resolve_schema(&mut self, batch: &RecordBatch) -> Result<()> {
        self.key_types = self
            .keys
            .iter()
            .map(|k| {
                batch.column(k).map(|c| c.data_type()).ok_or_else(|| {
                    Error::Schema(format!(
                        "group-by column '{k}' not found; available: {}",
                        batch.column_names().join(", ")
                    ))
                })
            })
            .collect::<Result<Vec<_>>>()?;

        for (i, spec) in self.specs.iter().enumerate() {
            if let Some(col) = &spec.input {
                let t = batch
                    .column(col)
                    .ok_or_else(|| {
                        Error::Schema(format!(
                            "aggregation column '{col}' not found; available: {}",
                            batch.column_names().join(", ")
                        ))
                    })?
                    .data_type();
                match spec.function {
                    AggFn::Sum | AggFn::Avg => match t {
                        DataType::Int32
                        | DataType::Int64
                        | DataType::Float32
                        | DataType::Float64 => {}
                        _ => {
                            return Err(Error::Schema(format!(
                                "{:?} not supported for {t} column '{col}'",
                                spec.function
                            )))
                        }
                    },
                    AggFn::Min | AggFn::Max => {
                        if t == DataType::Bool {
                            return Err(Error::Schema(format!(
                                "{:?} not supported for bool column '{col}'",
                                spec.function
                            )));
                        }
                    }
                    AggFn::Count => {}
                }
                self.out_types[i] = Some(t);
            }
        }
        self.resolved = true;
        Ok(())
    }

    fn accumulate_row(&mut self, batch: &RecordBatch, ri: usize) -> Result<()> {
        if !self.resolved {
            self.resolve_schema(batch)?;
        }

        let key: Vec<Value> = self
            .keys
            .iter()
            .map(|k| batch.cell(ri, k).unwrap_or(Value::Null))
            .collect();
        let canonical = encode_key(&key);

        if !self.map.contains_key(&canonical) && self.map.len() >= self.group_limit {
            self.flush_map()?;
        }

        let group = self.map.entry(canonical).or_insert_with(|| Group {
            key,
            accs: self.specs.iter().map(|_| Acc::Count(0)).collect(),
        });

        for (i, spec) in self.specs.iter().enumerate() {
            let acc = &mut group.accs[i];
            let running = acc.clone();
            *acc = accumulate(&running, spec, batch, ri)?;
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Aggregation math
// ---------------------------------------------------------------------------

fn accumulate(running: &Acc, spec: &AggSpec, batch: &RecordBatch, ri: usize) -> Result<Acc> {
    let (Some(col), function) = (&spec.input, spec.function) else {
        if spec.function == AggFn::Count {
            return match running {
                Acc::Count(c) => Ok(Acc::Count(c + 1)),
                _ => panic!("internal: Count accumulator expected"),
            };
        }
        return Err(Error::Schema(
            "aggregation needs an input column (except COUNT(*))".into(),
        ));
    };

    let v = batch.cell(ri, col).unwrap_or(Value::Null);
    if matches!(v, Value::Null) {
        return Ok(running.clone());
    }
    if function == AggFn::Count {
        return match running {
            Acc::Count(c) => Ok(Acc::Count(c + 1)),
            _ => panic!("internal: Count accumulator expected"),
        };
    }

    match (running, function) {
        (Acc::SumInt(s), AggFn::Sum) => Ok(Acc::SumInt(s + as_i64(&v)?)),
        (Acc::SumFloat(s), AggFn::Sum) => Ok(Acc::SumFloat(s + as_f64(&v)?)),
        (_, AggFn::Sum) => match v {
            Value::Int32(_) | Value::Int64(_) => Ok(Acc::SumInt(as_i64(&v)?)),
            _ => Ok(Acc::SumFloat(as_f64(&v)?)),
        },
        (Acc::Avg { sum, count }, AggFn::Avg) => Ok(Acc::Avg {
            sum: sum + as_f64(&v)?,
            count: count + 1,
        }),
        (_, AggFn::Avg) => Ok(Acc::Avg {
            sum: as_f64(&v)?,
            count: 1,
        }),
        (Acc::Min(m), AggFn::Min) => Ok(Acc::Min(if value_less(&v, m) { v } else { m.clone() })),
        (_, AggFn::Min) => Ok(Acc::Min(v)),
        (Acc::Max(m), AggFn::Max) => Ok(Acc::Max(if value_greater(&v, m) { v } else { m.clone() })),
        (_, AggFn::Max) => Ok(Acc::Max(v)),
        (_, AggFn::Count) => unreachable!("COUNT handled before match"),
    }
}

fn as_i64(v: &Value) -> Result<i64> {
    match v {
        Value::Int32(x) => Ok(*x as i64),
        Value::Int64(x) => Ok(*x),
        Value::Float32(x) => Ok(*x as i64),
        Value::Float64(x) => Ok(*x as i64),
        _ => Err(Error::Schema(format!(
            "cannot aggregate value {v:?} as integer"
        ))),
    }
}

fn as_f64(v: &Value) -> Result<f64> {
    match v {
        Value::Int32(x) => Ok(*x as f64),
        Value::Int64(x) => Ok(*x as f64),
        Value::Float32(x) => Ok(*x as f64),
        Value::Float64(x) => Ok(*x),
        _ => Err(Error::Schema(format!(
            "cannot aggregate value {v:?} as float"
        ))),
    }
}

fn value_less(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Int32(x), Value::Int32(y)) => x < y,
        (Value::Int64(x), Value::Int64(y)) => x < y,
        (Value::Float32(x), Value::Float32(y)) => x < y,
        (Value::Float64(x), Value::Float64(y)) => x < y,
        (Value::Utf8(x), Value::Utf8(y)) => x < y,
        (Value::Bool(x), Value::Bool(y)) => x < y,
        _ => {
            let a = as_f64(a).unwrap_or(f64::NAN);
            let b = as_f64(b).unwrap_or(f64::NAN);
            a < b
        }
    }
}

fn value_greater(a: &Value, b: &Value) -> bool {
    !value_less(a, b) && a != b
}

fn fold_acc(acc: &mut Acc, partial: &Acc) {
    let next = match (&*acc, partial) {
        (Acc::SumInt(a), Acc::SumInt(b)) => Acc::SumInt(*a + *b),
        (Acc::SumFloat(a), Acc::SumFloat(b)) => Acc::SumFloat(*a + *b),
        (
            Acc::Avg { sum, count },
            Acc::Avg {
                sum: bsum,
                count: bcount,
            },
        ) => Acc::Avg {
            sum: *sum + *bsum,
            count: *count + *bcount,
        },
        (Acc::Count(a), Acc::Count(b)) => Acc::Count(*a + *b),
        (Acc::Min(a), Acc::Min(b)) => Acc::Min(if value_less(b, a) {
            b.clone()
        } else {
            a.clone()
        }),
        (Acc::Max(a), Acc::Max(b)) => Acc::Max(if value_greater(b, a) {
            b.clone()
        } else {
            a.clone()
        }),
        // Fresh group (Count(0) sentinel): adopt the partial outright.
        _ => partial.clone(),
    };
    *acc = next;
}

fn acc_output_type(spec: &AggSpec, resolved: Option<DataType>) -> DataType {
    match spec.function {
        AggFn::Sum => match resolved {
            Some(DataType::Float32) | Some(DataType::Float64) => DataType::Float64,
            _ => DataType::Int64,
        },
        AggFn::Count => DataType::Int64,
        AggFn::Avg => DataType::Float64,
        AggFn::Min | AggFn::Max => resolved.unwrap_or(DataType::Utf8),
    }
}

fn acc_to_value(acc: &Acc, _out_type: Option<DataType>) -> Value {
    match acc {
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
        Acc::Min(v) => v.clone(),
        Acc::Max(v) => v.clone(),
    }
}

/// Spill-column values for an accumulator (AVG spills sum+count).
fn acc_to_spill(acc: &Acc, spec: &AggSpec, out_type: Option<DataType>) -> Vec<Value> {
    match (acc, spec.function) {
        (Acc::Avg { sum, count }, AggFn::Avg) => {
            vec![Value::Float64(*sum), Value::Int64(*count as i64)]
        }
        _ => vec![acc_to_value(acc, out_type)],
    }
}

fn acc_from_spill(spec: &AggSpec, _out_type: Option<DataType>, vals: Vec<Value>) -> Acc {
    match spec.function {
        AggFn::Avg => match (vals.first(), vals.get(1)) {
            (Some(Value::Float64(sum)), Some(Value::Int64(count))) => Acc::Avg {
                sum: *sum,
                count: *count as u64,
            },
            _ => Acc::Avg { sum: 0.0, count: 0 },
        },
        AggFn::Sum | AggFn::Count => match vals.into_iter().next() {
            Some(Value::Int64(x)) => Acc::SumInt(x),
            Some(Value::Float64(x)) => Acc::SumFloat(x),
            Some(Value::Int32(x)) => Acc::SumInt(x as i64),
            _ => Acc::Count(0),
        },
        AggFn::Min | AggFn::Max => match vals.into_iter().next() {
            Some(v) => Acc::Min(v),
            None => Acc::Count(0),
        },
    }
}

/// Canonical, unambiguous string key for a group.
fn encode_key(values: &[Value]) -> String {
    let mut out = String::new();
    for v in values {
        match v {
            Value::Int32(x) => {
                out.push_str("i:");
                for b in x.to_le_bytes() {
                    out.push_str(&format!("{b:02x}"));
                }
            }
            Value::Int64(x) => {
                out.push_str("j:");
                for b in x.to_le_bytes() {
                    out.push_str(&format!("{b:02x}"));
                }
            }
            Value::Float32(x) => {
                out.push_str("f:");
                for b in x.to_le_bytes() {
                    out.push_str(&format!("{b:02x}"));
                }
            }
            Value::Float64(x) => {
                out.push_str("g:");
                for b in x.to_le_bytes() {
                    out.push_str(&format!("{b:02x}"));
                }
            }
            Value::Bool(x) => out.push_str(if *x { "t" } else { "f" }),
            Value::Utf8(s) => {
                out.push_str("s:");
                out.push_str(&s.len().to_string());
                out.push(':');
                out.push_str(s);
            }
            Value::Date(x) => {
                out.push_str("D:");
                for b in x.to_le_bytes() {
                    out.push_str(&format!("{b:02x}"));
                }
            }
            Value::Timestamp(x) => {
                out.push_str("T:");
                for b in x.to_le_bytes() {
                    out.push_str(&format!("{b:02x}"));
                }
            }
            Value::Null => out.push('n'),
        }
    }
    out
}

fn hash_key(values: &[Value]) -> u64 {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    let mut h = DefaultHasher::new();
    for v in values {
        match v {
            Value::Int32(x) => x.hash(&mut h),
            Value::Int64(x) => x.hash(&mut h),
            Value::Float32(x) => x.to_bits().hash(&mut h),
            Value::Float64(x) => x.to_bits().hash(&mut h),
            Value::Bool(x) => x.hash(&mut h),
            Value::Utf8(s) => s.hash(&mut h),
            Value::Date(x) => x.hash(&mut h),
            Value::Timestamp(x) => x.hash(&mut h),
            Value::Null => 0u8.hash(&mut h),
        }
    }
    h.finish()
}

// ---------------------------------------------------------------------------
// PipelineStage impl
// ---------------------------------------------------------------------------

#[async_trait::async_trait]
impl crate::pipeline::PipelineStage for GroupByAgg {
    fn name(&self) -> &'static str {
        "GroupByAgg"
    }

    async fn process(&mut self, batch: RecordBatch) -> Result<Vec<RecordBatch>> {
        for ri in 0..batch.num_rows() {
            self.accumulate_row(&batch, ri)?;
        }
        Ok(Vec::new())
    }

    async fn finish(&mut self) -> Result<Vec<RecordBatch>> {
        self.emit_final()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::column::Column;
    use crate::pipeline::PipelineStage;

    fn batch() -> RecordBatch {
        let mut cat = Column::new("cat", DataType::Utf8, 4);
        cat.push(Value::Utf8("a".into()));
        cat.push(Value::Utf8("b".into()));
        cat.push(Value::Utf8("a".into()));
        cat.push(Value::Utf8("c".into()));
        let mut price = Column::new("price", DataType::Float64, 4);
        price.push(Value::Float64(10.0));
        price.push(Value::Float64(20.0));
        price.push(Value::Float64(30.0));
        price.push(Value::Float64(5.0));
        let mut qty = Column::new("qty", DataType::Int32, 4);
        qty.push(Value::Int32(1));
        qty.push(Value::Int32(2));
        qty.push(Value::Int32(3));
        qty.push(Value::Int32(4));
        RecordBatch::new(vec![cat, price, qty])
    }

    #[tokio::test]
    async fn aggregate_groups() {
        let mut stage = GroupByAgg::new(
            vec!["cat".into()],
            vec![
                AggSpec::sum("price"),
                AggSpec::avg("price"),
                AggSpec::count("qty"),
                AggSpec::count_all("total"),
                AggSpec::min("qty"),
                AggSpec::max("price"),
            ],
        );
        stage.process(batch()).await.unwrap();
        let out = stage.finish().await.unwrap();
        assert_eq!(out.len(), 1);
        let b = &out[0];
        assert_eq!(b.num_rows(), 3);
        for ri in 0..3 {
            let cat = match b.cell(ri, "cat").unwrap() {
                Value::Utf8(s) => s,
                _ => panic!(),
            };
            let sum = match b.cell(ri, "sum_price").unwrap() {
                Value::Float64(s) => s,
                v => panic!("{v:?}"),
            };
            let avg = match b.cell(ri, "avg_price").unwrap() {
                Value::Float64(s) => s,
                v => panic!("{v:?}"),
            };
            let count = match b.cell(ri, "count_qty").unwrap() {
                Value::Int64(s) => s,
                v => panic!("{v:?}"),
            };
            let total = match b.cell(ri, "total").unwrap() {
                Value::Int64(s) => s,
                v => panic!("{v:?}"),
            };
            let min = match b.cell(ri, "min_qty").unwrap() {
                Value::Int32(s) => s,
                v => panic!("{v:?}"),
            };
            let max = match b.cell(ri, "max_price").unwrap() {
                Value::Float64(s) => s,
                v => panic!("{v:?}"),
            };
            match cat.as_str() {
                "a" => {
                    // COUNT(*) counts the rows in this group (SQL semantics).
                    assert_eq!((sum, count, total, min, max), (40.0, 2, 2, 1, 30.0));
                    assert!((avg - 20.0).abs() < 1e-9);
                }
                "b" => assert_eq!((sum, count, total, min, max), (20.0, 1, 1, 2, 20.0)),
                "c" => assert_eq!((sum, count, total, min, max), (5.0, 1, 1, 4, 5.0)),
                other => panic!("unexpected cat {other}"),
            }
        }
    }

    #[tokio::test]
    async fn aggregate_nulls_skipped() {
        let mut cat = Column::new("cat", DataType::Utf8, 2);
        cat.push(Value::Utf8("a".into()));
        cat.push(Value::Utf8("a".into()));
        let mut price = Column::new("price", DataType::Int32, 2);
        price.push(Value::Int32(5));
        price.push(Value::Null);
        let b = RecordBatch::new(vec![cat, price]);

        let mut stage = GroupByAgg::new(
            vec!["cat".into()],
            vec![
                AggSpec::sum("price"),
                AggSpec::count("price"),
                AggSpec::count_all("total"),
            ],
        );
        stage.process(b).await.unwrap();
        let out = stage.finish().await.unwrap();
        let b = &out[0];
        assert_eq!(b.cell(0, "sum_price"), Some(Value::Int64(5)));
        assert_eq!(b.cell(0, "count_price"), Some(Value::Int64(1)));
        assert_eq!(b.cell(0, "total"), Some(Value::Int64(2)));
    }

    #[tokio::test]
    async fn aggregate_spills() {
        let mut stage = GroupByAgg::new(
            vec!["cat".into()],
            vec![AggSpec::sum("price"), AggSpec::avg("price")],
        )
        .with_group_limit(2);
        for _ in 0..10 {
            stage.process(batch()).await.unwrap();
        }
        let out = stage.finish().await.unwrap();
        assert_eq!(out.len(), 1);
        let b = &out[0];
        assert_eq!(b.num_rows(), 3);
        for ri in 0..3 {
            let cat = match b.cell(ri, "cat").unwrap() {
                Value::Utf8(s) => s,
                _ => panic!(),
            };
            let sum = match b.cell(ri, "sum_price").unwrap() {
                Value::Float64(s) => s,
                v => panic!("{v:?}"),
            };
            let avg = match b.cell(ri, "avg_price").unwrap() {
                Value::Float64(s) => s,
                v => panic!("{v:?}"),
            };
            match cat.as_str() {
                "a" => {
                    assert_eq!(sum, 40.0 * 10.0);
                    assert!((avg - 20.0).abs() < 1e-9);
                }
                "b" => {
                    assert_eq!(sum, 20.0 * 10.0);
                    assert!((avg - 20.0).abs() < 1e-9);
                }
                "c" => {
                    assert_eq!(sum, 5.0 * 10.0);
                    assert!((avg - 5.0).abs() < 1e-9);
                }
                other => panic!("unexpected cat {other}"),
            }
        }
    }
}
