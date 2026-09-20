use crate::column::Column;
use crate::pipeline::{Error, Result};
use crate::table::RecordBatch;
use crate::value::{DataType, Value};
use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::io::BufWriter;
use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
use std::sync::Arc;
use tpt_stream_columnar::format::{ChunkedReader, ChunkedWriter};

static SORT_ID: AtomicUsize = AtomicUsize::new(0);

/// Flush the in-memory run once it accumulates this many rows.
const DEFAULT_RUN_LIMIT: usize = 1 << 20;

/// Sort-able byte encoding of a single cell. Numeric encodings are
/// byte-order-comparable with the standard operator ordering.
fn encode_cell(value: &Value) -> Vec<u8> {
    match value {
        Value::Int32(v) => ((*v as u32) ^ 0x8000_0000).to_be_bytes().to_vec(),
        Value::Int64(v) => ((*v as u64) ^ 0x8000_0000_0000_0000).to_be_bytes().to_vec(),
        Value::Float32(v) => encode_f32_bits(v, u32::to_be_bytes).to_vec(),
        Value::Float64(v) => encode_f64_bits(v, u64::to_be_bytes).to_vec(),
        Value::Bool(v) => vec![u8::from(*v)],
        Value::Date(v) => ((*v as u32) ^ 0x8000_0000).to_be_bytes().to_vec(),
        Value::Timestamp(v) => ((*v as u64) ^ 0x8000_0000_0000_0000).to_be_bytes().to_vec(),
        Value::Utf8(s) => s.as_bytes().to_vec(),
        Value::Null => Vec::new(),
    }
}

fn encode_f32_bits(v: &f32, conv: impl FnOnce(u32) -> [u8; 4]) -> [u8; 4] {
    let bits = v.to_bits();
    let ordered = if v.is_sign_negative() {
        !bits
    } else {
        bits | 0x8000_0000
    };
    conv(ordered)
}

fn encode_f64_bits(v: &f64, conv: impl FnOnce(u64) -> [u8; 8]) -> [u8; 8] {
    let bits = v.to_bits();
    let ordered = if v.is_sign_negative() {
        !bits
    } else {
        bits | 0x8000_0000_0000_0000
    };
    conv(ordered)
}

#[derive(Clone)]
struct SortKey {
    cells: Vec<Vec<u8>>,
}

struct Run {
    reader: ChunkedReader<std::fs::File>,
    batch: Option<RecordBatch>,
    cursor: usize,
}

impl Run {
    fn next_row(&mut self) -> Result<Option<Vec<Value>>> {
        loop {
            if let Some(batch) = &self.batch {
                if self.cursor < batch.num_rows() {
                    let row = batch
                        .columns()
                        .iter()
                        .map(|c| c.get(self.cursor).unwrap_or(Value::Null))
                        .collect();
                    self.cursor += 1;
                    return Ok(Some(row));
                }
            }
            match self.reader.next_batch()? {
                Some(b) => {
                    self.batch = Some(b);
                    self.cursor = 0;
                }
                None => return Ok(None),
            }
        }
    }
}

struct HeapItem {
    key: SortKey,
    run: usize,
    desc: Arc<Vec<bool>>,
}

impl PartialEq for HeapItem {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for HeapItem {}

impl PartialOrd for HeapItem {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for HeapItem {
    fn cmp(&self, other: &Self) -> Ordering {
        // Reverse so BinaryHeap acts as a min-heap (pop = smallest key).
        compare_keys(&other.key, &self.key, &self.desc)
    }
}

fn compare_keys(a: &SortKey, b: &SortKey, desc: &[bool]) -> Ordering {
    for (i, (a_cell, b_cell)) in a.cells.iter().zip(b.cells.iter()).enumerate() {
        let ord = a_cell.cmp(b_cell);
        if ord != Ordering::Equal {
            return if desc.get(i).copied().unwrap_or(false) {
                ord.reverse()
            } else {
                ord
            };
        }
    }
    Ordering::Equal
}

pub struct Sort {
    sort_cols: Vec<String>,
    descending: Vec<bool>,
    run_limit: usize,
    buffered: Vec<RecordBatch>,
    buffered_rows: usize,
    runs: Vec<std::path::PathBuf>,
    id: usize,
    schema: Option<Vec<(String, DataType)>>,
}

impl Sort {
    pub fn new(sort_cols: Vec<String>, descending: Vec<bool>) -> Self {
        Sort {
            sort_cols,
            descending,
            run_limit: DEFAULT_RUN_LIMIT,
            buffered: Vec::new(),
            buffered_rows: 0,
            runs: Vec::new(),
            id: SORT_ID.fetch_add(1, AtomicOrdering::Relaxed),
            schema: None,
        }
    }

    pub fn with_run_limit(mut self, limit: usize) -> Self {
        self.run_limit = limit;
        self
    }

    fn resolve_schema(&mut self, batch: &RecordBatch) -> Result<()> {
        let schema = batch.schema();
        for col in &self.sort_cols {
            if !schema.iter().any(|(name, _)| name == col) {
                return Err(Error::Schema(format!(
                    "sort column '{col}' not found; available: {}",
                    batch.column_names().join(", ")
                )));
            }
        }
        self.schema = Some(schema);
        Ok(())
    }

    fn sort_keys_for(&self, batch: &RecordBatch, ri: usize) -> SortKey {
        let cells = self
            .sort_cols
            .iter()
            .map(|col| encode_cell(&batch.cell(ri, col).unwrap_or(Value::Null)))
            .collect();
        SortKey { cells }
    }

    fn run_path(&self, run_index: usize) -> std::path::PathBuf {
        std::env::temp_dir().join(format!(
            "tpt-streamforge-sort-{}-r{run_index}.tptcol",
            self.id
        ))
    }

    /// Sort everything currently buffered and write it out as one sorted run.
    fn spill_run(&mut self) -> Result<()> {
        let batches = std::mem::take(&mut self.buffered);
        self.buffered_rows = 0;
        if batches.is_empty() {
            return Ok(());
        }
        let run_index = self.runs.len();
        let path = self.run_path(run_index);
        let mut rows: Vec<(SortKey, usize, usize)> =
            Vec::with_capacity(self.buffered_rows + batches[0].num_rows());
        for (bi, batch) in batches.iter().enumerate() {
            for ri in 0..batch.num_rows() {
                rows.push((self.sort_keys_for(batch, ri), bi, ri));
            }
        }
        let desc = Arc::new(self.descending.clone());
        rows.sort_by(|a, b| compare_keys(&a.0, &b.0, &desc));

        let schema = self
            .schema
            .as_ref()
            .expect("schema resolved")
            .iter()
            .map(|(n, t)| (n.clone(), *t))
            .collect::<Vec<_>>();
        let file = std::fs::File::create(&path)?;
        let mut writer = ChunkedWriter::new(BufWriter::with_capacity(1 << 16, file), false);
        let mut columns: Option<Vec<Column>> = None;
        for (_, bi, ri) in &rows {
            if columns.is_none() {
                columns = Some(
                    schema
                        .iter()
                        .map(|(name, dtype)| {
                            Column::new(name.clone(), *dtype, crate::DEFAULT_CHUNK_ROWS)
                        })
                        .collect(),
                );
            }
            let batch = &batches[*bi];
            let cols = columns.as_mut().expect("inited");
            for (ci, _) in schema.iter().enumerate() {
                cols[ci].push(
                    batch
                        .column_by_index(ci)
                        .unwrap()
                        .get(*ri)
                        .unwrap_or(Value::Null),
                );
            }
            if cols[0].len() >= crate::DEFAULT_CHUNK_ROWS {
                let done = columns.take().expect("inited");
                writer.write_batch(&RecordBatch::new(done))?;
            }
        }
        if let Some(cols) = columns {
            if !cols[0].is_empty() {
                writer.write_batch(&RecordBatch::new(cols))?;
            }
        }
        writer.finish()?;
        self.runs.push(path);
        Ok(())
    }

    fn emit_sorted_memory(&mut self) -> Result<Vec<RecordBatch>> {
        let batches = std::mem::take(&mut self.buffered);
        self.buffered_rows = 0;
        if batches.is_empty() {
            return Ok(Vec::new());
        }
        let desc = Arc::new(self.descending.clone());
        let mut rows: Vec<(SortKey, usize, usize)> = Vec::with_capacity(self.buffered_rows);
        for (bi, batch) in batches.iter().enumerate() {
            for ri in 0..batch.num_rows() {
                rows.push((self.sort_keys_for(batch, ri), bi, ri));
            }
        }
        rows.sort_by(|a, b| compare_keys(&a.0, &b.0, &desc));
        Ok(self.assemble(&rows, &batches, self.schema.as_ref().expect("schema")))
    }

    fn assemble(
        &self,
        rows: &[(SortKey, usize, usize)],
        batches: &[RecordBatch],
        schema: &[(String, DataType)],
    ) -> Vec<RecordBatch> {
        let mut out = Vec::new();
        let mut columns: Option<Vec<Column>> = None;
        for (_, bi, ri) in rows {
            if columns.is_none() {
                columns = Some(
                    schema
                        .iter()
                        .map(|(name, dtype)| {
                            Column::new(name.clone(), *dtype, crate::DEFAULT_CHUNK_ROWS)
                        })
                        .collect(),
                );
            }
            let batch = &batches[*bi];
            let cols = columns.as_mut().expect("inited");
            for (ci, _) in schema.iter().enumerate() {
                cols[ci].push(
                    batch
                        .column_by_index(ci)
                        .unwrap()
                        .get(*ri)
                        .unwrap_or(Value::Null),
                );
            }
            if cols[0].len() >= crate::DEFAULT_CHUNK_ROWS {
                let done = columns.take().expect("inited");
                out.push(RecordBatch::new(done));
            }
        }
        if let Some(cols) = columns {
            if !cols[0].is_empty() {
                out.push(RecordBatch::new(cols));
            }
        }
        out
    }

    fn merge_runs(&mut self) -> Result<Vec<RecordBatch>> {
        let desc = Arc::new(self.descending.clone());
        let schema = self.schema.as_ref().expect("schema").clone();

        let mut runs: Vec<Run> = Vec::with_capacity(self.runs.len());
        for path in &self.runs {
            let file = std::fs::File::open(path)?;
            let run = Run {
                reader: ChunkedReader::new(file),
                batch: None,
                cursor: 0,
            };
            runs.push(run);
        }
        if runs.is_empty() {
            return Ok(Vec::new());
        }

        let mut heap: BinaryHeap<HeapItem> = BinaryHeap::with_capacity(runs.len());
        // Refill: each top-of-heap item is a pending row from a run. We pull the
        // row lazily so the key is computed here from the run's current row.
        let mut current: Vec<Option<Vec<Value>>> = runs.iter_mut().map(|_| None).collect();
        for (idx, run) in runs.iter_mut().enumerate() {
            current[idx] = run.next_row()?;
        }
        for (idx, row) in current.iter().enumerate() {
            if let Some(row) = row {
                let key = self.sort_key_from_values(row);
                heap.push(HeapItem {
                    key,
                    run: idx,
                    desc: desc.clone(),
                });
            }
        }

        let mut out = Vec::new();
        let mut columns: Option<Vec<Column>> = None;
        while let Some(item) = heap.pop() {
            let row = current[item.run].take().expect("row present");
            if columns.is_none() {
                columns = Some(
                    schema
                        .iter()
                        .map(|(name, dtype)| {
                            Column::new(name.clone(), *dtype, crate::DEFAULT_CHUNK_ROWS)
                        })
                        .collect(),
                );
            }
            let cols = columns.as_mut().expect("inited");
            for (ci, value) in row.into_iter().enumerate() {
                cols[ci].push(value);
            }
            if cols[0].len() >= crate::DEFAULT_CHUNK_ROWS {
                let done = columns.take().expect("inited");
                out.push(RecordBatch::new(done));
            }
            // Pull the next row from this run (if any) and re-push.
            if let Some(next_row) = runs[item.run].next_row()? {
                let key = self.sort_key_from_values(&next_row);
                current[item.run] = Some(next_row);
                heap.push(HeapItem {
                    key,
                    run: item.run,
                    desc: desc.clone(),
                });
            } else {
                current[item.run] = None;
            }
        }
        if let Some(cols) = columns {
            if !cols[0].is_empty() {
                out.push(RecordBatch::new(cols));
            }
        }
        Ok(out)
    }

    fn sort_key_from_values(&self, row: &[Value]) -> SortKey {
        let cells = self
            .sort_cols
            .iter()
            .map(|col| {
                let idx = self
                    .schema
                    .as_ref()
                    .and_then(|s| s.iter().position(|(name, _)| name == col));
                match idx {
                    Some(i) => encode_cell(row.get(i).unwrap_or(&Value::Null)),
                    None => Vec::new(),
                }
            })
            .collect();
        SortKey { cells }
    }
}

#[async_trait::async_trait]
impl crate::pipeline::PipelineStage for Sort {
    fn name(&self) -> &'static str {
        "Sort"
    }

    async fn process(&mut self, batch: RecordBatch) -> Result<Vec<RecordBatch>> {
        if self.schema.is_none() {
            self.resolve_schema(&batch)?;
        }
        self.buffered_rows += batch.num_rows();
        self.buffered.push(batch);
        if self.buffered_rows >= self.run_limit {
            self.spill_run()?;
        }
        Ok(Vec::new())
    }

    async fn finish(&mut self) -> Result<Vec<RecordBatch>> {
        self.spill_run()?;
        if self.runs.is_empty() {
            return self.emit_sorted_memory();
        }
        let result = self.merge_runs()?;
        for path in std::mem::take(&mut self.runs) {
            let _ = std::fs::remove_file(path);
        }
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::column::Column;
    use crate::pipeline::PipelineStage;

    fn sample_batches() -> (RecordBatch, RecordBatch) {
        let mut id = Column::new("id", DataType::Int32, 4);
        for v in [3, 1, 4, 2] {
            id.push(Value::Int32(v));
        }
        let mut s = Column::new("name", DataType::Utf8, 4);
        for v in ["c", "a", "d", "b"] {
            s.push(Value::Utf8(v.into()));
        }
        let b1 = RecordBatch::new(vec![id, s]);

        let mut id2 = Column::new("id", DataType::Int32, 3);
        for v in [0, 9, 5] {
            id2.push(Value::Int32(v));
        }
        let mut s2 = Column::new("name", DataType::Utf8, 3);
        for v in ["z", "k", "e"] {
            s2.push(Value::Utf8(v.into()));
        }
        let b2 = RecordBatch::new(vec![id2, s2]);
        (b1, b2)
    }

    #[tokio::test]
    async fn sort_in_memory() {
        let (b1, b2) = sample_batches();
        let mut stage = Sort::new(vec!["id".into()], vec![false]);
        stage.process(b1).await.unwrap();
        stage.process(b2).await.unwrap();
        let out = stage.finish().await.unwrap();
        let ids: Vec<i32> = out
            .iter()
            .flat_map(|b| {
                (0..b.num_rows()).map(|r| match b.cell(r, "id").unwrap() {
                    Value::Int32(v) => v,
                    v => panic!("{v:?}"),
                })
            })
            .collect();
        assert_eq!(ids, vec![0, 1, 2, 3, 4, 5, 9]);
    }

    #[tokio::test]
    async fn sort_descending() {
        let (b1, b2) = sample_batches();
        let mut stage = Sort::new(vec!["name".into()], vec![true]);
        stage.process(b1).await.unwrap();
        stage.process(b2).await.unwrap();
        let out = stage.finish().await.unwrap();
        let names: Vec<String> = out
            .iter()
            .flat_map(|b| {
                (0..b.num_rows()).map(|r| match b.cell(r, "name").unwrap() {
                    Value::Utf8(v) => v,
                    v => panic!("{v:?}"),
                })
            })
            .collect();
        assert_eq!(names, vec!["z", "k", "e", "d", "c", "b", "a"]);
    }

    #[tokio::test]
    async fn sort_spills_to_runs() {
        let mut stage = Sort::new(vec!["id".into()], vec![false]).with_run_limit(4);
        for _ in 0..5 {
            let (a, b) = sample_batches();
            stage.process(a).await.unwrap();
            stage.process(b).await.unwrap();
        }
        let out = stage.finish().await.unwrap();
        let ids: Vec<i32> = out
            .iter()
            .flat_map(|b| {
                (0..b.num_rows()).map(|r| match b.cell(r, "id").unwrap() {
                    Value::Int32(v) => v,
                    v => panic!("{v:?}"),
                })
            })
            .collect();
        let mut expected_all: Vec<i32> = Vec::new();
        for _ in 0..5 {
            for v in [3, 1, 4, 2, 0, 9, 5] {
                expected_all.push(v);
            }
        }
        expected_all.sort();
        assert_eq!(ids, expected_all, "merged runs must be globally sorted");
    }
}
