use crate::column::Column;
use crate::pipeline::{Error, Result};
use crate::table::RecordBatch;
use crate::value::{DataType, Value};
use std::collections::{HashMap, HashSet};
use std::io::BufWriter;
use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
use tpt_stream_columnar::format::{ChunkedReader, ChunkedWriter};

static JOIN_ID: AtomicUsize = AtomicUsize::new(0);

/// Number of build-key partitions; the probe looks up exactly one partition.
const JOIN_SPILL_PARTITIONS: usize = 16;

/// Once the in-memory build table exceeds this many entries, excess entries are
/// spilled to disk (hash-partitioned) instead of RAM.
const DEFAULT_BUILD_LIMIT: usize = 1 << 20;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
}

struct BuildEntry {
    key: String,
    row: Vec<Value>,
}

/// Deterministic key for join comparisons. Nulls are never matched.
fn key_string(values: &[&Value]) -> Option<String> {
    let mut out = String::new();
    for v in values {
        match v {
            Value::Null => return None,
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
        }
    }
    Some(out)
}

fn hash_key(values: &[&Value]) -> u64 {
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
            Value::Null => 0u8.hash(&mut h),
        }
    }
    h.finish()
}

struct SpillState {
    id: usize,
    written: Vec<bool>,
}

pub struct HashJoin {
    left_keys: Vec<String>,
    right_keys: Vec<String>,
    join_type: JoinType,
    right_schema: Vec<(String, DataType)>,
    map: HashMap<String, Vec<u32>>,
    build_rows: Vec<BuildEntry>,
    spill: Option<SpillState>,
    build_limit: usize,
    left_schema: Option<Vec<(String, DataType)>>,
    output_schema: Option<Vec<(String, DataType)>>,
    matched: HashSet<String>,
}

impl HashJoin {
    pub fn new(
        left_keys: Vec<String>,
        right_keys: Vec<String>,
        right_schema: Vec<(String, DataType)>,
        join_type: JoinType,
    ) -> Self {
        HashJoin {
            left_keys,
            right_keys,
            join_type,
            right_schema,
            map: HashMap::new(),
            build_rows: Vec::new(),
            spill: None,
            build_limit: DEFAULT_BUILD_LIMIT,
            left_schema: None,
            output_schema: None,
            matched: HashSet::new(),
        }
    }

    pub fn with_build_limit(mut self, limit: usize) -> Self {
        self.build_limit = limit;
        self
    }

    pub fn join_type(&self) -> JoinType {
        self.join_type
    }

    pub fn add_build_row(&mut self, row: Vec<Value>) -> Result<()> {
        let key = self
            .key_from_row(&self.right_keys, &row)
            .ok_or_else(|| Error::Schema("join key contains null on the build side".into()))?;
        let partition = (hash_key(&self.row_key_values(&self.right_keys, &row)) as usize)
            % JOIN_SPILL_PARTITIONS;
        if self.map.len() >= self.build_limit {
            self.spill_partition(partition, vec![(key, row)])?;
            return Ok(());
        }
        let idx = self.build_rows.len() as u32;
        self.build_rows.push(BuildEntry { key, row });
        self.map
            .entry(self.build_rows[idx as usize].key.clone())
            .or_default()
            .push(idx);
        Ok(())
    }

    fn key_from_row(&self, cols: &[String], row: &[Value]) -> Option<String> {
        let values: Vec<&Value> = cols
            .iter()
            .map(|name| self.value_at_self(name, row))
            .collect();
        key_string(&values)
    }

    fn value_at_self<'a>(&self, name: &str, row: &'a [Value]) -> &'a Value {
        self.right_schema
            .iter()
            .position(|(n, _)| n == name)
            .and_then(|i| row.get(i))
            .unwrap_or(&Value::Null)
    }

    fn row_key_values<'a>(&self, cols: &'a [String], row: &'a [Value]) -> Vec<&'a Value> {
        cols.iter()
            .map(|name| self.value_at_self(name, row))
            .collect()
    }

    fn spill_partition(
        &mut self,
        partition: usize,
        entries: Vec<(String, Vec<Value>)>,
    ) -> Result<()> {
        if self.spill.is_none() {
            self.spill = Some(SpillState {
                id: JOIN_ID.fetch_add(1, AtomicOrdering::Relaxed),
                written: vec![false; JOIN_SPILL_PARTITIONS],
            });
        }
        let id = self.spill.as_ref().unwrap().id;
        let path =
            std::env::temp_dir().join(format!("tpt-streamforge-join-{id}-p{partition}.tptcol"));
        let mut columns: Vec<Column> = self
            .right_schema
            .iter()
            .map(|(name, dtype)| Column::new(name.clone(), *dtype, entries.len()))
            .collect();
        for (_, row) in &entries {
            for (ci, _) in self.right_schema.iter().enumerate() {
                columns[ci].push(row.get(ci).cloned().unwrap_or(Value::Null));
            }
        }
        let file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)?;
        let mut writer = ChunkedWriter::new(BufWriter::with_capacity(1 << 16, file), false);
        writer.write_batch(&RecordBatch::new(columns))?;
        writer.finish()?;
        self.spill.as_mut().unwrap().written[partition] = true;
        Ok(())
    }

    fn spill_path(&self, partition: usize) -> std::path::PathBuf {
        std::env::temp_dir().join(format!(
            "tpt-streamforge-join-{}-p{partition}.tptcol",
            self.spill.as_ref().expect("spill state").id
        ))
    }

    /// All build rows (from memory and one partition file) matching `key`.
    fn matching_build_rows(&self, key: &str, partition: usize) -> Result<Vec<Vec<Value>>> {
        let mut out: Vec<Vec<Value>> = Vec::new();
        if let Some(ids) = self.map.get(key) {
            for &id in ids {
                out.push(self.build_rows[id as usize].row.clone());
            }
        }
        if let Some(spill) = &self.spill {
            if spill.written[partition] {
                let path = self.spill_path(partition);
                let file = std::fs::File::open(&path)?;
                let mut reader = ChunkedReader::new(file);
                while let Some(batch) = reader.next_batch()? {
                    for ri in 0..batch.num_rows() {
                        let row: Vec<Value> = batch
                            .columns()
                            .iter()
                            .map(|c| c.get(ri).unwrap_or(Value::Null))
                            .collect();
                        if self.key_from_row(&self.right_keys, &row).as_deref() == Some(key) {
                            out.push(row);
                        }
                    }
                }
            }
        }
        Ok(out)
    }

    fn resolve_left_schema(&mut self, batch: &RecordBatch) -> Result<()> {
        if self.left_schema.is_some() {
            return Ok(());
        }
        let left = batch.schema();
        for key in &self.left_keys {
            if !left.iter().any(|(n, _)| n == key) {
                return Err(Error::Schema(format!(
                    "join key column '{key}' not found on left side; available: {}",
                    batch.column_names().join(", ")
                )));
            }
        }
        for key in &self.right_keys {
            if !self.right_schema.iter().any(|(n, _)| n == key) {
                return Err(Error::Schema(format!(
                    "join key column '{key}' not found on build side"
                )));
            }
        }
        let mut output = left.clone();
        for (name, dtype) in &self.right_schema {
            let mut out_name = name.clone();
            if output.iter().any(|(n, _)| n == &out_name) {
                out_name = format!("{name}_r");
            }
            output.push((out_name, *dtype));
        }
        self.output_schema = Some(output);
        self.left_schema = Some(left);
        Ok(())
    }

    fn emit_row(
        columns: &mut [Column],
        left_row: &[Value],
        right_row: Option<&[Value]>,
        left_width: usize,
    ) {
        if left_row.is_empty() {
            for col in columns.iter_mut().take(left_width) {
                col.push(Value::Null);
            }
        } else {
            for (ci, value) in left_row.iter().enumerate() {
                columns[ci].push(value.clone());
            }
        }
        match right_row {
            Some(right) => {
                for (ci, value) in right.iter().enumerate() {
                    columns[left_width + ci].push(value.clone());
                }
            }
            None => {
                for col in columns.iter_mut().skip(left_width) {
                    col.push(Value::Null);
                }
            }
        }
    }

    fn push_row_batch(out: &mut Vec<RecordBatch>, columns: &mut Option<Vec<Column>>) {
        if let Some(cols) = columns.as_ref() {
            if cols[0].len() >= crate::DEFAULT_CHUNK_ROWS {
                out.push(RecordBatch::new(columns.take().expect("inited")));
            }
        }
    }

    fn probe_batch(&mut self, batch: &RecordBatch) -> Result<Vec<RecordBatch>> {
        self.resolve_left_schema(batch)?;
        let left_width = self.left_schema.as_ref().unwrap().len();
        let schema = self.output_schema.clone().expect("output schema");
        let mut out = Vec::new();
        let mut columns: Option<Vec<Column>> = None;

        for ri in 0..batch.num_rows() {
            let left_values: Vec<Value> = self
                .left_keys
                .iter()
                .map(|name| batch.cell(ri, name).unwrap_or(Value::Null))
                .collect();
            let left_refs: Vec<&Value> = left_values.iter().collect();
            let Some(key) = key_string(&left_refs) else {
                if matches!(self.join_type, JoinType::Left | JoinType::Full) {
                    let row: Vec<Value> = batch
                        .columns()
                        .iter()
                        .map(|c| c.get(ri).unwrap_or(Value::Null))
                        .collect();
                    if columns.is_none() {
                        columns = Some(new_columns(&schema));
                    }
                    Self::emit_row(columns.as_mut().unwrap(), &row, None, left_width);
                }
                continue;
            };
            let partition = (hash_key(&left_refs) as usize) % JOIN_SPILL_PARTITIONS;
            let matches = self.matching_build_rows(&key, partition)?;
            if !matches.is_empty() {
                self.matched.insert(key.clone());
            } else if matches!(self.join_type, JoinType::Left | JoinType::Full) {
                let row: Vec<Value> = batch
                    .columns()
                    .iter()
                    .map(|c| c.get(ri).unwrap_or(Value::Null))
                    .collect();
                if columns.is_none() {
                    columns = Some(new_columns(&schema));
                }
                Self::emit_row(columns.as_mut().unwrap(), &row, None, left_width);
                Self::push_row_batch(&mut out, &mut columns);
                continue;
            }
            let probe_row: Vec<Value> = batch
                .columns()
                .iter()
                .map(|c| c.get(ri).unwrap_or(Value::Null))
                .collect();
            for build in &matches {
                if columns.is_none() {
                    columns = Some(new_columns(&schema));
                }
                Self::emit_row(
                    columns.as_mut().unwrap(),
                    &probe_row,
                    Some(build),
                    left_width,
                );
                Self::push_row_batch(&mut out, &mut columns);
            }
        }
        if let Some(cols) = columns {
            if !cols[0].is_empty() {
                out.push(RecordBatch::new(cols));
            }
        }
        Ok(out)
    }

    fn emit_unmatched_build(&mut self) -> Result<Vec<RecordBatch>> {
        let Some(schema) = self.output_schema.clone() else {
            return Ok(Vec::new());
        };
        let left_width = self.left_schema.as_ref().unwrap().len();
        let mut out = Vec::new();
        let mut columns: Option<Vec<Column>> = None;

        // In-memory build rows.
        for entry in &self.build_rows {
            if self.matched.contains(&entry.key) {
                continue;
            }
            if columns.is_none() {
                columns = Some(new_columns(&schema));
            }
            Self::emit_row(columns.as_mut().unwrap(), &[], Some(&entry.row), left_width);
            Self::push_row_batch(&mut out, &mut columns);
        }

        // Build rows kept in spill partitions.
        if let Some(spill) = &self.spill {
            for p in 0..JOIN_SPILL_PARTITIONS {
                if !spill.written[p] {
                    continue;
                }
                let file = std::fs::File::open(self.spill_path(p))?;
                let mut reader = ChunkedReader::new(file);
                while let Some(batch) = reader.next_batch()? {
                    for ri in 0..batch.num_rows() {
                        let row: Vec<Value> = batch
                            .columns()
                            .iter()
                            .map(|c| c.get(ri).unwrap_or(Value::Null))
                            .collect();
                        let unmatched = self
                            .key_from_row(&self.right_keys, &row)
                            .map(|k| !self.matched.contains(&k))
                            .unwrap_or(true);
                        if unmatched {
                            if columns.is_none() {
                                columns = Some(new_columns(&schema));
                            }
                            Self::emit_row(columns.as_mut().unwrap(), &[], Some(&row), left_width);
                            Self::push_row_batch(&mut out, &mut columns);
                        }
                    }
                }
                std::fs::remove_file(self.spill_path(p)).ok();
            }
        }

        if let Some(cols) = columns {
            if !cols[0].is_empty() {
                out.push(RecordBatch::new(cols));
            }
        }
        Ok(out)
    }
}

fn new_columns(schema: &[(String, DataType)]) -> Vec<Column> {
    schema
        .iter()
        .map(|(name, dtype)| Column::new(name.clone(), *dtype, crate::DEFAULT_CHUNK_ROWS))
        .collect()
}

#[async_trait::async_trait]
impl crate::pipeline::PipelineStage for HashJoin {
    fn name(&self) -> &'static str {
        "HashJoin"
    }

    async fn process(&mut self, batch: RecordBatch) -> Result<Vec<RecordBatch>> {
        self.probe_batch(&batch)
    }

    async fn finish(&mut self) -> Result<Vec<RecordBatch>> {
        if matches!(self.join_type, JoinType::Right | JoinType::Full) {
            return self.emit_unmatched_build();
        }
        Ok(Vec::new())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pipeline::PipelineStage;

    fn build_side_rows() -> (Vec<Vec<Value>>, Vec<(String, DataType)>) {
        let mut rows = Vec::new();
        for (id, name) in [(1, "alice"), (2, "bob"), (3, "carol")] {
            rows.push(vec![Value::Int32(id), Value::Utf8(name.into())]);
        }
        let schema = vec![
            ("id".into(), DataType::Int32),
            ("name".into(), DataType::Utf8),
        ];
        (rows, schema)
    }

    fn probe_side() -> RecordBatch {
        let mut id = Column::new("id", DataType::Int32, 4);
        for v in [1, 1, 3, 99] {
            id.push(Value::Int32(v));
        }
        let mut score = Column::new("score", DataType::Float64, 4);
        for v in [10.0, 20.0, 30.0, 40.0] {
            score.push(Value::Float64(v));
        }
        RecordBatch::new(vec![id, score])
    }

    fn make_stage(jt: JoinType) -> HashJoin {
        let (rows, schema) = build_side_rows();
        let mut stage = HashJoin::new(vec!["id".into()], vec!["id".into()], schema, jt);
        for row in rows {
            stage.add_build_row(row).unwrap();
        }
        stage
    }

    #[tokio::test]
    async fn inner_join() {
        let mut stage = make_stage(JoinType::Inner);
        let out = stage.process(probe_side()).await.unwrap();
        let total: usize = out.iter().map(|b| b.num_rows()).sum();
        // id=1 matches twice, id=3 once, id=99 never.
        assert_eq!(total, 3);
    }

    #[tokio::test]
    async fn left_join() {
        let mut stage = make_stage(JoinType::Left);
        let out = stage.process(probe_side()).await.unwrap();
        let total: usize = out.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 4);
        let rows: Vec<Vec<Value>> = out
            .iter()
            .flat_map(|b| {
                (0..b.num_rows()).map(|r| {
                    b.columns()
                        .iter()
                        .map(|c| c.get(r).unwrap_or(Value::Null))
                        .collect::<Vec<_>>()
                })
            })
            .collect();
        assert!(
            rows.iter().any(|r| r[2] == Value::Null),
            "unmatched left row has null name"
        );
    }

    #[tokio::test]
    async fn right_join_emits_unmatched_build() {
        let mut stage = make_stage(JoinType::Right);
        let out = stage.process(probe_side()).await.unwrap();
        let tail = stage.finish().await.unwrap();
        let matched: usize = out.iter().map(|b| b.num_rows()).sum();
        let unmatched: usize = tail.iter().map(|b| b.num_rows()).sum();
        // matched: id 1 (x2) + id 3; unmatched build: id 2.
        assert_eq!(matched, 3);
        assert_eq!(unmatched, 1);
    }

    #[tokio::test]
    async fn build_side_spills() {
        let (rows, schema) = build_side_rows();
        let mut stage = HashJoin::new(
            vec!["id".into()],
            vec!["id".into()],
            schema,
            JoinType::Inner,
        )
        .with_build_limit(1);
        for row in rows {
            stage.add_build_row(row).unwrap();
        }
        assert!(stage.spill.is_some(), "limit=1 must force a spill");
        let out = stage.process(probe_side()).await.unwrap();
        let total: usize = out.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 3, "spilled build rows must still join");
        for p in 0..JOIN_SPILL_PARTITIONS {
            std::fs::remove_file(stage.spill_path(p)).ok();
        }
    }
}
