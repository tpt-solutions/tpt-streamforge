use crate::column::Column;
use crate::pipeline::Result;
use crate::table::RecordBatch;
use crate::value::Value;
use std::collections::HashMap;

/// Approximate dedup: a space-efficient bloom filter decides "definitely new"
/// vs "possibly seen". Rows that are "possibly seen" are checked exactly
/// against hashed keys. Gives O(bits_per_row) extra space, falling back to
/// exact per-key storage as needed.
pub struct Deduplicate {
    columns: Vec<String>,
    bits: Vec<u64>,
    bit_count: u64,
    seen: HashMap<String, ()>,
    /// When true, all rows are passed through (identity pipeline segment).
    disabled: bool,
}

const MURMUR_SEED: u64 = 0x9E3779B97F4A7C15;
const HASH_ROUNDS: u64 = 2;

impl Deduplicate {
    /// `columns`: the subset of columns that defines row identity. An empty
    /// slice means the entire row.
    pub fn new(columns: Vec<String>) -> Self {
        Self::with_capacity(columns, 1 << 20)
    }

    pub fn with_capacity(columns: Vec<String>, rows: usize) -> Self {
        let bits_per_row = 8u64;
        let mut bits = (rows as u64).saturating_mul(bits_per_row).max(64);
        bits = bits.next_power_of_two();
        let words = (bits / 64).max(1) as usize;
        Deduplicate {
            columns,
            bits: vec![0; words],
            bit_count: (words * 64) as u64,
            seen: HashMap::new(),
            disabled: false,
        }
    }

    /// Pass rows through unmodified (e.g. when a consumer only needs writing).
    pub fn disabled(mut self) -> Self {
        self.disabled = true;
        self
    }

    pub fn is_disabled(&self) -> bool {
        self.disabled
    }

    fn column_indexes(&self, batch: &RecordBatch) -> Vec<usize> {
        if self.columns.is_empty() {
            return (0..batch.num_columns()).collect();
        }
        self.columns
            .iter()
            .filter_map(|name| batch.column_names().iter().position(|n| n == name))
            .collect()
    }

    fn hash_bytes(bytes: &[u8], index: u64) -> u64 {
        // MurmurHash64A finalizer mixed twice; deterministic across platforms.
        let mut h = MURMUR_SEED ^ (bytes.len() as u64).wrapping_mul(0xC2B2AE3D27D4EB4F);
        for (i, b) in bytes.iter().enumerate() {
            let pos = index.wrapping_add(i as u64);
            h = h
                .wrapping_mul(0x5BD1E995)
                .wrapping_add((*b as u64).wrapping_add(pos));
        }
        h ^= h >> 33;
        h.wrapping_mul(0xFF51AFD7ED558CCD) ^ (h >> 29)
    }

    fn bloom_insert(&mut self, bytes: &[u8]) {
        for i in 0..HASH_ROUNDS {
            let bit = Self::hash_bytes(bytes, i) % self.bit_count;
            self.bits[(bit / 64) as usize] |= 1u64 << (bit % 64);
        }
    }

    fn bloom_maybe_seen(&self, bytes: &[u8]) -> bool {
        for i in 0..HASH_ROUNDS {
            let bit = Self::hash_bytes(bytes, i) % self.bit_count;
            if self.bits[(bit / 64) as usize] & (1u64 << (bit % 64)) == 0 {
                return false;
            }
        }
        true
    }

    fn row_key(batch: &RecordBatch, cols: &[usize], row: usize) -> String {
        let mut out = String::new();
        for &ci in cols {
            let value = batch.column_by_index(ci).and_then(|c| c.get(row));
            match value {
                Some(Value::Int32(x)) => {
                    out.push('i');
                    for b in x.to_le_bytes() {
                        out.push(b as char);
                    }
                }
                Some(Value::Int64(x)) => {
                    out.push('j');
                    for b in x.to_le_bytes() {
                        out.push(b as char);
                    }
                }
                Some(Value::Float32(x)) => {
                    out.push('f');
                    for b in x.to_le_bytes() {
                        out.push(b as char);
                    }
                }
                Some(Value::Float64(x)) => {
                    out.push('g');
                    for b in x.to_le_bytes() {
                        out.push(b as char);
                    }
                }
                Some(Value::Bool(x)) => out.push(if x { 't' } else { 'f' }),
                Some(Value::Utf8(s)) => {
                    out.push('s');
                    out.push_str(&s);
                }
                Some(Value::Null) => out.push('n'),
                None => out.push('n'),
            }
        }
        out
    }

    fn process_batch(&mut self, batch: RecordBatch) -> Result<Vec<RecordBatch>> {
        if self.disabled {
            return Ok(vec![batch]);
        }
        let cols = self.column_indexes(&batch);
        let num_cols = batch.num_columns();
        let mut keep: Vec<bool> = Vec::with_capacity(batch.num_rows());
        for row in 0..batch.num_rows() {
            let key = Self::row_key(&batch, &cols, row);
            let likely_new = !self.bloom_maybe_seen(key.as_bytes());
            if likely_new {
                self.bloom_insert(key.as_bytes());
                self.seen.insert(key, ());
                keep.push(true);
            } else {
                use std::collections::hash_map::Entry;
                match self.seen.entry(key) {
                    // Bloom said "maybe seen"; exact check rejects false positives.
                    Entry::Vacant(entry) => {
                        entry.insert(());
                        keep.push(true);
                    }
                    Entry::Occupied(_) => keep.push(false),
                }
            }
        }
        if keep.iter().all(|&k| k) {
            return Ok(vec![batch]);
        }
        let mut columns: Vec<Column> = Vec::with_capacity(num_cols);
        for ci in 0..num_cols {
            let src = batch.column_by_index(ci).expect("column");
            let mut col = Column::new(src.name().to_string(), src.data_type(), keep.len());
            for (row, &on) in keep.iter().enumerate() {
                if on {
                    col.push(src.get(row).unwrap_or(Value::Null));
                }
            }
            columns.push(col);
        }
        Ok(vec![RecordBatch::new(columns)])
    }
}

#[async_trait::async_trait]
impl crate::pipeline::PipelineStage for Deduplicate {
    fn name(&self) -> &'static str {
        "Deduplicate"
    }

    async fn process(&mut self, batch: RecordBatch) -> Result<Vec<RecordBatch>> {
        self.process_batch(batch)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::column::Column;
    use crate::pipeline::PipelineStage;
    use crate::value::{DataType, Value};

    fn batch_with(vals: &[i32]) -> RecordBatch {
        let mut id = Column::new("id", DataType::Int32, vals.len());
        for v in vals {
            id.push(Value::Int32(*v));
        }
        RecordBatch::new(vec![id])
    }

    #[tokio::test]
    async fn dedup_identity_on_unique_rows() {
        let mut d = Deduplicate::new(vec!["id".into()]);
        let b = batch_with(&[1, 2, 3]);
        let out = d.process(b).await.unwrap();
        assert_eq!(out[0].num_rows(), 3);
    }

    #[tokio::test]
    async fn dedup_removes_duplicates_across_batches() {
        let mut d = Deduplicate::new(vec!["id".into()]);
        let o1 = d.process(batch_with(&[1, 2, 3])).await.unwrap();
        let o2 = d.process(batch_with(&[3, 4])).await.unwrap();
        let total: usize = o1.iter().chain(o2.iter()).map(|b| b.num_rows()).sum();
        assert_eq!(total, 4);
    }
}
