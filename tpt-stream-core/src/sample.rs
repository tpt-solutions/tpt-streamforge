//! Deterministic keyed sampling (`Sample`).
//!
//! Keeps a row when a stable hash of `(seed, key columns)` falls below
//! `fraction` of the hash space. Because the decision depends only on the key
//! values and the seed, it is:
//!
//! - **reproducible** — the same input and seed always select the same rows;
//! - **consistent per key** — every row sharing a key is kept or dropped
//!   together, so sampling by `customer_id` keeps whole customers;
//! - **stateless** — no memory of earlier rows, safe to narrow on retry.
//!
//! The hash is a fixed FNV-1a + SplitMix64 finaliser (not `DefaultHasher`, whose
//! algorithm is not stable across Rust releases), fed by the same value
//! hashing the aggregate stage uses. Integers hash in native byte order, so
//! samples are stable across runs and releases on one platform.

use std::hash::Hasher;

use crate::error::{Error, Result};
use crate::pipeline::PipelineStage;
use crate::table::RecordBatch;
use crate::value::Value;

/// FNV-1a 64-bit with a SplitMix64 avalanche on `finish`.
struct StableHasher(u64);

impl StableHasher {
    fn new(seed: u64) -> Self {
        let mut h = StableHasher(0xcbf2_9ce4_8422_2325);
        h.write_u64(seed);
        h
    }
}

impl Hasher for StableHasher {
    fn write(&mut self, bytes: &[u8]) {
        for b in bytes {
            self.0 ^= u64::from(*b);
            self.0 = self.0.wrapping_mul(0x0000_0100_0000_01b3);
        }
    }

    fn finish(&self) -> u64 {
        let mut z = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }
}

/// Map `(seed, key)` to a uniform value in `[0, 1)`.
pub fn sample_position(key: &[Value], seed: u64) -> f64 {
    let mut h = StableHasher::new(seed);
    crate::agg::hash_values(key, &mut h);
    // Top 53 bits -> exactly representable in an f64 mantissa.
    (h.finish() >> 11) as f64 / (1u64 << 53) as f64
}

/// Pipeline stage keeping a deterministic, key-consistent `fraction` of rows.
#[derive(Debug)]
pub struct Sample {
    fraction: f64,
    key_columns: Vec<String>,
    seed: u64,
}

impl Sample {
    /// `fraction` must be in `[0, 1]` and `key_columns` non-empty; violations
    /// surface as [`Error::Config`] when the pipeline runs.
    pub fn new(fraction: f64, key_columns: Vec<String>, seed: u64) -> Self {
        Sample {
            fraction,
            key_columns,
            seed,
        }
    }
}

#[async_trait::async_trait]
impl PipelineStage for Sample {
    fn name(&self) -> &'static str {
        "sample"
    }

    async fn process(&mut self, mut batch: RecordBatch) -> Result<Vec<RecordBatch>> {
        if !(0.0..=1.0).contains(&self.fraction) {
            return Err(Error::Config(format!(
                "sample: fraction must be within [0, 1], got {}",
                self.fraction
            )));
        }
        if self.key_columns.is_empty() {
            return Err(Error::Config(
                "sample: at least one key column is required".into(),
            ));
        }
        let mut cols = Vec::with_capacity(self.key_columns.len());
        for name in &self.key_columns {
            cols.push(batch.column(name).ok_or_else(|| {
                Error::Schema(format!(
                    "sample: key column {name:?} not found (have {:?})",
                    batch.column_names()
                ))
            })?);
        }
        let n = batch.num_rows();
        let mut keep = Vec::with_capacity(n);
        let mut key = Vec::with_capacity(cols.len());
        for row in 0..n {
            key.clear();
            key.extend(cols.iter().map(|c| c.get(row).unwrap_or(Value::Null)));
            keep.push(sample_position(&key, self.seed) < self.fraction);
        }
        if n > 0 && keep.iter().all(|k| *k) {
            return Ok(vec![batch]);
        }
        for column in batch.columns_mut() {
            column.retain_rows(&keep);
        }
        batch.recompute_row_count();
        Ok(vec![batch])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Column, DataType};

    fn ids(range: std::ops::Range<i64>) -> RecordBatch {
        let mut c = Column::new("id", DataType::Int64, (range.end - range.start) as usize);
        for v in range {
            c.push(Value::Int64(v));
        }
        RecordBatch::new(vec![c])
    }

    fn rt() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap()
    }

    fn kept(fraction: f64, seed: u64, batch: RecordBatch) -> Vec<i64> {
        let out = rt()
            .block_on(Sample::new(fraction, vec!["id".into()], seed).process(batch))
            .unwrap();
        let col = out[0].column("id").unwrap();
        (0..col.len())
            .map(|i| match col.get(i) {
                Some(Value::Int64(v)) => v,
                other => panic!("{other:?}"),
            })
            .collect()
    }

    #[test]
    fn deterministic_for_same_seed_and_independent_of_batching() {
        let a = kept(0.3, 7, ids(0..20_000));
        let b = kept(0.3, 7, ids(0..20_000));
        assert_eq!(a, b);
        // Splitting the stream into two batches selects the same rows.
        let mut split = kept(0.3, 7, ids(0..9_000));
        split.extend(kept(0.3, 7, ids(9_000..20_000)));
        assert_eq!(a, split);
        // A different seed picks a different sample.
        assert_ne!(a, kept(0.3, 8, ids(0..20_000)));
    }

    #[test]
    fn hash_is_pinned_across_releases() {
        // Guards the "stable across releases" promise: changing the hash
        // changes every user's sample, so it must be a deliberate decision.
        let p = sample_position(&[Value::Int64(1)], 0);
        assert!((0.0..1.0).contains(&p));
        assert_eq!(p, sample_position(&[Value::Int64(1)], 0));
        let ones = kept(0.5, 0, ids(0..8));
        assert_eq!(ones, vec![0i64, 4, 5, 6, 7]);
    }

    #[test]
    fn fraction_is_roughly_correct() {
        for f in [0.01, 0.1, 0.5, 0.9] {
            let n = 200_000usize;
            let got = kept(f, 42, ids(0..n as i64)).len() as f64 / n as f64;
            assert!((got - f).abs() < f * 0.1 + 0.005, "fraction {f}: got {got}");
        }
    }

    #[test]
    fn extremes_and_duplicate_keys() {
        assert!(kept(0.0, 1, ids(0..1000)).is_empty());
        assert_eq!(kept(1.0, 1, ids(0..1000)).len(), 1000);
        // Repeated key values are kept or dropped together.
        let mut c = Column::new("id", DataType::Int64, 2000);
        for v in 0..1000 {
            c.push(Value::Int64(v));
            c.push(Value::Int64(v));
        }
        let out = kept(0.5, 3, RecordBatch::new(vec![c]));
        assert_eq!(out.len() % 2, 0);
        assert!(out.chunks(2).all(|p| p[0] == p[1]));
    }

    #[test]
    fn bad_config_is_reported() {
        let rt = rt();
        for (f, keys) in [(1.5, vec!["id"]), (f64::NAN, vec!["id"]), (0.5, vec![])] {
            let keys = keys.into_iter().map(String::from).collect();
            let err = rt
                .block_on(Sample::new(f, keys, 0).process(ids(0..3)))
                .unwrap_err();
            assert!(matches!(err, Error::Config(_)), "{err}");
        }
        let err = rt
            .block_on(Sample::new(0.5, vec!["nope".into()], 0).process(ids(0..3)))
            .unwrap_err();
        assert!(err.to_string().contains("nope"), "{err}");
    }
}
