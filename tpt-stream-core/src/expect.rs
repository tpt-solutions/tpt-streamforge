//! Data-quality assertions as a pipeline stage (`Expect`).
//!
//! Attach checks with [`crate::Pipeline::expect_checks`]; a violation aborts
//! `execute()` with [`crate::Error::DataQuality`]. Checks:
//!
//! - [`Check::RowsAtLeast`] / [`Check::RowsAtMost`] — total row bounds,
//!   enforced when the stream ends
//! - [`Check::NoNulls`] — a column must not contain nulls (checked per batch)
//! - [`Check::Unique`] — a column's values must be unique across the stream

use std::collections::HashSet;

use crate::error::{Error, Result};
use crate::pipeline::PipelineStage;
use crate::table::RecordBatch;
use crate::value::Value;

/// One data-quality assertion.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Check {
    /// The pipeline must produce at least this many rows.
    RowsAtLeast(u64),
    /// The pipeline must produce at most this many rows.
    RowsAtMost(u64),
    /// The named column must not contain nulls in any row.
    NoNulls(String),
    /// The named column's non-null values must be unique across the stream.
    Unique(String),
}

impl Check {
    pub fn describe(&self) -> String {
        match self {
            Check::RowsAtLeast(n) => format!("rows >= {n}"),
            Check::RowsAtMost(n) => format!("rows <= {n}"),
            Check::NoNulls(col) => format!("no nulls in {col:?}"),
            Check::Unique(col) => format!("unique {col:?}"),
        }
    }
}

/// Pipeline stage asserting the attached checks. Passes rows through
/// unchanged; fails the pipeline via [`crate::Error::DataQuality`] on the
/// first violation.
pub struct Expect {
    checks: Vec<Check>,
    total_rows: u64,
    seen_keys: HashSet<String>,
}

impl Expect {
    pub fn new(checks: Vec<Check>) -> Self {
        Expect {
            checks,
            total_rows: 0,
            seen_keys: HashSet::new(),
        }
    }

    fn column<'a>(
        &self,
        batch: &'a RecordBatch,
        name: &str,
        check: &Check,
    ) -> Result<&'a crate::column::Column> {
        batch.column(name).ok_or_else(|| {
            Error::DataQuality(format!(
                "check {:?}: column {name:?} not found (have {:?})",
                check.describe(),
                batch.column_names()
            ))
        })
    }
}

impl std::fmt::Debug for Expect {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Expect")
            .field("checks", &self.checks)
            .finish()
    }
}

#[async_trait::async_trait]
impl PipelineStage for Expect {
    fn name(&self) -> &'static str {
        "expect"
    }

    async fn process(&mut self, batch: RecordBatch) -> Result<Vec<RecordBatch>> {
        for check in &self.checks {
            match check {
                Check::NoNulls(col_name) => {
                    let column = self.column(&batch, col_name, check)?;
                    for row in 0..column.len() {
                        if matches!(column.get(row), Some(Value::Null) | None) {
                            return Err(Error::DataQuality(format!(
                                "check '{}': null at row {} of batch",
                                check.describe(),
                                self.total_rows + row as u64,
                            )));
                        }
                    }
                }
                Check::Unique(col_name) => {
                    let column = self.column(&batch, col_name, check)?;
                    for row in 0..column.len() {
                        let value = column.get(row).unwrap_or(Value::Null);
                        if value == Value::Null {
                            continue;
                        }
                        let key = format!("{value:?}");
                        if !self.seen_keys.insert(key) {
                            return Err(Error::DataQuality(format!(
                                "check '{}': duplicate value {value:?} near row {} of stream",
                                check.describe(),
                                self.total_rows + row as u64,
                            )));
                        }
                    }
                }
                Check::RowsAtLeast(_) | Check::RowsAtMost(_) => {
                    // Enforced once the stream ends (finish).
                }
            }
        }
        self.total_rows += batch.num_rows() as u64;
        Ok(vec![batch])
    }

    async fn finish(&mut self) -> Result<Vec<RecordBatch>> {
        for check in &self.checks {
            match check {
                Check::RowsAtLeast(n) if self.total_rows < *n => {
                    return Err(Error::DataQuality(format!(
                        "check '{}': produced {} row(s)",
                        check.describe(),
                        self.total_rows
                    )));
                }
                Check::RowsAtMost(n) if self.total_rows > *n => {
                    return Err(Error::DataQuality(format!(
                        "check '{}': produced {} row(s)",
                        check.describe(),
                        self.total_rows
                    )));
                }
                _ => {}
            }
        }
        Ok(Vec::new())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn batch(values: &[i32]) -> RecordBatch {
        let mut col = crate::Column::new("v", crate::DataType::Int32, values.len());
        for v in values {
            col.push(Value::Int32(*v));
        }
        RecordBatch::new(vec![col])
    }

    fn runtime() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
    }

    #[test]
    fn no_nulls_violation() {
        let rt = runtime();
        rt.block_on(async {
            let mut col = crate::Column::new("v", crate::DataType::Int32, 2);
            col.push(Value::Int32(1));
            col.push(Value::Null);
            let batch = RecordBatch::new(vec![col]);
            let mut expect = Expect::new(vec![Check::NoNulls("v".into())]);
            let err = expect.process(batch).await.unwrap_err();
            assert!(err.to_string().contains("data quality"), "{err}");
        });
    }

    #[test]
    fn unique_violation_across_batches() {
        let rt = runtime();
        rt.block_on(async {
            let mut expect = Expect::new(vec![Check::Unique("v".into())]);
            expect.process(batch(&[1, 2])).await.unwrap();
            let err = expect.process(batch(&[2, 3])).await.unwrap_err();
            assert!(err.to_string().contains("duplicate"), "{err}");
        });
    }

    #[test]
    fn row_bounds_enforced_at_finish() {
        let rt = runtime();
        rt.block_on(async {
            let mut expect = Expect::new(vec![Check::RowsAtLeast(3)]);
            expect.process(batch(&[1, 2])).await.unwrap();
            let err = expect.finish().await.unwrap_err();
            assert!(err.to_string().contains("rows >="), "{err}");
        });
    }

    #[test]
    fn missing_column_names_the_check() {
        let rt = runtime();
        rt.block_on(async {
            let mut expect = Expect::new(vec![Check::NoNulls("nope".into())]);
            let err = expect.process(batch(&[1])).await.unwrap_err();
            assert!(err.to_string().contains("nope"), "{err}");
        });
    }

    #[test]
    fn rows_pass_through_untouched() {
        let rt = runtime();
        rt.block_on(async {
            let mut expect = Expect::new(vec![Check::RowsAtMost(10)]);
            let out = expect.process(batch(&[1, 2, 3])).await.unwrap();
            assert_eq!(out.len(), 1);
            assert_eq!(out[0].num_rows(), 3);
        });
    }
}
