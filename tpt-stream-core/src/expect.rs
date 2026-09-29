//! Data-quality assertions as a pipeline stage (`Expect`).
//!
//! Attach checks with [`crate::Pipeline::expect_checks`]; a violation aborts
//! `execute()` with [`crate::Error::DataQuality`]. Checks:
//!
//! - [`Check::RowsAtLeast`] / [`Check::RowsAtMost`] — total row bounds,
//!   enforced when the stream ends
//! - [`Check::NoNulls`] — a column must not contain nulls (checked per batch)
//! - [`Check::Unique`] — a column's values must be unique across the stream
//! - [`Check::Range`] — numeric values must lie within `[min, max]` (either
//!   bound optional; nulls are skipped, NaN fails)
//! - [`Check::OneOf`] — values must belong to an allowed set (nulls skipped)
//! - [`Check::Type`] — the column must have the given [`DataType`]
//!
//! Use `NoNulls` alongside `Range` / `OneOf` when nulls are also forbidden.

use std::collections::HashSet;

use crate::error::{Error, Result};
use crate::pipeline::PipelineStage;
use crate::table::RecordBatch;
use crate::value::{DataType, Value};

/// One data-quality assertion.
#[derive(Debug, Clone, PartialEq)]
pub enum Check {
    /// The pipeline must produce at least this many rows.
    RowsAtLeast(u64),
    /// The pipeline must produce at most this many rows.
    RowsAtMost(u64),
    /// The named column must not contain nulls in any row.
    NoNulls(String),
    /// The named column's non-null values must be unique across the stream.
    Unique(String),
    /// Every non-null value of the column must be numeric (integer, float,
    /// date or timestamp) and lie within the inclusive `[min, max]` range.
    /// `None` leaves that side open. NaN and non-numeric values violate it.
    Range {
        column: String,
        min: Option<f64>,
        max: Option<f64>,
    },
    /// Every non-null value of the column must equal one of `allowed`.
    /// Integer values compare numerically across `Int32`/`Int64`.
    OneOf { column: String, allowed: Vec<Value> },
    /// The column must have exactly this data type (checked per batch).
    Type { column: String, expected: DataType },
}

impl Check {
    /// `column` must lie within `[min, max]` (either bound optional).
    pub fn range(column: impl Into<String>, min: Option<f64>, max: Option<f64>) -> Self {
        Check::Range {
            column: column.into(),
            min,
            max,
        }
    }

    /// `column` must only contain values from `allowed` (or nulls).
    pub fn one_of(column: impl Into<String>, allowed: Vec<Value>) -> Self {
        Check::OneOf {
            column: column.into(),
            allowed,
        }
    }

    /// `column` must have data type `expected`.
    pub fn of_type(column: impl Into<String>, expected: DataType) -> Self {
        Check::Type {
            column: column.into(),
            expected,
        }
    }

    pub fn describe(&self) -> String {
        match self {
            Check::RowsAtLeast(n) => format!("rows >= {n}"),
            Check::RowsAtMost(n) => format!("rows <= {n}"),
            Check::NoNulls(col) => format!("no nulls in {col:?}"),
            Check::Unique(col) => format!("unique {col:?}"),
            Check::Range { column, min, max } => match (min, max) {
                (Some(lo), Some(hi)) => format!("{column:?} in [{lo}, {hi}]"),
                (Some(lo), None) => format!("{column:?} >= {lo}"),
                (None, Some(hi)) => format!("{column:?} <= {hi}"),
                (None, None) => format!("{column:?} is numeric"),
            },
            Check::OneOf { column, allowed } => {
                let set: Vec<String> = allowed.iter().map(|v| format!("{v:?}")).collect();
                format!("{column:?} in {{{}}}", set.join(", "))
            }
            Check::Type { column, expected } => format!("{column:?} has type {expected:?}"),
        }
    }
}

fn numeric(value: &Value) -> Option<f64> {
    match value {
        Value::Int32(x) => Some(f64::from(*x)),
        Value::Int64(x) | Value::Timestamp(x) => Some(*x as f64),
        Value::Float32(x) => Some(f64::from(*x)),
        Value::Float64(x) => Some(*x),
        Value::Date(x) => Some(f64::from(*x)),
        _ => None,
    }
}

fn integer(value: &Value) -> Option<i64> {
    match value {
        Value::Int32(x) => Some(i64::from(*x)),
        Value::Int64(x) => Some(*x),
        _ => None,
    }
}

fn allowed_contains(allowed: &[Value], value: &Value) -> bool {
    allowed.iter().any(|a| match (integer(a), integer(value)) {
        (Some(x), Some(y)) => x == y,
        _ => a == value,
    })
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
                Check::Range { column, min, max } => {
                    let col = self.column(&batch, column, check)?;
                    for row in 0..col.len() {
                        let value = col.get(row).unwrap_or(Value::Null);
                        if value == Value::Null {
                            continue;
                        }
                        let ok = numeric(&value).is_some_and(|x| {
                            !x.is_nan()
                                && min.is_none_or(|lo| x >= lo)
                                && max.is_none_or(|hi| x <= hi)
                        });
                        if !ok {
                            return Err(Error::DataQuality(format!(
                                "check '{}': value {value:?} out of range near row {} of stream",
                                check.describe(),
                                self.total_rows + row as u64,
                            )));
                        }
                    }
                }
                Check::OneOf { column, allowed } => {
                    let col = self.column(&batch, column, check)?;
                    for row in 0..col.len() {
                        let value = col.get(row).unwrap_or(Value::Null);
                        if value != Value::Null && !allowed_contains(allowed, &value) {
                            return Err(Error::DataQuality(format!(
                                "check '{}': value {value:?} not allowed near row {} of stream",
                                check.describe(),
                                self.total_rows + row as u64,
                            )));
                        }
                    }
                }
                Check::Type { column, expected } => {
                    let col = self.column(&batch, column, check)?;
                    if col.data_type() != *expected {
                        return Err(Error::DataQuality(format!(
                            "check '{}': column has type {:?}",
                            check.describe(),
                            col.data_type(),
                        )));
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

    fn run(check: Check, values: &[i32]) -> Result<Vec<RecordBatch>> {
        runtime().block_on(async { Expect::new(vec![check]).process(batch(values)).await })
    }

    #[test]
    fn range_bounds_inclusive_and_open_sided() {
        assert!(run(Check::range("v", Some(1.0), Some(3.0)), &[1, 2, 3]).is_ok());
        let err = run(Check::range("v", Some(1.0), Some(3.0)), &[1, 4]).unwrap_err();
        assert!(err.to_string().contains("out of range"), "{err}");
        assert!(run(Check::range("v", Some(0.0), None), &[5, 500]).is_ok());
        assert!(run(Check::range("v", None, Some(0.0)), &[5]).is_err());
    }

    #[test]
    fn range_skips_nulls_and_rejects_strings_and_nan() {
        let rt = runtime();
        rt.block_on(async {
            let mut col = crate::Column::new("v", crate::DataType::Int32, 2);
            col.push(Value::Int32(1));
            col.push(Value::Null);
            let mut e = Expect::new(vec![Check::range("v", Some(0.0), Some(2.0))]);
            e.process(RecordBatch::new(vec![col])).await.unwrap();

            let mut f = crate::Column::new("v", crate::DataType::Float64, 1);
            f.push(Value::Float64(f64::NAN));
            let mut e = Expect::new(vec![Check::range("v", Some(0.0), None)]);
            assert!(e.process(RecordBatch::new(vec![f])).await.is_err());

            let mut s = crate::Column::new("v", crate::DataType::Utf8, 1);
            s.push(Value::Utf8("x".into()));
            let mut e = Expect::new(vec![Check::range("v", None, None)]);
            assert!(e.process(RecordBatch::new(vec![s])).await.is_err());
        });
    }

    #[test]
    fn one_of_matches_across_integer_widths() {
        let allowed = vec![Value::Int64(1), Value::Int64(2)];
        assert!(run(Check::one_of("v", allowed.clone()), &[1, 2, 2]).is_ok());
        let err = run(Check::one_of("v", allowed), &[1, 9]).unwrap_err();
        assert!(err.to_string().contains("not allowed"), "{err}");
    }

    #[test]
    fn one_of_strings() {
        let rt = runtime();
        rt.block_on(async {
            let mut col = crate::Column::new("s", crate::DataType::Utf8, 2);
            col.push(Value::Utf8("a".into()));
            col.push(Value::Utf8("z".into()));
            let mut e = Expect::new(vec![Check::one_of(
                "s",
                vec![Value::Utf8("a".into()), Value::Utf8("b".into())],
            )]);
            assert!(e.process(RecordBatch::new(vec![col])).await.is_err());
        });
    }

    #[test]
    fn type_check() {
        assert!(run(Check::of_type("v", DataType::Int32), &[1]).is_ok());
        let err = run(Check::of_type("v", DataType::Utf8), &[1]).unwrap_err();
        assert!(err.to_string().contains("has type"), "{err}");
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
