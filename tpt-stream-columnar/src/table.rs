use crate::column::Column;
use crate::value::{DataType, Value};
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct RecordBatch {
    columns: Vec<Column>,
    row_count: usize,
    index: HashMap<String, usize>,
}

impl RecordBatch {
    pub fn new(columns: Vec<Column>) -> Self {
        let mut index = HashMap::with_capacity(columns.len());
        for (i, col) in columns.iter().enumerate() {
            index.insert(col.name().to_string(), i);
        }

        let row_count = columns.first().map_or(0, |c| c.len());
        #[cfg(debug_assertions)]
        for col in columns.iter().skip(1) {
            assert_eq!(
                col.len(),
                row_count,
                "all columns in a RecordBatch must have equal length"
            );
        }

        RecordBatch {
            columns,
            row_count,
            index,
        }
    }

    pub fn empty() -> Self {
        RecordBatch::new(Vec::new())
    }

    pub fn num_columns(&self) -> usize {
        self.columns.len()
    }

    pub fn num_rows(&self) -> usize {
        self.row_count
    }

    pub fn is_empty(&self) -> bool {
        self.row_count == 0
    }

    pub fn column(&self, name: &str) -> Option<&Column> {
        self.index.get(name).map(|&i| &self.columns[i])
    }

    pub fn column_mut(&mut self, name: &str) -> Option<&mut Column> {
        self.index
            .get(name)
            .copied()
            .map(move |i| &mut self.columns[i])
    }

    pub fn column_by_index(&self, i: usize) -> Option<&Column> {
        self.columns.get(i)
    }

    pub fn columns(&self) -> &[Column] {
        &self.columns
    }

    pub fn columns_mut(&mut self) -> &mut [Column] {
        &mut self.columns
    }

    /// Keep only the first `n` rows of every column.
    pub fn truncate(&mut self, n: usize) {
        for col in &mut self.columns {
            col.truncate(n);
        }
        self.row_count = n.min(self.row_count);
    }

    /// Recompute `row_count` from the first column's length. Call after in-place
    /// mutations (e.g. `Column::retain_rows`) that change buffer lengths.
    pub fn recompute_row_count(&mut self) {
        self.row_count = self.columns.first().map_or(0, |c| c.len());
    }

    pub fn schema(&self) -> Vec<(String, DataType)> {
        self.columns
            .iter()
            .map(|c| (c.name().to_string(), c.data_type()))
            .collect()
    }

    pub fn column_names(&self) -> Vec<&str> {
        self.columns.iter().map(|c| c.name()).collect()
    }

    pub fn cell(&self, row: usize, col_name: &str) -> Option<Value> {
        let col = self.column(col_name)?;
        col.get(row)
    }

    /// Fallible [`Self::append_rows`]. Use when the batches come from an
    /// untrusted source (a `.tptcol` file) so a schema mismatch is a normal
    /// error rather than a panic.
    pub fn try_append_rows(&mut self, other: &RecordBatch) -> Result<(), AppendError> {
        if self.columns.len() != other.columns.len() {
            return Err(AppendError::ColumnCount {
                left: self.columns.len(),
                right: other.columns.len(),
            });
        }
        for (i, col) in self.columns.iter_mut().enumerate() {
            let other_col = &other.columns[i];
            if col.name() != other_col.name() {
                return Err(AppendError::ColumnName {
                    index: i,
                    left: col.name().to_string(),
                    right: other_col.name().to_string(),
                });
            }
            col.try_append_column(other_col)?;
        }
        self.row_count += other.row_count;
        Ok(())
    }

    /// Panics if the schemas differ; use [`Self::try_append_rows`] for
    /// untrusted batches.
    pub fn append_rows(&mut self, other: &RecordBatch) {
        if let Err(e) = self.try_append_rows(other) {
            panic!("{e}");
        }
    }
}

/// Why two record batches could not be concatenated.
#[derive(Debug, Clone, PartialEq)]
pub enum AppendError {
    /// Different number of columns.
    ColumnCount { left: usize, right: usize },
    /// Column names differ at this index.
    ColumnName {
        index: usize,
        left: String,
        right: String,
    },
    /// A column's data type differs.
    Type(crate::column::TypeMismatch),
}

impl From<crate::column::TypeMismatch> for AppendError {
    fn from(e: crate::column::TypeMismatch) -> Self {
        AppendError::Type(e)
    }
}

impl std::fmt::Display for AppendError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AppendError::ColumnCount { left, right } => {
                write!(f, "column count mismatch in append_rows: {left} vs {right}")
            }
            AppendError::ColumnName { index, left, right } => write!(
                f,
                "column names must match in append_rows: column {index} is {left:?} vs {right:?}"
            ),
            AppendError::Type(e) => write!(f, "{e}"),
        }
    }
}

impl std::error::Error for AppendError {}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_batch() -> RecordBatch {
        let mut id = Column::new("id", DataType::Int32, 3);
        id.push(Value::Int32(1));
        id.push(Value::Int32(2));
        id.push(Value::Int32(3));
        let mut name = Column::new("name", DataType::Utf8, 3);
        name.push(Value::Utf8("a".into()));
        name.push(Value::Utf8("b".into()));
        name.push(Value::Utf8("c".into()));
        RecordBatch::new(vec![id, name])
    }

    #[test]
    fn batch_metadata() {
        let b = sample_batch();
        assert_eq!(b.num_columns(), 2);
        assert_eq!(b.num_rows(), 3);
        assert_eq!(b.column_names(), vec!["id", "name"]);
        assert_eq!(b.schema()[0], ("id".to_string(), DataType::Int32));
    }

    #[test]
    fn batch_cell_lookup() {
        let b = sample_batch();
        assert_eq!(b.cell(1, "name"), Some(Value::Utf8("b".into())));
        assert_eq!(b.cell(0, "missing"), None);
    }

    #[test]
    fn batch_append_rows() {
        let mut a = sample_batch();
        let b = sample_batch();
        a.append_rows(&b);
        assert_eq!(a.num_rows(), 6);
        assert_eq!(a.cell(4, "id"), Some(Value::Int32(2)));
    }
}
