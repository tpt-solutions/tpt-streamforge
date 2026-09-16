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

    pub fn append_rows(&mut self, other: &RecordBatch) {
        assert_eq!(
            self.columns.len(),
            other.columns.len(),
            "column count mismatch in append_rows"
        );
        for (i, col) in self.columns.iter_mut().enumerate() {
            let other_col = &other.columns[i];
            assert_eq!(
                col.name(),
                other_col.name(),
                "column names must match in append_rows"
            );
            col.append_column(other_col);
        }
        self.row_count += other.row_count;
    }
}

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
