use crate::column::Column;
use crate::pipeline::{Error, PipelineStage, Result};
use crate::row::Row;
use crate::table::RecordBatch;
use crate::value::{DataType, Value};

// ---------------------------------------------------------------------------
// Filter
// ---------------------------------------------------------------------------

pub struct Filter {
    predicate: Box<dyn FnMut(Row) -> bool + Send>,
}

impl Filter {
    pub fn new<F>(predicate: F) -> Self
    where
        F: FnMut(Row) -> bool + Send + 'static,
    {
        Filter {
            predicate: Box::new(predicate),
        }
    }
}

impl std::fmt::Debug for Filter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Filter").finish()
    }
}

#[async_trait::async_trait]
impl PipelineStage for Filter {
    fn name(&self) -> &'static str {
        "filter"
    }

    async fn process(&mut self, mut batch: RecordBatch) -> Result<Vec<RecordBatch>> {
        let num_rows = batch.num_rows();
        let mut keep = Vec::with_capacity(num_rows);
        let mut any_dropped = false;
        for i in 0..num_rows {
            let row = Row::new(&batch, i);
            let keep_row = (self.predicate)(row);
            keep.push(keep_row);
            if !keep_row {
                any_dropped = true;
            }
        }

        if num_rows > 0 && !any_dropped {
            return Ok(vec![batch]);
        }

        for column in batch.columns_mut() {
            column.retain_rows(&keep);
        }
        batch.recompute_row_count();

        Ok(vec![batch])
    }
}

// ---------------------------------------------------------------------------
// Map
// ---------------------------------------------------------------------------

pub struct Map {
    output_columns: Vec<String>,
    mapper: Box<dyn FnMut(Row) -> Vec<Value> + Send>,
    schema: Option<Vec<DataType>>,
}

impl Map {
    pub fn new<F>(output_columns: Vec<String>, mapper: F) -> Self
    where
        F: FnMut(Row) -> Vec<Value> + Send + 'static,
    {
        Map {
            output_columns,
            mapper: Box::new(mapper),
            schema: None,
        }
    }
}

impl std::fmt::Debug for Map {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Map")
            .field("output_columns", &self.output_columns)
            .finish()
    }
}

#[async_trait::async_trait]
impl PipelineStage for Map {
    fn name(&self) -> &'static str {
        "map"
    }

    async fn process(&mut self, batch: RecordBatch) -> Result<Vec<RecordBatch>> {
        let num_rows = batch.num_rows();
        let expected = self.output_columns.len();
        let mut cols: Vec<Column> = Vec::new();

        for i in 0..num_rows {
            let row = Row::new(&batch, i);
            let values = (self.mapper)(row);
            if values.len() != expected {
                return Err(Error::Schema(format!(
                    "map produced {} values but {} columns were declared",
                    values.len(),
                    expected
                )));
            }

            if self.schema.is_none() {
                let inferred: Vec<DataType> = values
                    .iter()
                    .map(|v| {
                        if matches!(v, Value::Null) {
                            DataType::Utf8
                        } else {
                            v.data_type()
                        }
                    })
                    .collect();
                self.schema = Some(inferred);
            }

            if cols.is_empty() {
                cols = self
                    .output_columns
                    .iter()
                    .zip(self.schema.as_ref().expect("set above").iter())
                    .map(|(name, dt)| Column::new(name.clone(), *dt, num_rows))
                    .collect();
            }

            for (col, value) in cols.iter_mut().zip(values) {
                col.push(value);
            }
        }

        Ok(vec![RecordBatch::new(cols)])
    }
}

// ---------------------------------------------------------------------------
// Select (projection)
// ---------------------------------------------------------------------------

pub struct Select {
    columns: Vec<String>,
    drop_mode: bool,
}

impl Select {
    pub fn keep(columns: Vec<String>) -> Self {
        Select {
            columns,
            drop_mode: false,
        }
    }

    pub fn drop(columns: Vec<String>) -> Self {
        Select {
            columns,
            drop_mode: true,
        }
    }
}

impl std::fmt::Debug for Select {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Select")
            .field("columns", &self.columns)
            .field("drop_mode", &self.drop_mode)
            .finish()
    }
}

#[async_trait::async_trait]
impl PipelineStage for Select {
    fn name(&self) -> &'static str {
        "select"
    }

    async fn process(&mut self, batch: RecordBatch) -> Result<Vec<RecordBatch>> {
        let names = batch.column_names();
        let selected: Vec<String> = if self.drop_mode {
            names
                .iter()
                .filter(|n| !self.columns.iter().any(|c| c == *n))
                .map(|s| s.to_string())
                .collect()
        } else {
            for col in &self.columns {
                if !batch.column(col).is_some() {
                    return Err(Error::Schema(format!(
                        "select: column {:?} not found in batch",
                        col
                    )));
                }
            }
            self.columns.clone()
        };

        let mut columns = Vec::with_capacity(selected.len());
        for name in &selected {
            if let Some(col) = batch.column(name) {
                columns.push(col.clone());
            }
        }
        Ok(vec![RecordBatch::new(columns)])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_batch() -> RecordBatch {
        let mut id = Column::new("id", DataType::Int32, 4);
        id.push(Value::Int32(1));
        id.push(Value::Int32(2));
        id.push(Value::Int32(3));
        id.push(Value::Int32(4));
        let mut name = Column::new("name", DataType::Utf8, 4);
        name.push(Value::Utf8("a".into()));
        name.push(Value::Utf8("b".into()));
        name.push(Value::Null);
        name.push(Value::Utf8("d".into()));
        RecordBatch::new(vec![id, name])
    }

    #[tokio::test]
    async fn filter_keeps_matching_rows() {
        let batch = sample_batch();
        let mut filter = Filter::new(|row| {
            row.get("id") == Some(Value::Int32(2)) || row.get("id") == Some(Value::Int32(3))
        });
        let out = filter.process(batch).await.unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].num_rows(), 2);
        assert_eq!(out[0].cell(0, "id"), Some(Value::Int32(2)));
        assert_eq!(out[0].cell(1, "id"), Some(Value::Int32(3)));
        assert_eq!(out[0].cell(1, "name"), Some(Value::Null));
    }

    #[tokio::test]
    async fn filter_passthrough_when_nothing_dropped() {
        let batch = sample_batch();
        let mut filter = Filter::new(|_row| true);
        let out = filter.process(batch).await.unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].num_rows(), 4);
    }

    #[tokio::test]
    async fn map_projects_new_columns() {
        let batch = sample_batch();
        let mut map = Map::new(vec!["id".to_string(), "id2".to_string()], |row| {
            let id = match row.get("id") {
                Some(Value::Int32(v)) => Value::Int64(v as i64),
                _ => Value::Null,
            };
            let id2 = match row.get("id") {
                Some(Value::Int32(v)) => Value::Utf8(format!("x{v}")),
                _ => Value::Null,
            };
            vec![id, id2]
        });
        let out = map.process(batch).await.unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].num_rows(), 4);
        assert_eq!(out[0].cell(2, "id"), Some(Value::Int64(3)));
        assert_eq!(out[0].cell(2, "id2"), Some(Value::Utf8("x3".into())));
        assert_eq!(out[0].schema()[0], ("id".to_string(), DataType::Int64));
    }

    #[tokio::test]
    async fn select_keeps_columns() {
        let batch = sample_batch();
        let mut select = Select::keep(vec!["name".to_string()]);
        let out = select.process(batch).await.unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].num_columns(), 1);
        assert_eq!(out[0].num_rows(), 4);
        assert_eq!(out[0].cell(2, "name"), Some(Value::Null));
    }

    #[tokio::test]
    async fn select_drops_columns() {
        let batch = sample_batch();
        let mut select = Select::drop(vec!["id".to_string()]);
        let out = select.process(batch).await.unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].num_columns(), 1);
        assert_eq!(out[0].cell(0, "name"), Some(Value::Utf8("a".into())));
    }

    #[tokio::test]
    async fn select_missing_column_errors() {
        let batch = sample_batch();
        let mut select = Select::keep(vec!["nope".to_string()]);
        let err = select.process(batch).await.err().unwrap();
        assert!(matches!(err, Error::Schema(_)));
    }
}
