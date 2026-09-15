use crate::table::RecordBatch;

#[derive(Clone, Copy)]
pub struct Row<'a> {
    batch: &'a RecordBatch,
    index: usize,
}

impl<'a> Row<'a> {
    pub fn new(batch: &'a RecordBatch, index: usize) -> Self {
        debug_assert!(index < batch.num_rows());
        Row { batch, index }
    }

    pub fn index(&self) -> usize {
        self.index
    }

    pub fn batch(&self) -> &'a RecordBatch {
        self.batch
    }

    pub fn get(&self, column: &str) -> Option<crate::value::Value> {
        self.batch.cell(self.index, column)
    }

    pub fn num_columns(&self) -> usize {
        self.batch.num_columns()
    }

    pub fn field_names(&self) -> Vec<&str> {
        self.batch.column_names()
    }
}

impl<'a> std::fmt::Debug for Row<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let names = self.batch.column_names();
        let mut map = f.debug_map();
        for name in names {
            map.entry(&name, &self.batch.cell(self.index, name));
        }
        map.finish()
    }
}
