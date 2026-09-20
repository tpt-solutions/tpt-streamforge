use crate::value::{DataType, Value};

#[derive(Debug, Clone)]
pub enum ColumnBuffer {
    Int32(Vec<i32>),
    Int64(Vec<i64>),
    Float32(Vec<f32>),
    Float64(Vec<f64>),
    Utf8(Vec<String>),
    Bool(Vec<bool>),
    /// Days since 1970-01-01.
    Date(Vec<i32>),
    /// Microseconds since 1970-01-01T00:00:00Z.
    Timestamp(Vec<i64>),
}

impl ColumnBuffer {
    pub fn with_capacity(data_type: DataType, capacity: usize) -> Self {
        match data_type {
            DataType::Int32 => ColumnBuffer::Int32(Vec::with_capacity(capacity)),
            DataType::Int64 => ColumnBuffer::Int64(Vec::with_capacity(capacity)),
            DataType::Float32 => ColumnBuffer::Float32(Vec::with_capacity(capacity)),
            DataType::Float64 => ColumnBuffer::Float64(Vec::with_capacity(capacity)),
            DataType::Utf8 => ColumnBuffer::Utf8(Vec::with_capacity(capacity)),
            DataType::Bool => ColumnBuffer::Bool(Vec::with_capacity(capacity)),
            DataType::Date => ColumnBuffer::Date(Vec::with_capacity(capacity)),
            DataType::Timestamp => ColumnBuffer::Timestamp(Vec::with_capacity(capacity)),
        }
    }

    pub fn len(&self) -> usize {
        match self {
            ColumnBuffer::Int32(v) => v.len(),
            ColumnBuffer::Int64(v) => v.len(),
            ColumnBuffer::Float32(v) => v.len(),
            ColumnBuffer::Float64(v) => v.len(),
            ColumnBuffer::Utf8(v) => v.len(),
            ColumnBuffer::Bool(v) => v.len(),
            ColumnBuffer::Date(v) => v.len(),
            ColumnBuffer::Timestamp(v) => v.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn data_type(&self) -> DataType {
        match self {
            ColumnBuffer::Int32(_) => DataType::Int32,
            ColumnBuffer::Int64(_) => DataType::Int64,
            ColumnBuffer::Float32(_) => DataType::Float32,
            ColumnBuffer::Float64(_) => DataType::Float64,
            ColumnBuffer::Utf8(_) => DataType::Utf8,
            ColumnBuffer::Bool(_) => DataType::Bool,
            ColumnBuffer::Date(_) => DataType::Date,
            ColumnBuffer::Timestamp(_) => DataType::Timestamp,
        }
    }

    /// Push a non-null value. Panics (via debug check) if the variant mismatches.
    pub fn push_value(&mut self, value: Value) {
        match (self, value) {
            (ColumnBuffer::Int32(buf), Value::Int32(v)) => buf.push(v),
            (ColumnBuffer::Int64(buf), Value::Int64(v)) => buf.push(v),
            (ColumnBuffer::Float32(buf), Value::Float32(v)) => buf.push(v),
            (ColumnBuffer::Float64(buf), Value::Float64(v)) => buf.push(v),
            (ColumnBuffer::Utf8(buf), Value::Utf8(v)) => buf.push(v),
            (ColumnBuffer::Bool(buf), Value::Bool(v)) => buf.push(v),
            (ColumnBuffer::Date(buf), Value::Date(v)) => buf.push(v),
            (ColumnBuffer::Timestamp(buf), Value::Timestamp(v)) => buf.push(v),
            (column, value) => panic!(
                "type mismatch: cannot push {value:?} into {:?} buffer",
                column.data_type()
            ),
        }
    }

    /// Push a null representation (a sentinel value for numeric/bool types).
    pub fn push_null(&mut self) {
        match self {
            ColumnBuffer::Int32(buf) => buf.push(0),
            ColumnBuffer::Int64(buf) => buf.push(0),
            ColumnBuffer::Float32(buf) => buf.push(0.0),
            ColumnBuffer::Float64(buf) => buf.push(0.0),
            ColumnBuffer::Utf8(buf) => buf.push(String::new()),
            ColumnBuffer::Bool(buf) => buf.push(false),
            ColumnBuffer::Date(buf) => buf.push(0),
            ColumnBuffer::Timestamp(buf) => buf.push(0),
        }
    }

    pub fn get_value(&self, index: usize) -> Option<Value> {
        let value = match self {
            ColumnBuffer::Int32(v) => Value::Int32(*v.get(index)?),
            ColumnBuffer::Int64(v) => Value::Int64(*v.get(index)?),
            ColumnBuffer::Float32(v) => Value::Float32(*v.get(index)?),
            ColumnBuffer::Float64(v) => Value::Float64(*v.get(index)?),
            ColumnBuffer::Utf8(v) => Value::Utf8(v.get(index)?.clone()),
            ColumnBuffer::Bool(v) => Value::Bool(*v.get(index)?),
            ColumnBuffer::Date(v) => Value::Date(*v.get(index)?),
            ColumnBuffer::Timestamp(v) => Value::Timestamp(*v.get(index)?),
        };
        Some(value)
    }

    pub fn set_value(&mut self, index: usize, value: Value) {
        match (self, value) {
            (ColumnBuffer::Int32(buf), Value::Int32(v)) => buf[index] = v,
            (ColumnBuffer::Int64(buf), Value::Int64(v)) => buf[index] = v,
            (ColumnBuffer::Float32(buf), Value::Float32(v)) => buf[index] = v,
            (ColumnBuffer::Float64(buf), Value::Float64(v)) => buf[index] = v,
            (ColumnBuffer::Utf8(buf), Value::Utf8(v)) => buf[index] = v,
            (ColumnBuffer::Bool(buf), Value::Bool(v)) => buf[index] = v,
            (ColumnBuffer::Date(buf), Value::Date(v)) => buf[index] = v,
            (ColumnBuffer::Timestamp(buf), Value::Timestamp(v)) => buf[index] = v,
            (column, value) => panic!(
                "type mismatch: cannot set {value:?} into {:?} buffer",
                column.data_type()
            ),
        }
    }

    /// Write the null sentinel value at `index` (caller is responsible for the null bitmap).
    pub fn set_null_sentinel(&mut self, index: usize) {
        match self {
            ColumnBuffer::Int32(buf) => buf[index] = 0,
            ColumnBuffer::Int64(buf) => buf[index] = 0,
            ColumnBuffer::Float32(buf) => buf[index] = 0.0,
            ColumnBuffer::Float64(buf) => buf[index] = 0.0,
            ColumnBuffer::Utf8(buf) => buf[index] = String::new(),
            ColumnBuffer::Bool(buf) => buf[index] = false,
            ColumnBuffer::Date(buf) => buf[index] = 0,
            ColumnBuffer::Timestamp(buf) => buf[index] = 0,
        }
    }

    pub fn extend_from(&mut self, other: &ColumnBuffer) {
        match (self, other) {
            (ColumnBuffer::Int32(a), ColumnBuffer::Int32(b)) => a.extend_from_slice(b),
            (ColumnBuffer::Int64(a), ColumnBuffer::Int64(b)) => a.extend_from_slice(b),
            (ColumnBuffer::Float32(a), ColumnBuffer::Float32(b)) => a.extend_from_slice(b),
            (ColumnBuffer::Float64(a), ColumnBuffer::Float64(b)) => a.extend_from_slice(b),
            (ColumnBuffer::Utf8(a), ColumnBuffer::Utf8(b)) => a.extend_from_slice(b),
            (ColumnBuffer::Bool(a), ColumnBuffer::Bool(b)) => a.extend_from_slice(b),
            (ColumnBuffer::Date(a), ColumnBuffer::Date(b)) => a.extend_from_slice(b),
            (ColumnBuffer::Timestamp(a), ColumnBuffer::Timestamp(b)) => a.extend_from_slice(b),
            _ => panic!("column type mismatch in extend_from"),
        }
    }

    /// Remove the element at `index` without preserving order (swap-remove). Fast,
    /// but changes row order. Used for in-place filtering where order is preserved
    /// separately, so prefer `retain`.
    pub fn swap_remove(&mut self, index: usize) {
        match self {
            ColumnBuffer::Int32(v) => {
                v.swap_remove(index);
            }
            ColumnBuffer::Int64(v) => {
                v.swap_remove(index);
            }
            ColumnBuffer::Float32(v) => {
                v.swap_remove(index);
            }
            ColumnBuffer::Float64(v) => {
                v.swap_remove(index);
            }
            ColumnBuffer::Utf8(v) => {
                v.swap_remove(index);
            }
            ColumnBuffer::Bool(v) => {
                v.swap_remove(index);
            }
            ColumnBuffer::Date(v) => {
                v.swap_remove(index);
            }
            ColumnBuffer::Timestamp(v) => {
                v.swap_remove(index);
            }
        }
    }

    pub fn truncate(&mut self, len: usize) {
        match self {
            ColumnBuffer::Int32(v) => v.truncate(len),
            ColumnBuffer::Int64(v) => v.truncate(len),
            ColumnBuffer::Float32(v) => v.truncate(len),
            ColumnBuffer::Float64(v) => v.truncate(len),
            ColumnBuffer::Utf8(v) => v.truncate(len),
            ColumnBuffer::Bool(v) => v.truncate(len),
            ColumnBuffer::Date(v) => v.truncate(len),
            ColumnBuffer::Timestamp(v) => v.truncate(len),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Column {
    name: String,
    data_type: DataType,
    buffer: ColumnBuffer,
    nulls: Vec<bool>,
}

impl Column {
    pub fn new(name: impl Into<String>, data_type: DataType, capacity: usize) -> Self {
        Column {
            name: name.into(),
            data_type,
            buffer: ColumnBuffer::with_capacity(data_type, capacity),
            nulls: Vec::with_capacity(capacity),
        }
    }

    pub fn from_data(
        name: impl Into<String>,
        data_type: DataType,
        buffer: ColumnBuffer,
        nulls: Vec<bool>,
    ) -> Self {
        assert_eq!(buffer.len(), nulls.len(), "null bitmap length mismatch");
        Column {
            name: name.into(),
            data_type,
            buffer,
            nulls,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn data_type(&self) -> DataType {
        self.data_type
    }

    pub fn buffer(&self) -> &ColumnBuffer {
        &self.buffer
    }

    pub fn buffer_mut(&mut self) -> &mut ColumnBuffer {
        &mut self.buffer
    }

    pub fn nulls(&self) -> &[bool] {
        &self.nulls
    }

    pub fn is_null(&self, index: usize) -> bool {
        self.nulls.get(index).copied().unwrap_or(false)
    }

    pub fn len(&self) -> usize {
        self.buffer.len()
    }

    pub fn is_empty(&self) -> bool {
        self.buffer.is_empty()
    }

    pub fn push(&mut self, value: Value) {
        match value {
            Value::Null => {
                self.buffer.push_null();
                self.nulls.push(true);
            }
            value => {
                self.buffer.push_value(value);
                self.nulls.push(false);
            }
        }
    }

    pub fn get(&self, index: usize) -> Option<Value> {
        if self.is_null(index) {
            return Some(Value::Null);
        }
        self.buffer.get_value(index)
    }

    pub fn set(&mut self, index: usize, value: Value) {
        match value {
            Value::Null => {
                self.buffer.set_null_sentinel(index);
                self.nulls[index] = true;
            }
            value => {
                self.buffer.set_value(index, value);
                self.nulls[index] = false;
            }
        }
    }

    /// Rebuild this column keeping only the rows for which `keep[i]` is true, preserving order.
    /// Keep only the first `n` rows.
    pub fn truncate(&mut self, n: usize) {
        self.buffer.truncate(n);
        self.nulls.truncate(n);
    }

    pub fn retain_rows(&mut self, keep: &[bool]) {
        let mut cursor = 0usize;
        for (i, keep_row) in keep.iter().enumerate() {
            if *keep_row {
                if cursor != i {
                    let v = self.buffer.get_value(i).expect("in-bounds");
                    self.buffer.set_value(cursor, v);
                    self.nulls[cursor] = self.nulls[i];
                }
                cursor += 1;
            }
        }
        self.buffer.truncate(cursor);
        self.nulls.truncate(cursor);
    }

    pub fn append_column(&mut self, other: &Column) {
        assert_eq!(
            self.data_type, other.data_type,
            "data type mismatch in append_column: {:?} vs {:?}",
            self.data_type, other.data_type
        );
        self.buffer.extend_from(&other.buffer);
        self.nulls.extend_from_slice(&other.nulls);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn column_push_get() {
        let mut col = Column::new("id", DataType::Int32, 4);
        col.push(Value::Int32(1));
        col.push(Value::Int32(2));
        assert_eq!(col.len(), 2);
        assert_eq!(col.get(0), Some(Value::Int32(1)));
        assert_eq!(col.get(1), Some(Value::Int32(2)));
        assert_eq!(col.get(2), None);
    }

    #[test]
    fn column_nulls() {
        let mut col = Column::new("id", DataType::Int32, 3);
        col.push(Value::Int32(1));
        col.push(Value::Null);
        col.push(Value::Int32(3));
        assert_eq!(col.len(), 3);
        assert_eq!(col.get(0), Some(Value::Int32(1)));
        assert_eq!(col.get(1), Some(Value::Null));
        assert_eq!(col.get(2), Some(Value::Int32(3)));
        assert_eq!(col.nulls(), &[false, true, false]);
    }

    #[test]
    fn column_string_nulls() {
        let mut col = Column::new("name", DataType::Utf8, 2);
        col.push(Value::Utf8("alice".into()));
        col.push(Value::Null);
        assert_eq!(col.get(0), Some(Value::Utf8("alice".into())));
        assert_eq!(col.get(1), Some(Value::Null));
    }

    #[test]
    fn column_retain() {
        let mut col = Column::new("id", DataType::Int32, 4);
        col.push(Value::Int32(1));
        col.push(Value::Int32(2));
        col.push(Value::Null);
        col.push(Value::Int32(4));
        col.retain_rows(&[true, false, true, true]);
        assert_eq!(col.len(), 3);
        assert_eq!(col.get(0), Some(Value::Int32(1)));
        assert_eq!(col.get(1), Some(Value::Null));
        assert_eq!(col.get(2), Some(Value::Int32(4)));
    }

    #[test]
    fn column_append() {
        let mut a = Column::new("id", DataType::Int32, 2);
        a.push(Value::Int32(1));
        a.push(Value::Null);
        let mut b = Column::new("id", DataType::Int32, 1);
        b.push(Value::Int32(9));
        a.append_column(&b);
        assert_eq!(a.len(), 3);
        assert_eq!(a.get(1), Some(Value::Null));
        assert_eq!(a.get(2), Some(Value::Int32(9)));
    }

    #[test]
    #[should_panic]
    fn column_type_mismatch_panics() {
        let mut col = Column::new("id", DataType::Int32, 1);
        col.push(Value::Utf8("oops".into()));
    }
}
