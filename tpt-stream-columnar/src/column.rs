use crate::value::{DataType, Value};

/// A [`Value`] (or a whole buffer) was written into a column of a different
/// type. Raised by the `try_*` methods; the non-`try_` builders turn it into a
/// panic because a mismatch there is a bug in the calling code, not bad input.
#[derive(Debug, Clone, PartialEq)]
pub struct TypeMismatch {
    /// The type the column was declared with.
    pub column: DataType,
    /// The type (and sample) that was offered instead.
    pub value: Value,
}

impl std::fmt::Display for TypeMismatch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "type mismatch: {} value for a {:?} column",
            value_type_name(&self.value),
            self.column
        )
    }
}

impl std::error::Error for TypeMismatch {}

fn value_type_name(value: &Value) -> &'static str {
    match value {
        Value::Null => "null",
        Value::Int32(_) => "int32",
        Value::Int64(_) => "int64",
        Value::Float32(_) => "float32",
        Value::Float64(_) => "float64",
        Value::Utf8(_) => "string",
        Value::Bool(_) => "bool",
        Value::Date(_) => "date",
        Value::Timestamp(_) => "timestamp",
    }
}

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

    /// A representative value for error messages. Empty buffers yield a typed
    /// zero/null so a mismatch is still reportable.
    fn sample_value(&self) -> Value {
        match self {
            ColumnBuffer::Int32(v) => v
                .first()
                .map(|x| Value::Int32(*x))
                .unwrap_or(Value::Int32(0)),
            ColumnBuffer::Int64(v) => v
                .first()
                .map(|x| Value::Int64(*x))
                .unwrap_or(Value::Int64(0)),
            ColumnBuffer::Float32(v) => v
                .first()
                .map(|x| Value::Float32(*x))
                .unwrap_or(Value::Float32(0.0)),
            ColumnBuffer::Float64(v) => v
                .first()
                .map(|x| Value::Float64(*x))
                .unwrap_or(Value::Float64(0.0)),
            ColumnBuffer::Utf8(v) => v
                .first()
                .map(|x| Value::Utf8(x.clone()))
                .unwrap_or(Value::Utf8(String::new())),
            ColumnBuffer::Bool(v) => v
                .first()
                .map(|x| Value::Bool(*x))
                .unwrap_or(Value::Bool(false)),
            ColumnBuffer::Date(v) => v.first().map(|x| Value::Date(*x)).unwrap_or(Value::Date(0)),
            ColumnBuffer::Timestamp(v) => v
                .first()
                .map(|x| Value::Timestamp(*x))
                .unwrap_or(Value::Timestamp(0)),
        }
    }

    /// Fallible [`Self::push_value`]. Prefer this when the value's type comes
    /// from outside the program (a decoded file, a user-supplied map) rather
    /// than from a match arm the compiler checked.
    pub fn try_push_value(&mut self, value: Value) -> Result<(), TypeMismatch> {
        match (self, value) {
            (ColumnBuffer::Int32(buf), Value::Int32(v)) => buf.push(v),
            (ColumnBuffer::Int64(buf), Value::Int64(v)) => buf.push(v),
            (ColumnBuffer::Float32(buf), Value::Float32(v)) => buf.push(v),
            (ColumnBuffer::Float64(buf), Value::Float64(v)) => buf.push(v),
            (ColumnBuffer::Utf8(buf), Value::Utf8(v)) => buf.push(v),
            (ColumnBuffer::Bool(buf), Value::Bool(v)) => buf.push(v),
            (ColumnBuffer::Date(buf), Value::Date(v)) => buf.push(v),
            (ColumnBuffer::Timestamp(buf), Value::Timestamp(v)) => buf.push(v),
            (column, value) => {
                return Err(TypeMismatch {
                    column: column.data_type(),
                    value,
                })
            }
        }
        Ok(())
    }

    /// Push a non-null value. Panics if the variant mismatches; use
    /// [`Self::try_push_value`] for values whose type is not statically known.
    pub fn push_value(&mut self, value: Value) {
        if let Err(mismatch) = self.try_push_value(value) {
            panic!("{mismatch}");
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

    /// Fallible [`Self::set_value`]. Use when the value's type is not
    /// statically known to match the buffer.
    pub fn try_set_value(&mut self, index: usize, value: Value) -> Result<(), TypeMismatch> {
        match (self, value) {
            (ColumnBuffer::Int32(buf), Value::Int32(v)) => buf[index] = v,
            (ColumnBuffer::Int64(buf), Value::Int64(v)) => buf[index] = v,
            (ColumnBuffer::Float32(buf), Value::Float32(v)) => buf[index] = v,
            (ColumnBuffer::Float64(buf), Value::Float64(v)) => buf[index] = v,
            (ColumnBuffer::Utf8(buf), Value::Utf8(v)) => buf[index] = v,
            (ColumnBuffer::Bool(buf), Value::Bool(v)) => buf[index] = v,
            (ColumnBuffer::Date(buf), Value::Date(v)) => buf[index] = v,
            (ColumnBuffer::Timestamp(buf), Value::Timestamp(v)) => buf[index] = v,
            (column, value) => {
                return Err(TypeMismatch {
                    column: column.data_type(),
                    value,
                })
            }
        }
        Ok(())
    }

    /// Panics if the variant mismatches; use [`Self::try_set_value`] otherwise.
    pub fn set_value(&mut self, index: usize, value: Value) {
        if let Err(mismatch) = self.try_set_value(index, value) {
            panic!("{mismatch}");
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

    /// Fallible [`Self::extend_from`]: the two buffers must hold the same type.
    pub fn try_extend_from(&mut self, other: &ColumnBuffer) -> Result<(), TypeMismatch> {
        match (self, other) {
            (ColumnBuffer::Int32(a), ColumnBuffer::Int32(b)) => a.extend_from_slice(b),
            (ColumnBuffer::Int64(a), ColumnBuffer::Int64(b)) => a.extend_from_slice(b),
            (ColumnBuffer::Float32(a), ColumnBuffer::Float32(b)) => a.extend_from_slice(b),
            (ColumnBuffer::Float64(a), ColumnBuffer::Float64(b)) => a.extend_from_slice(b),
            (ColumnBuffer::Utf8(a), ColumnBuffer::Utf8(b)) => a.extend_from_slice(b),
            (ColumnBuffer::Bool(a), ColumnBuffer::Bool(b)) => a.extend_from_slice(b),
            (ColumnBuffer::Date(a), ColumnBuffer::Date(b)) => a.extend_from_slice(b),
            (ColumnBuffer::Timestamp(a), ColumnBuffer::Timestamp(b)) => a.extend_from_slice(b),
            (column, other) => {
                return Err(TypeMismatch {
                    column: column.data_type(),
                    value: other.sample_value(),
                })
            }
        }
        Ok(())
    }

    /// Panics on a type mismatch; use [`Self::try_extend_from`] for buffers
    /// whose type is not statically known.
    pub fn extend_from(&mut self, other: &ColumnBuffer) {
        if let Err(mismatch) = self.try_extend_from(other) {
            panic!("{mismatch}");
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

    /// Fallible [`Self::push`]. Use when the value's type is not statically
    /// known to match the column (e.g. a value decoded from a file).
    pub fn try_push(&mut self, value: Value) -> Result<(), TypeMismatch> {
        match value {
            Value::Null => {
                self.buffer.push_null();
                self.nulls.push(true);
                Ok(())
            }
            value => {
                self.buffer.try_push_value(value)?;
                self.nulls.push(false);
                Ok(())
            }
        }
    }

    /// Panics on a type mismatch; use [`Self::try_push`] for untrusted values.
    pub fn push(&mut self, value: Value) {
        if let Err(mismatch) = self.try_push(value) {
            panic!("{mismatch}");
        }
    }

    pub fn get(&self, index: usize) -> Option<Value> {
        if self.is_null(index) {
            return Some(Value::Null);
        }
        self.buffer.get_value(index)
    }

    /// Fallible [`Self::set`]. Use when the value's type is not statically
    /// known to match the column.
    pub fn try_set(&mut self, index: usize, value: Value) -> Result<(), TypeMismatch> {
        match value {
            Value::Null => {
                self.buffer.set_null_sentinel(index);
                self.nulls[index] = true;
                Ok(())
            }
            value => {
                self.buffer.try_set_value(index, value)?;
                self.nulls[index] = false;
                Ok(())
            }
        }
    }

    /// Panics on a type mismatch; use [`Self::try_set`] for untrusted values.
    pub fn set(&mut self, index: usize, value: Value) {
        if let Err(mismatch) = self.try_set(index, value) {
            panic!("{mismatch}");
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

    /// Fallible [`Self::append_column`]. The two columns must have the same
    /// data type; use this instead of `append_column` when that is not
    /// statically guaranteed (e.g. columns read from an untrusted file).
    pub fn try_append_column(&mut self, other: &Column) -> Result<(), TypeMismatch> {
        if self.data_type != other.data_type {
            return Err(TypeMismatch {
                column: self.data_type,
                value: other.buffer.sample_value(),
            });
        }
        self.buffer.try_extend_from(&other.buffer)?;
        self.nulls.extend_from_slice(&other.nulls);
        Ok(())
    }

    /// Panics if the data types differ; use [`Self::try_append_column`] when
    /// the types are not statically known to match.
    pub fn append_column(&mut self, other: &Column) {
        if let Err(mismatch) = self.try_append_column(other) {
            panic!("data type mismatch in append_column: {mismatch}");
        }
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
    #[should_panic(expected = "type mismatch: string value for a Int32 column")]
    fn column_type_mismatch_panics() {
        let mut col = Column::new("id", DataType::Int32, 1);
        col.push(Value::Utf8("oops".into()));
    }

    /// The `try_*` form must report the mismatch instead of aborting, so a
    /// decoder handling untrusted input can reject the row and carry on.
    #[test]
    fn column_try_push_reports_mismatch() {
        let mut col = Column::new("id", DataType::Int32, 1);
        let err = col.try_push(Value::Utf8("oops".into())).unwrap_err();
        assert_eq!(err.column, DataType::Int32);
        assert_eq!(value_type_name(&err.value), "string");
        // The failed push left the column untouched.
        assert_eq!(col.len(), 0);
        col.try_push(Value::Int32(7)).unwrap();
        assert_eq!(col.get(0), Some(Value::Int32(7)));
    }

    #[test]
    fn column_try_set_reports_mismatch() {
        let mut col = Column::new("id", DataType::Int32, 2);
        col.push(Value::Int32(1));
        col.push(Value::Int32(2));
        assert!(col.try_set(0, Value::Bool(true)).is_err());
        assert_eq!(col.get(0), Some(Value::Int32(1)));
        col.try_set(0, Value::Int32(9)).unwrap();
        assert_eq!(col.get(0), Some(Value::Int32(9)));
    }

    #[test]
    fn column_try_append_reports_mismatch() {
        let mut a = Column::new("id", DataType::Int32, 1);
        a.push(Value::Int32(1));
        let mut b = Column::new("id", DataType::Int64, 1);
        b.push(Value::Int64(2));
        let err = a.try_append_column(&b).unwrap_err();
        assert_eq!(err.column, DataType::Int32);
        assert_eq!(value_type_name(&err.value), "int64");
        assert_eq!(a.len(), 1, "a failed append must not change the column");

        let mut c = Column::new("id", DataType::Int32, 1);
        c.push(Value::Int32(3));
        a.try_append_column(&c).unwrap();
        assert_eq!(a.len(), 2);
    }
}
