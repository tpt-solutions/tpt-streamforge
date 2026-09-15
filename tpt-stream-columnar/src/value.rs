use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DataType {
    Int32,
    Int64,
    Float32,
    Float64,
    Utf8,
    Bool,
}

impl DataType {
    pub const ALL: [DataType; 6] = [
        DataType::Int32,
        DataType::Int64,
        DataType::Float32,
        DataType::Float64,
        DataType::Utf8,
        DataType::Bool,
    ];

    pub fn to_byte(self) -> u8 {
        match self {
            DataType::Int32 => 0,
            DataType::Int64 => 1,
            DataType::Float32 => 2,
            DataType::Float64 => 3,
            DataType::Utf8 => 4,
            DataType::Bool => 5,
        }
    }

    pub fn from_byte(b: u8) -> Option<DataType> {
        Some(match b {
            0 => DataType::Int32,
            1 => DataType::Int64,
            2 => DataType::Float32,
            3 => DataType::Float64,
            4 => DataType::Utf8,
            5 => DataType::Bool,
            _ => return None,
        })
    }
}

impl fmt::Display for DataType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            DataType::Int32 => "int32",
            DataType::Int64 => "int64",
            DataType::Float32 => "float32",
            DataType::Float64 => "float64",
            DataType::Utf8 => "utf8",
            DataType::Bool => "bool",
        };
        f.write_str(s)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Int32(i32),
    Int64(i64),
    Float32(f32),
    Float64(f64),
    Utf8(String),
    Bool(bool),
    Null,
}

impl Value {
    pub fn data_type(&self) -> DataType {
        match self {
            Value::Int32(_) => DataType::Int32,
            Value::Int64(_) => DataType::Int64,
            Value::Float32(_) => DataType::Float32,
            Value::Float64(_) => DataType::Float64,
            Value::Utf8(_) => DataType::Utf8,
            Value::Bool(_) => DataType::Bool,
            Value::Null => DataType::Utf8,
        }
    }
}

impl From<i32> for Value {
    fn from(v: i32) -> Self {
        Value::Int32(v)
    }
}

impl From<i64> for Value {
    fn from(v: i64) -> Self {
        Value::Int64(v)
    }
}

impl From<f32> for Value {
    fn from(v: f32) -> Self {
        Value::Float32(v)
    }
}

impl From<f64> for Value {
    fn from(v: f64) -> Self {
        Value::Float64(v)
    }
}

impl From<String> for Value {
    fn from(v: String) -> Self {
        Value::Utf8(v)
    }
}

impl From<&str> for Value {
    fn from(v: &str) -> Self {
        Value::Utf8(v.to_string())
    }
}

impl From<bool> for Value {
    fn from(v: bool) -> Self {
        Value::Bool(v)
    }
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::Int32(v) => write!(f, "{v}"),
            Value::Int64(v) => write!(f, "{v}"),
            Value::Float32(v) => write!(f, "{v}"),
            Value::Float64(v) => write!(f, "{v}"),
            Value::Utf8(v) => write!(f, "{v}"),
            Value::Bool(v) => write!(f, "{v}"),
            Value::Null => write!(f, "null"),
        }
    }
}
