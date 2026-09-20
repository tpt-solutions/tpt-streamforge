"""Fix the last date matches: postgres coerce_to catch-all, sqlite decl/read."""

# 1. postgres coerce_to catch-all: add Date/Timestamp string parsing
p = 'tpt-stream-core/src/postgres.rs'
s = open(p, encoding='utf-8').read()
old = """                    DataType::Int32 => {
                        if let Ok(v) = s.parse() {
                            return Value::Int32(v);
                        }
                    }"""
new = """                    DataType::Date => {
                        if let Some(days) = tpt_stream_columnar::value::parse_date(s) {
                            return Value::Date(days);
                        }
                    }
                    DataType::Timestamp => {
                        if let Some(micros) = tpt_stream_columnar::value::parse_timestamp(s) {
                            return Value::Timestamp(micros);
                        }
                    }
                    DataType::Int32 => {
                        if let Ok(v) = s.parse() {
                            return Value::Int32(v);
                        }
                    }"""
assert old in s, 'coerce catch-all'
s = s.replace(old, new)
open(p, 'w', encoding='utf-8', newline='\n').write(s)
print('postgres coerce done')

# 2. sqlite infer_from_value + sqlite_value
p = 'tpt-stream-core/src/sqlite.rs'
s = open(p, encoding='utf-8').read()
old = """fn infer_from_value(value: &Value) -> DataType {
    match value {
        Value::Bool(_) => DataType::Bool,
        Value::Int32(_) | Value::Int64(_) => DataType::Int64,
        Value::Float32(_) | Value::Float64(_) => DataType::Float64,
        Value::Utf8(_) => DataType::Utf8,
        Value::Null => DataType::Utf8,
    }
}"""
new = """fn infer_from_value(value: &Value) -> DataType {
    match value {
        Value::Date(_) => DataType::Date,
        Value::Timestamp(_) => DataType::Timestamp,
        Value::Bool(_) => DataType::Bool,
        Value::Int32(_) | Value::Int64(_) => DataType::Int64,
        Value::Float32(_) | Value::Float64(_) => DataType::Float64,
        Value::Utf8(_) => DataType::Utf8,
        Value::Null => DataType::Utf8,
    }
}"""
assert old in s, 'sqlite infer'
s = s.replace(old, new)

old = """        (V::Real(f), DataType::Bool) => Value::Bool(f != 0.0),
        (V::Real(f), DataType::Utf8) => Value::Utf8(f.to_string()),
        (V::Text(s), _) => Value::Utf8(String::from_utf8_lossy(s).into_owned()),
        (V::Blob(b), _) => Value::Utf8(String::from_utf8_lossy(b).into_owned()),
    }
}"""
new = """        (V::Real(f), DataType::Bool) => Value::Bool(f != 0.0),
        (V::Real(f), DataType::Utf8) => Value::Utf8(f.to_string()),
        // SQLite stores dates as ISO text (DATE affinity columns read back
        // as Text); parse into the declared type when it's date-like.
        (V::Text(s), DataType::Date) => {
            let text = String::from_utf8_lossy(s);
            tpt_stream_columnar::value::parse_date(text.trim())
                .map_or_else(|| Value::Utf8(text.into_owned()), Value::Date)
        }
        (V::Text(s), DataType::Timestamp) => {
            let text = String::from_utf8_lossy(s);
            tpt_stream_columnar::value::parse_timestamp(text.trim())
                .map_or_else(|| Value::Utf8(text.into_owned()), Value::Timestamp)
        }
        (V::Integer(i), DataType::Date) => Value::Date(i as i32),
        (V::Integer(i), DataType::Timestamp) => Value::Timestamp(i),
        (V::Text(s), _) => Value::Utf8(String::from_utf8_lossy(s).into_owned()),
        (V::Blob(b), _) => Value::Utf8(String::from_utf8_lossy(b).into_owned()),
    }
}"""
assert old in s, 'sqlite_value'
s = s.replace(old, new)

# declared-type detection: DATE/TIMESTAMP decl types
old = """    } else if decl.contains("CHAR") || decl.contains("TEXT") || decl.contains("CLOB") {
        Some(DataType::Utf8)
    } else {
        None
    }
}"""
new = """    } else if decl.contains("CHAR") || decl.contains("TEXT") || decl.contains("CLOB") {
        Some(DataType::Utf8)
    } else if decl.contains("TIMESTAMP") || decl.contains("DATETIME") {
        Some(DataType::Timestamp)
    } else if decl.contains("DATE") {
        Some(DataType::Date)
    } else {
        None
    }
}"""
assert old in s, 'sqlite decltypes'
s = s.replace(old, new)
open(p, 'w', encoding='utf-8', newline='\n').write(s)
print('sqlite read-side done')
