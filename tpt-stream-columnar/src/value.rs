use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DataType {
    Int32,
    Int64,
    Float32,
    Float64,
    Utf8,
    Bool,
    /// Calendar date (`YYYY-MM-DD`), stored as days since 1970-01-01.
    Date,
    /// Instant stored as microseconds since 1970-01-01T00:00:00Z.
    Timestamp,
}

impl DataType {
    pub const ALL: [DataType; 8] = [
        DataType::Int32,
        DataType::Int64,
        DataType::Float32,
        DataType::Float64,
        DataType::Utf8,
        DataType::Bool,
        DataType::Date,
        DataType::Timestamp,
    ];

    pub fn to_byte(self) -> u8 {
        match self {
            DataType::Int32 => 0,
            DataType::Int64 => 1,
            DataType::Float32 => 2,
            DataType::Float64 => 3,
            DataType::Utf8 => 4,
            DataType::Bool => 5,
            DataType::Date => 6,
            DataType::Timestamp => 7,
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
            6 => DataType::Date,
            7 => DataType::Timestamp,
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
            DataType::Date => "date",
            DataType::Timestamp => "timestamp",
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
    /// Days since 1970-01-01 (negative for earlier dates).
    Date(i32),
    /// Microseconds since 1970-01-01T00:00:00Z.
    Timestamp(i64),
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
            Value::Date(_) => DataType::Date,
            Value::Timestamp(_) => DataType::Timestamp,
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
            Value::Date(days) => write!(f, "{}", date_to_string(*days)),
            Value::Timestamp(micros) => write!(f, "{}", timestamp_to_string(*micros)),
            Value::Null => write!(f, "null"),
        }
    }
}

// ---------------------------------------------------------------------------
// Civil calendar conversion (Howard Hinnant's algorithms; no external deps)
// ---------------------------------------------------------------------------

/// Days since 1970-01-01 for a proleptic-Gregorian civil date.
pub fn days_from_civil(year: i64, month: u32, day: u32) -> i32 {
    let y = if month <= 2 { year - 1 } else { year };
    let era = y.div_euclid(400);
    let yoe = y - era * 400; // [0, 399]
    let m = month as i64;
    let d = day as i64;
    let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    (era * 146_097 + doe - 719_468) as i32
}

/// Inverse of [`days_from_civil`]: `(year, month, day)`.
pub fn civil_from_days(days: i32) -> (i64, u32, u32) {
    let z = days as i64 + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097; // [0, 146096]
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365; // [0, 399]
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // [0, 365]
    let mp = (5 * doy + 2) / 153; // [0, 11]
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32; // [1, 31]
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32; // [1, 12]
    (if m <= 2 { y + 1 } else { y }, m, d)
}

/// Parse a strict `YYYY-MM-DD` date; returns days since epoch.
pub fn parse_date(text: &str) -> Option<i32> {
    let bytes = text.as_bytes();
    if bytes.len() != 10 || bytes[4] != b'-' || bytes[7] != b'-' {
        return None;
    }
    let year: i64 = text.get(0..4)?.parse().ok()?;
    let month: u32 = text.get(5..7)?.parse().ok()?;
    let day: u32 = text.get(8..10)?.parse().ok()?;
    if !(1..=12).contains(&month) || !(1..=31).contains(&day) {
        return None;
    }
    // Reject impossible day-of-month per month length (leap years included).
    let leap = (year % 4 == 0 && year % 100 != 0) || year % 400 == 0;
    let max_day = match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if leap => 29,
        2 => 28,
        _ => return None,
    };
    if day > max_day {
        return None;
    }
    Some(days_from_civil(year, month, day))
}

/// Parse an ISO 8601 timestamp: `YYYY-MM-DD[T| ]HH:MM[:SS[.ffffff]]` with an
/// optional `Z`, `+HH:MM`, or `-HH:MM` offset (missing offset means UTC).
/// Returns microseconds since epoch.
pub fn parse_timestamp(text: &str) -> Option<i64> {
    let (date_part, rest) = text.split_once(['T', ' '])?;
    let days = parse_date(date_part)?;
    let rest = rest.trim_end_matches('Z');
    let (time_part, offset) = match rest.rfind(['+', '-']) {
        Some(idx) if idx > 0 => (&rest[..idx], Some(&rest[idx..])),
        _ => (rest, None),
    };
    let mut parts = time_part.split(':');
    let hour: i64 = parts.next()?.parse().ok()?;
    let minute: i64 = parts.next().unwrap_or("0").parse().ok()?;
    let (second, subsec_micros) = match parts.next() {
        Some(sec) if !sec.is_empty() => {
            let (whole, frac) = sec.split_once('.').unwrap_or((sec, ""));
            let second: i64 = whole.parse().ok()?;
            let mut micros = 0i64;
            if !frac.is_empty() {
                let padded: String = frac.chars().take(6).collect();
                micros = padded.parse::<i64>().ok()? * 10i64.pow(6 - padded.len() as u32);
            }
            (second, micros)
        }
        _ => (0, 0),
    };
    if hour > 23 || minute > 59 || second > 59 {
        return None;
    }
    let mut micros = days as i64 * 86_400_000_000
        + hour * 3_600_000_000
        + minute * 60_000_000
        + second * 1_000_000
        + subsec_micros;
    if let Some(offset) = offset {
        let sign = if offset.starts_with('-') { -1 } else { 1 };
        let offset = &offset[1..];
        let (oh, om) = offset.split_once(':').unwrap_or((offset, "0"));
        let shift = oh.parse::<i64>().ok()? * 3_600_000_000 + om.parse::<i64>().ok()? * 60_000_000;
        micros -= sign * shift;
    }
    Some(micros)
}

/// Render days since epoch as `YYYY-MM-DD`.
pub fn date_to_string(days: i32) -> String {
    let (y, m, d) = civil_from_days(days);
    format!("{y:04}-{m:02}-{d:02}")
}

/// Render microseconds since epoch as `YYYY-MM-DDTHH:MM:SS[.ffffff]Z`
/// (fractional part omitted when zero).
pub fn timestamp_to_string(micros: i64) -> String {
    let secs = micros.div_euclid(1_000_000);
    let subsec = micros.rem_euclid(1_000_000);
    let days = secs.div_euclid(86_400) as i32;
    let secs_of_day = secs.rem_euclid(86_400);
    let (y, m, d) = civil_from_days(days);
    let base = format!(
        "{y:04}-{m:02}-{d:02}T{:02}:{:02}:{:02}",
        secs_of_day / 3600,
        (secs_of_day % 3600) / 60,
        secs_of_day % 60
    );
    if subsec == 0 {
        format!("{base}Z")
    } else {
        format!("{base}.{:06}Z", subsec)
    }
}

#[cfg(test)]
mod date_tests {
    use super::*;

    #[test]
    fn date_roundtrip() {
        for days in [-25567, -1, 0, 1, 19_723] {
            let (y, m, d) = civil_from_days(days);
            assert_eq!(days_from_civil(y, m, d), days);
        }
    }

    #[test]
    fn parse_known_dates() {
        assert_eq!(parse_date("1970-01-01"), Some(0));
        assert_eq!(parse_date("2024-02-29"), Some(19_782)); // leap day
        assert_eq!(parse_date("2024-02-30"), None);
        assert_eq!(parse_date("2024-13-01"), None);
        assert_eq!(parse_date("24-1-1"), None);
    }

    #[test]
    fn parse_known_timestamps() {
        assert_eq!(parse_timestamp("1970-01-01T00:00:00Z"), Some(0));
        assert_eq!(parse_timestamp("1970-01-01 00:00:00"), Some(0));
        assert_eq!(
            parse_timestamp("2024-02-29T12:30:59.5Z"),
            Some(1_709_209_859_500_000)
        );
        // +02:00 offset shifts back to UTC.
        assert_eq!(parse_timestamp("1970-01-01T02:00:00+02:00"), Some(0));
        assert_eq!(parse_timestamp("1970-01-01"), None); // date-only is not a timestamp
    }

    #[test]
    fn timestamp_display_roundtrip() {
        let text = "2024-02-29T12:30:59Z";
        assert_eq!(timestamp_to_string(parse_timestamp(text).unwrap()), text);
    }
}
