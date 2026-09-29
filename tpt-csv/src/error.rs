use std::fmt;

/// Errors produced while reading or writing CSV.
#[derive(Debug)]
pub enum Error {
    /// The underlying reader/writer failed.
    Io(std::io::Error),
    /// A record contained bytes that were not valid UTF-8.
    Utf8 { line: u64 },
    /// A record had the wrong number of fields (columnar strict mode).
    Ragged {
        line: u64,
        expected: usize,
        got: usize,
    },
    /// A single record exceeded the reader's `max_record_bytes` cap (for
    /// example an unterminated quoted field swallowing the rest of the input).
    RecordTooLarge { line: u64, limit: usize },
}

pub type Result<T> = std::result::Result<T, Error>;

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Io(e) => write!(f, "csv: {e}"),
            Error::Utf8 { line } => write!(f, "csv: invalid UTF-8 in record at line {line}"),
            Error::Ragged {
                line,
                expected,
                got,
            } => write!(
                f,
                "csv: row at line {line} has {got} field(s), expected {expected}"
            ),
            Error::RecordTooLarge { line, limit } => write!(
                f,
                "csv: record at line {line} exceeds the {limit}-byte record size limit"
            ),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Io(e) => Some(e),
            Error::Utf8 { .. } | Error::Ragged { .. } | Error::RecordTooLarge { .. } => None,
        }
    }
}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Error::Io(e)
    }
}
