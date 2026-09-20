//! A minimal, dependency-free streaming CSV reader/writer.
//!
//! Written for [`tpt-streamforge`](https://github.com/tpt-solutions/tpt-streamforge)
//! to replace the (excellent, but Apache-2.0-only-adjacent via its `ryu`
//! dependency) [`csv`](https://docs.rs/csv) crate, so that a consumer who
//! wants this project's dependency tree available under MIT terms alone
//! always has that option. It covers the RFC 4180 subset that matters in
//! practice:
//!
//! - Fields separated by `,`; records separated by `\n` or `\r\n`.
//! - A field may be quoted with `"`; a literal `"` inside a quoted field is
//!   written as `""`.
//! - Quoted fields may contain commas and embedded newlines.
//! - Records are not required to have a fixed field count — this crate
//!   reports whatever it reads/writes and leaves that policy decision to
//!   the caller (some callers want ragged rows to be a hard error, others
//!   want to quarantine or drop them).
//!
//! ```
//! use tpt_csv::{ReaderBuilder, StringRecord};
//!
//! let mut reader = ReaderBuilder::new().from_reader("a,b\n1,2\n".as_bytes());
//! assert_eq!(reader.headers().unwrap().iter().collect::<Vec<_>>(), vec!["a", "b"]);
//! let mut record = StringRecord::new();
//! assert!(reader.read_record(&mut record).unwrap());
//! assert_eq!(record.iter().collect::<Vec<_>>(), vec!["1", "2"]);
//! ```

mod error;
mod reader;
mod record;
mod writer;

pub use error::{Error, Result};
pub use reader::{Position, Reader, ReaderBuilder};
pub use record::StringRecord;
pub use writer::{Writer, WriterBuilder};
