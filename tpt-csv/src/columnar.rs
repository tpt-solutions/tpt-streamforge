use std::io::Read;

use crate::error::{Error, Result};
use crate::reader::{Reader, ReaderBuilder};
use crate::record::StringRecord;

/// A chunk of parsed CSV in column-major (SoA) layout.
///
/// Each column has its own arena and offset table — no row-major intermediate
/// and no transpose pass. Created by [`ColumnarReader::read_chunk_into`].
pub struct ColumnarChunk {
    pub num_columns: usize,
    pub row_count: usize,
    arenas: Vec<Vec<u8>>,
    offsets: Vec<Vec<(usize, usize)>>,
}

impl ColumnarChunk {
    /// Allocate a chunk for `num_columns` columns with initial row capacity
    /// `capacity_rows`.
    pub fn new(num_columns: usize, capacity_rows: usize) -> Self {
        let bytes_cap = capacity_rows * 8;
        ColumnarChunk {
            num_columns,
            row_count: 0,
            arenas: (0..num_columns)
                .map(|_| Vec::with_capacity(bytes_cap))
                .collect(),
            offsets: (0..num_columns)
                .map(|_| Vec::with_capacity(capacity_rows))
                .collect(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.row_count == 0
    }

    pub(crate) fn clear(&mut self) {
        self.row_count = 0;
        for a in &mut self.arenas {
            a.clear();
        }
        for o in &mut self.offsets {
            o.clear();
        }
    }

    /// Commit a staged row into the per-column arenas.
    ///
    /// `scratch_cells` must have exactly `self.num_columns` entries.
    pub(crate) fn commit_row(&mut self, scratch_arena: &[u8], scratch_cells: &[(usize, usize)]) {
        debug_assert_eq!(scratch_cells.len(), self.num_columns);
        for (col, &(start, len)) in scratch_cells.iter().enumerate() {
            let col_arena = &mut self.arenas[col];
            let col_start = col_arena.len();
            col_arena.extend_from_slice(&scratch_arena[start..start + len]);
            self.offsets[col].push((col_start, len));
        }
        self.row_count += 1;
    }

    /// Iterate over the field strings for column `i`.
    ///
    /// All bytes were validated as UTF-8 when stored, so the conversion is
    /// infallible.
    pub fn column_bytes(&self, i: usize) -> impl Iterator<Item = &str> {
        let arena = &self.arenas[i];
        self.offsets[i].iter().map(move |&(start, len)| {
            // SAFETY: every field was validated as UTF-8 by read_record_into.
            unsafe { std::str::from_utf8_unchecked(&arena[start..start + len]) }
        })
    }
}

/// How ragged rows (wrong number of fields) are handled by [`ColumnarReader`].
pub enum RaggedRowPolicy<'a> {
    /// Return a [`Error::Ragged`] error immediately.
    Strict,
    /// Silently drop the row and continue.
    Skip,
    /// Call the closure with the row's raw field strings. If the closure
    /// returns `Err`, propagate the I/O error and stop parsing.
    Quarantine(&'a mut dyn FnMut(&[&str]) -> std::io::Result<()>),
}

/// A columnar CSV reader that parses fields directly into per-column arenas.
///
/// No row-major intermediate is built and no transpose pass is required.
/// Each call to [`read_chunk_into`](ColumnarReader::read_chunk_into) fills a
/// [`ColumnarChunk`] with up to `max_rows` well-formed rows.
pub struct ColumnarReader<R> {
    reader: Reader<R>,
    headers: StringRecord,
    num_columns: usize,
    scratch_arena: Vec<u8>,
    scratch_cells: Vec<(usize, usize)>,
}

impl<R: Read> ColumnarReader<R> {
    /// Build from a raw reader. The first record is consumed as the header row.
    pub fn from_reader(reader: R) -> Result<Self> {
        let mut reader = ReaderBuilder::new()
            .has_headers(true)
            .flexible(true)
            .from_reader(reader);
        let headers = reader.headers()?.clone();
        let num_columns = headers.len();
        Ok(ColumnarReader {
            reader,
            headers,
            num_columns,
            scratch_arena: Vec::with_capacity(num_columns * 16),
            scratch_cells: Vec::with_capacity(num_columns),
        })
    }

    pub fn headers(&self) -> &StringRecord {
        &self.headers
    }

    pub fn num_columns(&self) -> usize {
        self.num_columns
    }

    /// Fill `chunk` with up to `max_rows` well-formed rows, applying `policy`
    /// to any ragged row. Returns `Ok(true)` when at least one row was added,
    /// `Ok(false)` when the input is exhausted and `chunk` is empty.
    pub fn read_chunk_into(
        &mut self,
        max_rows: usize,
        policy: &mut RaggedRowPolicy<'_>,
        chunk: &mut ColumnarChunk,
    ) -> Result<bool> {
        chunk.clear();
        while chunk.row_count < max_rows {
            self.scratch_arena.clear();
            self.scratch_cells.clear();
            let row_line = self.reader.position().line();
            let got = self
                .reader
                .read_record_into(&mut self.scratch_arena, &mut self.scratch_cells)?;
            if !got {
                break;
            }
            if self.scratch_cells.len() != self.num_columns {
                match policy {
                    RaggedRowPolicy::Strict => {
                        return Err(Error::Ragged {
                            line: row_line,
                            expected: self.num_columns,
                            got: self.scratch_cells.len(),
                        });
                    }
                    RaggedRowPolicy::Skip => continue,
                    RaggedRowPolicy::Quarantine(f) => {
                        let mut field_strs: Vec<&str> =
                            Vec::with_capacity(self.scratch_cells.len());
                        for &(start, len) in &self.scratch_cells {
                            // SAFETY: validated as UTF-8 by read_record_into.
                            field_strs.push(unsafe {
                                std::str::from_utf8_unchecked(
                                    &self.scratch_arena[start..start + len],
                                )
                            });
                        }
                        f(&field_strs).map_err(Error::Io)?;
                        continue;
                    }
                }
            }
            chunk.commit_row(&self.scratch_arena, &self.scratch_cells);
        }
        Ok(!chunk.is_empty())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn read_chunk(csv: &str) -> ColumnarChunk {
        let mut cr = ColumnarReader::from_reader(csv.as_bytes()).unwrap();
        let nc = cr.num_columns();
        let mut chunk = ColumnarChunk::new(nc, 64);
        let mut policy = RaggedRowPolicy::Strict;
        cr.read_chunk_into(64, &mut policy, &mut chunk).unwrap();
        chunk
    }

    #[test]
    fn simple_columnar_read() {
        let chunk = read_chunk("a,b,c\n1,hello,3\n4,world,6\n");
        assert_eq!(chunk.row_count, 2);
        assert_eq!(chunk.num_columns, 3);
        assert_eq!(chunk.column_bytes(0).collect::<Vec<_>>(), vec!["1", "4"]);
        assert_eq!(
            chunk.column_bytes(1).collect::<Vec<_>>(),
            vec!["hello", "world"]
        );
        assert_eq!(chunk.column_bytes(2).collect::<Vec<_>>(), vec!["3", "6"]);
    }

    #[test]
    fn quoted_fields_columnar() {
        let chunk = read_chunk("x,y\n\"hello, world\",\"line1\nline2\"\n");
        assert_eq!(chunk.row_count, 1);
        assert_eq!(chunk.column_bytes(0).next().unwrap(), "hello, world");
        assert_eq!(chunk.column_bytes(1).next().unwrap(), "line1\nline2");
    }

    #[test]
    fn ragged_strict_returns_error() {
        let mut cr = ColumnarReader::from_reader("a,b\n1\n".as_bytes()).unwrap();
        let mut chunk = ColumnarChunk::new(2, 8);
        let mut policy = RaggedRowPolicy::Strict;
        let err = cr.read_chunk_into(8, &mut policy, &mut chunk).unwrap_err();
        assert!(matches!(
            err,
            Error::Ragged {
                expected: 2,
                got: 1,
                ..
            }
        ));
    }

    #[test]
    fn ragged_skip_drops_row() {
        let mut cr = ColumnarReader::from_reader("a,b\n1\n2,3\n".as_bytes()).unwrap();
        let mut chunk = ColumnarChunk::new(2, 8);
        let mut policy = RaggedRowPolicy::Skip;
        cr.read_chunk_into(8, &mut policy, &mut chunk).unwrap();
        assert_eq!(chunk.row_count, 1);
        assert_eq!(chunk.column_bytes(0).next().unwrap(), "2");
    }

    #[test]
    fn ragged_quarantine_calls_callback() {
        let mut captured: Vec<Vec<String>> = Vec::new();
        let mut cr = ColumnarReader::from_reader("a,b\n1\n2,3\n".as_bytes()).unwrap();
        let mut chunk = ColumnarChunk::new(2, 8);
        let mut policy = RaggedRowPolicy::Quarantine(&mut |fields: &[&str]| {
            captured.push(fields.iter().map(|s| s.to_string()).collect());
            Ok(())
        });
        cr.read_chunk_into(8, &mut policy, &mut chunk).unwrap();
        assert_eq!(chunk.row_count, 1);
        assert_eq!(captured, vec![vec!["1".to_string()]]);
    }

    #[test]
    fn chunk_reads_are_bounded_by_max_rows() {
        let mut cr = ColumnarReader::from_reader("n\n1\n2\n3\n4\n5\n".as_bytes()).unwrap();
        let mut chunk = ColumnarChunk::new(1, 2);
        let mut policy = RaggedRowPolicy::Strict;
        assert!(cr.read_chunk_into(2, &mut policy, &mut chunk).unwrap());
        assert_eq!(chunk.row_count, 2);
        assert!(cr.read_chunk_into(2, &mut policy, &mut chunk).unwrap());
        assert_eq!(chunk.row_count, 2);
        assert!(cr.read_chunk_into(2, &mut policy, &mut chunk).unwrap());
        assert_eq!(chunk.row_count, 1);
        assert!(!cr.read_chunk_into(2, &mut policy, &mut chunk).unwrap());
    }
}
