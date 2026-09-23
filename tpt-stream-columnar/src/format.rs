use crate::column::Column;
use crate::table::RecordBatch;
use crate::value::{DataType, Value};
use std::io::{Read, Write};
use thiserror::Error;

pub const MAGIC: [u8; 8] = *b"TPTCOL1\x00";
pub const VERSION: u16 = 1;

const FLAG_ZSTD: u16 = 0x0001;

#[derive(Debug, Error)]
pub enum FormatError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("invalid magic bytes: {0:?}")]
    BadMagic([u8; 8]),
    #[error("unsupported format version {0}")]
    BadVersion(u16),
    #[error("invalid column type byte {0}")]
    BadType(u8),
    #[error("malformed buffer: {0}")]
    Malformed(String),
    #[error("zstd error: {0}")]
    Zstd(String),
}

pub type Result<T> = std::result::Result<T, FormatError>;

// ---------------------------------------------------------------------------
// Byte buffer (de)serialization helpers
// ---------------------------------------------------------------------------

fn needs_zstd(value: &[u8]) -> bool {
    value.len() > 512
}

fn compress(data: &[u8]) -> Result<Vec<u8>> {
    #[cfg(feature = "zstd")]
    {
        zstd::bulk::compress(data, 3).map_err(|e| FormatError::Zstd(e.to_string()))
    }
    #[cfg(not(feature = "zstd"))]
    {
        let _ = data;
        Err(FormatError::Zstd(
            "zstd support not enabled (build with feature `zstd`)".into(),
        ))
    }
}

fn decompress(data: &[u8], expected_len: usize) -> Result<Vec<u8>> {
    #[cfg(feature = "zstd")]
    {
        zstd::bulk::decompress(data, expected_len).map_err(|e| FormatError::Zstd(e.to_string()))
    }
    #[cfg(not(feature = "zstd"))]
    {
        let _ = (data, expected_len);
        Err(FormatError::Zstd(
            "zstd support not enabled (build with feature `zstd`)".into(),
        ))
    }
}

fn maybe_compress(data: &[u8], compress_flag: bool) -> Result<Vec<u8>> {
    if compress_flag && needs_zstd(data) {
        compress(data)
    } else {
        Ok(data.to_vec())
    }
}

fn maybe_decompress(data: &[u8], expected_len: usize, was_compressed: bool) -> Result<Vec<u8>> {
    if was_compressed {
        decompress(data, expected_len)
    } else {
        Ok(data.to_vec())
    }
}

fn encode_dtype(data_type: DataType) -> u8 {
    data_type.to_byte()
}

fn append_u16(out: &mut Vec<u8>, v: u16) {
    out.extend_from_slice(&v.to_le_bytes());
}

fn append_u32(out: &mut Vec<u8>, v: u32) {
    out.extend_from_slice(&v.to_le_bytes());
}

fn append_u64(out: &mut Vec<u8>, v: u64) {
    out.extend_from_slice(&v.to_le_bytes());
}

fn read_u16(r: &mut &[u8], what: &str) -> Result<u16> {
    let bytes = read_n(r, 2, what)?;
    Ok(u16::from_le_bytes([bytes[0], bytes[1]]))
}

fn read_u32(r: &mut &[u8], what: &str) -> Result<u32> {
    let bytes = read_n(r, 4, what)?;
    Ok(u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
}

fn read_u64(r: &mut &[u8], what: &str) -> Result<u64> {
    let bytes = read_n(r, 8, what)?;
    Ok(u64::from_le_bytes(bytes.try_into().map_err(|_| {
        FormatError::Malformed(format!("{what}: expected 8 bytes"))
    })?))
}

fn read_n<'a>(r: &mut &'a [u8], n: usize, what: &str) -> Result<&'a [u8]> {
    if r.len() < n {
        return Err(FormatError::Malformed(format!(
            "truncated input while reading {what}"
        )));
    }
    let (head, tail) = r.split_at(n);
    *r = tail;
    Ok(head)
}

// ---------------------------------------------------------------------------
// Column buffer codecs
// ---------------------------------------------------------------------------

fn encode_buffer(column: &Column) -> Vec<u8> {
    let buf = column.buffer();
    match buf {
        crate::column::ColumnBuffer::Int32(v) => {
            let mut out = Vec::with_capacity(v.len() * 4);
            for x in v {
                out.extend_from_slice(&x.to_le_bytes());
            }
            out
        }
        crate::column::ColumnBuffer::Int64(v) => {
            let mut out = Vec::with_capacity(v.len() * 8);
            for x in v {
                out.extend_from_slice(&x.to_le_bytes());
            }
            out
        }
        crate::column::ColumnBuffer::Float32(v) => {
            let mut out = Vec::with_capacity(v.len() * 4);
            for x in v {
                out.extend_from_slice(&x.to_le_bytes());
            }
            out
        }
        crate::column::ColumnBuffer::Float64(v) => {
            let mut out = Vec::with_capacity(v.len() * 8);
            for x in v {
                out.extend_from_slice(&x.to_le_bytes());
            }
            out
        }
        crate::column::ColumnBuffer::Bool(v) => {
            let mut out = Vec::with_capacity(v.len());
            for x in v {
                out.push(u8::from(*x));
            }
            out
        }
        crate::column::ColumnBuffer::Date(v) => {
            let mut out = Vec::with_capacity(v.len() * 4);
            for x in v {
                out.extend_from_slice(&x.to_le_bytes());
            }
            out
        }
        crate::column::ColumnBuffer::Timestamp(v) => {
            let mut out = Vec::with_capacity(v.len() * 8);
            for x in v {
                out.extend_from_slice(&x.to_le_bytes());
            }
            out
        }
        crate::column::ColumnBuffer::Utf8(v) => {
            // offsets (row+1 entries) + string blob
            let mut offsets = Vec::with_capacity((v.len() + 1) * 8);
            let mut blob = Vec::new();
            let mut cursor: u64 = 0;
            offsets.extend_from_slice(&0u64.to_le_bytes());
            for s in v {
                blob.extend_from_slice(s.as_bytes());
                cursor += s.len() as u64;
                offsets.extend_from_slice(&cursor.to_le_bytes());
            }
            let mut out = Vec::with_capacity(offsets.len() + blob.len());
            out.extend_from_slice(&offsets);
            out.extend_from_slice(&blob);
            out
        }
    }
}

fn decode_buffer(
    data_type: DataType,
    rows: usize,
    data: &[u8],
    nulls: &[bool],
) -> Result<crate::column::ColumnBuffer> {
    let mut buffer = crate::column::ColumnBuffer::with_capacity(data_type, rows);
    for i in 0..rows {
        if nulls[i] {
            buffer.push_null();
            continue;
        }
        let value = match data_type {
            DataType::Int32 => {
                let slice = slice_chunk::<4>(data, i, rows, "int32")?;
                Value::Int32(i32::from_le_bytes(
                    slice
                        .try_into()
                        .map_err(|_| FormatError::Malformed("int32 slice length".into()))?,
                ))
            }
            DataType::Int64 => {
                let slice = slice_chunk::<8>(data, i, rows, "int64")?;
                Value::Int64(i64::from_le_bytes(
                    slice
                        .try_into()
                        .map_err(|_| FormatError::Malformed("int64 slice length".into()))?,
                ))
            }
            DataType::Float32 => {
                let slice = slice_chunk::<4>(data, i, rows, "float32")?;
                Value::Float32(f32::from_le_bytes(
                    slice
                        .try_into()
                        .map_err(|_| FormatError::Malformed("float32 slice length".into()))?,
                ))
            }
            DataType::Float64 => {
                let slice = slice_chunk::<8>(data, i, rows, "float64")?;
                Value::Float64(f64::from_le_bytes(
                    slice
                        .try_into()
                        .map_err(|_| FormatError::Malformed("float64 slice length".into()))?,
                ))
            }
            DataType::Bool => {
                if data.len() <= i {
                    return Err(FormatError::Malformed("bool data too short".into()));
                }
                Value::Bool(data[i] != 0)
            }
            DataType::Date => {
                let slice = slice_chunk::<4>(data, i, rows, "date")?;
                Value::Date(i32::from_le_bytes(
                    slice
                        .try_into()
                        .map_err(|_| FormatError::Malformed("date slice length".into()))?,
                ))
            }
            DataType::Timestamp => {
                let slice = slice_chunk::<8>(data, i, rows, "timestamp")?;
                Value::Timestamp(i64::from_le_bytes(
                    slice
                        .try_into()
                        .map_err(|_| FormatError::Malformed("timestamp slice length".into()))?,
                ))
            }
            DataType::Utf8 => decode_utf8(data, i, rows)?,
        };
        buffer.push_value(value);
    }
    Ok(buffer)
}

fn slice_chunk<'a, const N: usize>(
    data: &'a [u8],
    index: usize,
    rows: usize,
    what: &str,
) -> Result<&'a [u8]> {
    let expected = rows
        .checked_mul(N)
        .ok_or_else(|| FormatError::Malformed(format!("{what} size overflow")))?;
    if data.len() < expected {
        return Err(FormatError::Malformed(format!(
            "{what} buffer too short: {} bytes for {rows} rows",
            data.len()
        )));
    }
    let start = index * N;
    Ok(&data[start..start + N])
}

fn decode_utf8(data: &[u8], index: usize, rows: usize) -> Result<Value> {
    if data.len() < (rows + 1) * 8 {
        return Err(FormatError::Malformed(
            "utf8 offsets buffer too short".into(),
        ));
    }
    let start = u64::from_le_bytes(
        data[index * 8..index * 8 + 8]
            .try_into()
            .map_err(|_| FormatError::Malformed("utf8 start-offset slice length".into()))?,
    ) as usize;
    let end = u64::from_le_bytes(
        data[(index + 1) * 8..(index + 1) * 8 + 8]
            .try_into()
            .map_err(|_| FormatError::Malformed("utf8 end-offset slice length".into()))?,
    ) as usize;
    let blob_start = (rows + 1) * 8;
    if end < start || blob_start + end > data.len() {
        return Err(FormatError::Malformed("utf8 offset out of range".into()));
    }
    let bytes = &data[blob_start + start..blob_start + end];
    let s = String::from_utf8_lossy(bytes).into_owned();
    Ok(Value::Utf8(s))
}

// ---------------------------------------------------------------------------
// Chunk (single RecordBatch) encoding
// ---------------------------------------------------------------------------

pub fn encode_batch(batch: &RecordBatch, use_zstd: bool) -> Result<Vec<u8>> {
    let used_zstd = use_zstd && batch_has_big_buffers(batch);
    let out = encode_chunk(batch, used_zstd)?;
    Ok(out)
}

fn batch_has_big_buffers(batch: &RecordBatch) -> bool {
    batch.columns().iter().any(|c| {
        let nulls = c.len();
        let data = match c.buffer() {
            crate::column::ColumnBuffer::Int32(v) => v.len() * 4,
            crate::column::ColumnBuffer::Int64(v) => v.len() * 8,
            crate::column::ColumnBuffer::Float32(v) => v.len() * 4,
            crate::column::ColumnBuffer::Float64(v) => v.len() * 8,
            crate::column::ColumnBuffer::Bool(v) => v.len(),
            crate::column::ColumnBuffer::Utf8(v) => v.len() * 8,
            crate::column::ColumnBuffer::Date(v) => v.len() * 4,
            crate::column::ColumnBuffer::Timestamp(v) => v.len() * 8,
        };
        nulls > 512 || data > 512
    })
}

fn encode_chunk(batch: &RecordBatch, use_zstd: bool) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    let flags = if use_zstd { FLAG_ZSTD } else { 0 };
    out.extend_from_slice(&MAGIC);
    append_u16(&mut out, VERSION);
    append_u16(&mut out, flags);
    append_u32(&mut out, batch.num_columns() as u32);
    append_u64(&mut out, batch.num_rows() as u64);

    for column in batch.columns() {
        let name = column.name().as_bytes();
        if name.len() > u16::MAX as usize {
            return Err(FormatError::Malformed("column name too long".into()));
        }
        let nulls = encode_nulls(column);
        let data = encode_buffer(column);

        append_u16(&mut out, name.len() as u16);
        out.extend_from_slice(name);
        out.push(encode_dtype(column.data_type()));

        let null_len = nulls.len() as u64;
        let null_stored = if use_zstd && needs_zstd(&nulls) {
            compress(&nulls)?
        } else {
            nulls
        };
        let data_bytes = maybe_compress(&data, use_zstd)?;

        append_u64(&mut out, null_len);
        append_u64(&mut out, null_stored.len() as u64);
        out.extend_from_slice(&null_stored);
        append_u64(&mut out, data.len() as u64);
        append_u64(&mut out, data_bytes.len() as u64);
        out.extend_from_slice(&data_bytes);
    }
    Ok(out)
}

fn encode_nulls(column: &Column) -> Vec<u8> {
    let mut out = Vec::with_capacity(column.len());
    for i in 0..column.len() {
        out.push(u8::from(column.is_null(i)));
    }
    out
}

pub fn decode_batch(bytes: &[u8]) -> Result<RecordBatch> {
    let mut r = bytes;
    let magic: [u8; 8] = read_n(&mut r, 8, "magic")?
        .try_into()
        .map_err(|_| FormatError::Malformed("magic: expected 8 bytes".into()))?;
    if magic != MAGIC {
        return Err(FormatError::BadMagic(magic));
    }
    let version = read_u16(&mut r, "version")?;
    if version != VERSION {
        return Err(FormatError::BadVersion(version));
    }
    let flags = read_u16(&mut r, "flags")?;
    let was_compressed = flags & FLAG_ZSTD != 0;
    let num_columns = read_u32(&mut r, "num_columns")? as usize;
    let num_rows = read_u64(&mut r, "num_rows")? as usize;

    let mut columns = Vec::with_capacity(num_columns);
    for _ in 0..num_columns {
        let name_len = read_u16(&mut r, "name_len")? as usize;
        let name_bytes = read_n(&mut r, name_len, "column name")?;
        let name = String::from_utf8_lossy(name_bytes).into_owned();
        let type_byte = read_n(&mut r, 1, "column type")?[0];
        let data_type = DataType::from_byte(type_byte).ok_or(FormatError::BadType(type_byte))?;

        let null_len = read_u64(&mut r, "null_len")? as usize;
        let null_stored_len = read_u64(&mut r, "null_bytes_len")? as usize;
        let null_stored = read_n(&mut r, null_stored_len, "null bytes")?;
        let null_bytes = maybe_decompress(null_stored, null_len, was_compressed)?;
        let nulls: Vec<bool> = if null_len == 0 {
            Vec::with_capacity(0)
        } else {
            null_bytes.iter().map(|b| *b != 0).collect()
        };

        let data_len = read_u64(&mut r, "data_len")? as usize;
        let data_stored_len = read_u64(&mut r, "data_bytes_len")? as usize;
        let data_stored = read_n(&mut r, data_stored_len, "data bytes")?;
        let data_bytes = maybe_decompress(data_stored, data_len, was_compressed)?;

        let buffer = decode_buffer(data_type, num_rows, &data_bytes, &nulls)?;
        columns.push(Column::from_data(name, data_type, buffer, nulls));
    }

    Ok(RecordBatch::new(columns))
}

// ---------------------------------------------------------------------------
// Chunked streaming reader / writer over a byte stream
// ---------------------------------------------------------------------------

pub struct ChunkedWriter<W: Write> {
    inner: W,
    compress: bool,
    batches: u64,
}

impl<W: Write> ChunkedWriter<W> {
    pub fn new(inner: W, compress: bool) -> Self {
        ChunkedWriter {
            inner,
            compress,
            batches: 0,
        }
    }

    pub fn batches_written(&self) -> u64 {
        self.batches
    }

    pub fn write_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        let bytes = encode_batch(batch, self.compress)?;
        self.inner.write_all(&bytes)?;
        self.batches += 1;
        Ok(())
    }

    pub fn finish(mut self) -> Result<W> {
        self.inner.flush()?;
        Ok(self.inner)
    }
}

pub struct ChunkedReader<R: Read> {
    inner: R,
    done: bool,
}

impl<R: Read> ChunkedReader<R> {
    pub fn new(inner: R) -> Self {
        ChunkedReader { inner, done: false }
    }

    fn read_exact_or_eof(&mut self, buf: &mut [u8]) -> Result<bool> {
        let mut filled = 0;
        while filled < buf.len() {
            match self.inner.read(&mut buf[filled..]) {
                Ok(0) => {
                    if filled == 0 {
                        return Ok(false);
                    }
                    return Err(FormatError::Malformed("unexpected EOF mid-chunk".into()));
                }
                Ok(n) => filled += n,
                Err(e) => {
                    if e.kind() == std::io::ErrorKind::Interrupted {
                        continue;
                    }
                    return Err(FormatError::Io(e));
                }
            }
        }
        Ok(true)
    }

    fn read_exact(&mut self, buf: &mut [u8]) -> Result<()> {
        if !self.read_exact_or_eof(buf)? {
            return Err(FormatError::Malformed("unexpected EOF".into()));
        }
        Ok(())
    }

    pub fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        if self.done {
            return Ok(None);
        }

        // Peek the magic; EOF at a chunk boundary ends the stream.
        let mut magic = [0u8; 8];
        if !self.read_exact_or_eof(&mut magic)? {
            self.done = true;
            return Ok(None);
        }
        if magic != MAGIC {
            // Rewind is not possible on a raw stream; but a corrupted prefix is fatal.
            return Err(FormatError::BadMagic(magic));
        }

        let mut header = [0u8; 4];
        self.read_exact(&mut header)?;
        let version = u16::from_le_bytes([header[0], header[1]]);
        let flags = u16::from_le_bytes([header[2], header[3]]);
        if version != VERSION {
            return Err(FormatError::BadVersion(version));
        }
        let was_compressed = flags & FLAG_ZSTD != 0;

        let mut counts = [0u8; 12];
        self.read_exact(&mut counts)?;
        let num_columns = u32::from_le_bytes([counts[0], counts[1], counts[2], counts[3]]) as usize;
        let num_rows = u64::from_le_bytes([
            counts[4], counts[5], counts[6], counts[7], counts[8], counts[9], counts[10],
            counts[11],
        ]) as usize;

        let mut columns = Vec::with_capacity(num_columns);
        for _ in 0..num_columns {
            let mut name_len_bytes = [0u8; 2];
            self.read_exact(&mut name_len_bytes)?;
            let name_len = u16::from_le_bytes(name_len_bytes) as usize;
            let mut name_bytes = vec![0u8; name_len];
            self.read_exact(&mut name_bytes)?;
            let name = String::from_utf8_lossy(&name_bytes).into_owned();

            let mut type_byte = [0u8; 1];
            self.read_exact(&mut type_byte)?;
            let data_type = DataType::from_byte(type_byte[0])
                .ok_or_else(|| FormatError::BadType(type_byte[0]))?;

            let null_len = self.read_u64()? as usize;
            let null_stored_len = self.read_u64()? as usize;
            let mut null_stored = vec![0u8; null_stored_len];
            self.read_exact(&mut null_stored)?;
            let null_bytes = maybe_decompress(&null_stored, null_len, was_compressed)?;
            let nulls: Vec<bool> = null_bytes.iter().map(|b| *b != 0).collect();

            let data_len = self.read_u64()? as usize;
            let data_stored_len = self.read_u64()? as usize;
            let mut data_stored = vec![0u8; data_stored_len];
            self.read_exact(&mut data_stored)?;
            let data_bytes = maybe_decompress(&data_stored, data_len, was_compressed)?;

            let buffer = decode_buffer(data_type, num_rows, &data_bytes, &nulls)?;
            columns.push(Column::from_data(name, data_type, buffer, nulls));
        }

        Ok(Some(RecordBatch::new(columns)))
    }

    fn read_u64(&mut self) -> Result<u64> {
        let mut b = [0u8; 8];
        self.read_exact(&mut b)?;
        Ok(u64::from_le_bytes(b))
    }
}

pub fn encode_batch_reader<R: Read>(inner: R) -> ChunkedReader<R> {
    ChunkedReader::new(inner)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::column::Column;
    use crate::value::{DataType, Value};

    fn sample_batch() -> RecordBatch {
        let mut id = Column::new("id", DataType::Int32, 4);
        id.push(Value::Int32(1));
        id.push(Value::Int32(2));
        id.push(Value::Null);
        id.push(Value::Int32(-7));
        let mut price = Column::new("price", DataType::Float64, 4);
        price.push(Value::Float64(1.5));
        price.push(Value::Null);
        price.push(Value::Float64(3.25));
        price.push(Value::Float64(-0.5));
        let mut name = Column::new("name", DataType::Utf8, 4);
        name.push(Value::Utf8("héllo".into()));
        name.push(Value::Utf8("".into()));
        name.push(Value::Null);
        name.push(Value::Utf8("d".into()));
        let mut ok = Column::new("ok", DataType::Bool, 4);
        ok.push(Value::Bool(true));
        ok.push(Value::Bool(false));
        ok.push(Value::Bool(true));
        ok.push(Value::Null);
        RecordBatch::new(vec![id, price, name, ok])
    }

    fn roundtrip(compress: bool) {
        let batch = sample_batch();
        let bytes = encode_batch(&batch, compress).unwrap();
        let decoded = decode_batch(&bytes).unwrap();
        assert_eq!(decoded.num_rows(), batch.num_rows());
        assert_eq!(decoded.schema(), batch.schema());
        for i in 0..batch.num_rows() {
            for col in batch.column_names() {
                assert_eq!(
                    decoded.cell(i, col),
                    batch.cell(i, col),
                    "row {i} col {col} mismatch"
                );
            }
        }
    }

    #[test]
    fn roundtrip_uncompressed() {
        roundtrip(false);
    }

    #[test]
    #[cfg(feature = "zstd")]
    fn roundtrip_zstd() {
        roundtrip(true);
    }

    #[test]
    fn chunked_stream_roundtrip() {
        let mut data = Vec::new();
        {
            let mut writer = ChunkedWriter::new(&mut data, false);
            for _ in 0..3 {
                writer.write_batch(&sample_batch()).unwrap();
            }
            writer.finish().unwrap();
        }
        let mut reader = ChunkedReader::new(&data[..]);
        let mut count = 0;
        while let Some(b) = reader.next_batch().unwrap() {
            assert_eq!(b.num_rows(), 4);
            count += 1;
        }
        assert_eq!(count, 3);
        assert!(reader.next_batch().unwrap().is_none());
    }

    #[test]
    #[cfg(feature = "zstd")]
    fn chunked_stream_roundtrip_zstd() {
        let mut data = Vec::new();
        {
            let mut writer = ChunkedWriter::new(&mut data, true);
            writer.write_batch(&sample_batch()).unwrap();
            writer.finish().unwrap();
        }
        let mut reader = ChunkedReader::new(&data[..]);
        let b = reader.next_batch().unwrap().unwrap();
        assert_eq!(b.num_rows(), 4);
        assert_eq!(b.cell(0, "name"), Some(Value::Utf8("héllo".into())));
        assert_eq!(b.cell(2, "name"), Some(Value::Null));
    }

    #[test]
    fn bad_magic_rejected() {
        let err = decode_batch(b"NOTTPTCOL").unwrap_err();
        assert!(matches!(err, FormatError::BadMagic(_)));
    }

    // --- Corrupt / truncated input regression tests -------------------------

    #[test]
    fn empty_input_is_clean_error() {
        let err = decode_batch(b"").unwrap_err();
        assert!(
            matches!(err, FormatError::Malformed(_)),
            "expected Malformed, got {err:?}"
        );
    }

    #[test]
    fn truncated_after_magic_is_clean_error() {
        // Only the magic bytes — no version/flags/counts.
        let err = decode_batch(&MAGIC).unwrap_err();
        assert!(
            matches!(err, FormatError::Malformed(_)),
            "expected Malformed, got {err:?}"
        );
    }

    #[test]
    fn truncated_mid_data_is_clean_error() {
        let batch = sample_batch();
        let mut bytes = encode_batch(&batch, false).unwrap();
        // Lop off the last 20 bytes to simulate truncation mid-data.
        let trunc_len = bytes.len().saturating_sub(20);
        bytes.truncate(trunc_len);
        let err = decode_batch(&bytes).unwrap_err();
        assert!(
            matches!(err, FormatError::Malformed(_) | FormatError::Io(_)),
            "expected Malformed or Io, got {err:?}"
        );
    }

    #[test]
    fn bad_version_is_clean_error() {
        let batch = sample_batch();
        let mut bytes = encode_batch(&batch, false).unwrap();
        // Version lives at bytes 8-9 (after the 8-byte magic); set it to 0xFF.
        if bytes.len() > 9 {
            bytes[8] = 0xFF;
            bytes[9] = 0xFF;
        }
        let err = decode_batch(&bytes).unwrap_err();
        assert!(
            matches!(err, FormatError::BadVersion(_)),
            "expected BadVersion, got {err:?}"
        );
    }

    #[test]
    fn chunked_reader_truncated_is_clean_error() {
        let mut data = Vec::new();
        {
            let mut writer = ChunkedWriter::new(&mut data, false);
            writer.write_batch(&sample_batch()).unwrap();
            writer.finish().unwrap();
        }
        // Truncate inside the first chunk's data.
        let trunc_len = data.len() / 2;
        let truncated = &data[..trunc_len];
        let mut reader = ChunkedReader::new(truncated);
        let result = reader.next_batch();
        assert!(
            result.is_err(),
            "expected Err on truncated chunked input, got Ok"
        );
    }

    #[test]
    fn all_types_roundtrip() {
        for dtype in DataType::ALL {
            let mut col = Column::new("col", dtype, 3);
            col.push(match dtype {
                DataType::Int32 => Value::Int32(42),
                DataType::Int64 => Value::Int64(42),
                DataType::Float32 => Value::Float32(1.25),
                DataType::Float64 => Value::Float64(1.25),
                DataType::Utf8 => Value::Utf8("text".into()),
                DataType::Bool => Value::Bool(true),
                DataType::Date => Value::Date(19_782),
                DataType::Timestamp => Value::Timestamp(1_709_209_859_000_000),
            });
            col.push(Value::Null);
            col.push(match dtype {
                DataType::Int32 => Value::Int32(-9),
                DataType::Int64 => Value::Int64(-9),
                DataType::Float32 => Value::Float32(-9.5),
                DataType::Float64 => Value::Float64(-9.5),
                DataType::Utf8 => Value::Utf8("第二".into()),
                DataType::Bool => Value::Bool(false),
                DataType::Date => Value::Date(-25_567),
                DataType::Timestamp => Value::Timestamp(-1),
            });
            let batch = RecordBatch::new(vec![col]);
            let bytes = encode_batch(&batch, false).unwrap();
            let decoded = decode_batch(&bytes).unwrap();
            assert_eq!(decoded.num_rows(), 3);
            assert_eq!(decoded.cell(0, "col"), batch.cell(0, "col"));
            assert_eq!(decoded.cell(1, "col"), Some(Value::Null));
            assert_eq!(decoded.cell(2, "col"), batch.cell(2, "col"));
        }
    }
}
