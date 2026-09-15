# tpt-stream-columnar

Minimal columnar data format for tpt-streamforge. Holds the fundamental
in-memory types (`DataType`, `Value`, `Column`, `RecordBatch`) and defines the
`.tptcol` on-disk chunk format plus streaming chunked readers/writers.

## In-memory model

- `DataType`: `Int32 | Int64 | Float32 | Float64 | Utf8 | Bool`
- `Value`: a cell value plus `Null`
- `Column`: a typed `Vec<T>` buffer plus a `Vec<bool>` null bitmap
- `RecordBatch`: a set of equal-length `Column`s with a name index

Column-oriented storage means a whole column shrinks/grows as one array
(`swap_remove`, `retain_rows`, `append_column`) instead of per-row mutation, and
serialization is a memcpy-friendly buffer loop.

## `.tptcol` chunk layout

A file is a sequence of chunks. Each chunk holds exactly one `RecordBatch`, so a
file with N chunks is an N-batch stream with constant memory usage. Reading is a
single pass; the reader treats EOF at a chunk boundary as end-of-stream.

Fixed header (24 bytes):

```
magic        [8]     b"TPTCOL1\x00"
version      u16     1
flags        u16     bit 0 = zstd compressed in this chunk
num_columns  u32
num_rows     u64     rows in this chunk
```

Per column (repeated for each column):

```
name_len     u16
name         [name_len]  UTF-8
type         u8          DataType::to_byte()
null_len     u64         uncompressed null-bitmap length (1 byte per row)
null_bytes_len  u64      stored length after (optional) compression
null_bytes   [null_bytes_len]
data_len     u64         uncompressed data buffer length
data_bytes_len u64       stored length after (optional) compression
data_bytes   [data_bytes_len]
```

Data buffer encodings (little-endian):

- `Int32`   : 4 bytes per value
- `Int64`   : 8 bytes per value
- `Float32` : 4 bytes per value
- `Float64` : 8 bytes per value
- `Bool`    : 1 byte per value (0/1)
- `Utf8`    : `(rows + 1)` u64 offets (cumulative byte length per row) followed
  by the concatenated string blob

Nulls live in the 1-byte-per-row null bitmap; numeric/bool/utf8 buffers still
allocate a sentinel slot per row so column arrays never shrink per-row.

## Compression

`zstd` is optional and feature-gated. When enabled and a buffer exceeds 512
bytes, `encode_batch` compresses the null bitmap and the data payload with
`zstd::bulk::compress` (level 3) and sets the chunk `flags` bit 0. The chunk
flag is only set when at least one buffer was actually compressed, so a decoder
never guesses. Decompression uses the stored uncompressed lengths as the
capacity hint.

Round-trip is lossless for all six types including unicode strings and nulls.

## API

- `encode_batch(&RecordBatch, use_zstd: bool) -> Vec<u8>`
- `decode_batch(&[u8]) -> RecordBatch`
- `ChunkedWriter<W: Write>` — `write_batch`, `finish`, `batches_written`
- `ChunkedReader<R: Read>` — streaming `next_batch() -> Option<RecordBatch>`

The `.tptcol`/zstd source+sink in `tpt-stream-core` build `Pipeline
read_columnar`/`write_columnar` on top of these primitives.