//! tpt-streamforge Python wrapper (PyO3).
//!
//! Exposes a fluent `Pipeline` class: every stage builder returns the same
//! `Pipeline` instance so calls can be chained, e.g.
//!
//! ```python
//! from tpt_streamforge import Pipeline
//! stats = (
//!     Pipeline()
//!     .read_csv("in.csv")
//!     .filter("score > 60")
//!     .map({"label": "upper(name)"})
//!     .write_csv("out.csv")
//!     .execute()
//! )
//! ```

use std::sync::{Arc, Mutex};

use pyo3::create_exception;
use pyo3::exceptions::PyException;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};

use tpt_stream_core::agg::AggSpec;
use tpt_stream_core::join::JoinType;
use tpt_stream_core::source::ErrorPolicy;
use tpt_stream_core::{Check, Pipeline};

create_exception!(tpt_streamforge, TptError, PyException);

fn to_pyerr(err: tpt_stream_core::Error) -> PyErr {
    TptError::new_err(err.to_string())
}

/// Wrap any Python-side error as a `TptError`.
fn py_err(err: PyErr) -> PyErr {
    TptError::new_err(err.to_string())
}

/// Fail fast if the last expression-stage call produced a parse error.
fn check_pending(pipeline: &Pipeline) -> PyResult<()> {
    if let Some(msg) = pipeline.pending_error() {
        return Err(TptError::new_err(msg.to_string()));
    }
    Ok(())
}

/// Serialize one telemetry event into a plain dict:
/// `{"event": ..., "rows": ..., ...}` (keys depend on the event kind).
fn event_to_dict(py: Python<'_>, event: &tpt_stream_core::TelemetryEvent) -> Py<PyDict> {
    use tpt_stream_core::TelemetryEvent::*;
    let dict = PyDict::new(py);
    match event {
        SourceBatch {
            rows,
            total_rows,
            batches,
        } => {
            let _ = dict.set_item("event", "source_batch");
            let _ = dict.set_item("rows", rows);
            let _ = dict.set_item("total_rows", total_rows);
            let _ = dict.set_item("batches", batches);
        }
        StageBatch {
            stage,
            rows_in,
            rows_out,
        } => {
            let _ = dict.set_item("event", "stage_batch");
            let _ = dict.set_item("stage", stage);
            let _ = dict.set_item("rows_in", rows_in);
            let _ = dict.set_item("rows_out", rows_out);
        }
        SinkBatch { rows, total_rows } => {
            let _ = dict.set_item("event", "sink_batch");
            let _ = dict.set_item("rows", rows);
            let _ = dict.set_item("total_rows", total_rows);
        }
        Done {
            rows,
            batches,
            elapsed,
        } => {
            let _ = dict.set_item("event", "done");
            let _ = dict.set_item("rows", rows);
            let _ = dict.set_item("batches", batches);
            let _ = dict.set_item("elapsed_ms", elapsed.as_secs_f64() * 1000.0);
        }
    }
    dict.unbind()
}

/// Parse `strict | skip | quarantine:<path>` into an [`ErrorPolicy`].
fn parse_error_policy(text: &str) -> PyResult<ErrorPolicy> {
    if text == "strict" {
        Ok(ErrorPolicy::Strict)
    } else if text == "skip" {
        Ok(ErrorPolicy::Skip)
    } else if let Some(path) = text.strip_prefix("quarantine:") {
        Ok(ErrorPolicy::Quarantine(path.to_string()))
    } else {
        Err(TptError::new_err(
            "error policy must be 'strict', 'skip', or 'quarantine:<path>'",
        ))
    }
}

/// Convert one core value into a Python object.
fn value_to_py(py: Python<'_>, value: &tpt_stream_core::Value) -> PyResult<PyObject> {
    use tpt_stream_core::Value;
    Ok(match value {
        Value::Null => py.None(),
        Value::Bool(b) => b
            .into_pyobject(py)
            .map(|o| o.to_owned().unbind().into_any())?,
        Value::Int32(v) => v
            .into_pyobject(py)
            .map(|o| o.to_owned().unbind().into_any())?,
        Value::Int64(v) => v
            .into_pyobject(py)
            .map(|o| o.to_owned().unbind().into_any())?,
        Value::Float32(v) => (*v as f64)
            .into_pyobject(py)
            .map(|o| o.to_owned().unbind().into_any())?,
        Value::Float64(v) => v
            .into_pyobject(py)
            .map(|o| o.to_owned().unbind().into_any())?,
        // Dates/timestamps surface as ISO strings.
        Value::Date(_) | Value::Timestamp(_) => value
            .to_string()
            .into_pyobject(py)
            .map(|o| o.to_owned().unbind().into_any())?,
        Value::Utf8(s) => s
            .into_pyobject(py)
            .map(|o| o.to_owned().unbind().into_any())?,
    })
}

#[pyclass(name = "Pipeline")]
struct PyPipeline {
    inner: Mutex<Pipeline>,
    progress: Mutex<Option<PyObject>>,
}

impl PyPipeline {
    /// Run the pipeline, holding the output in memory (used by `collect`,
    /// `to_arrow`, and `to_pandas`). Consumes the pipeline like `execute`.
    fn collect_batches(&self, py: Python<'_>) -> PyResult<Vec<tpt_stream_core::RecordBatch>> {
        {
            let inner = self.inner.lock().unwrap();
            check_pending(&inner)?;
        }
        let callback = self
            .progress
            .lock()
            .unwrap()
            .as_ref()
            .map(|c| c.clone_ref(py));
        if let Some(callback) = callback {
            let mut inner = self.inner.lock().unwrap();
            inner.on_progress(Arc::new(move |event| {
                Python::with_gil(|py| {
                    let dict = event_to_dict(py, event);
                    if let Err(err) = callback.call1(py, (dict,)) {
                        eprintln!("tpt_streamforge: progress callback raised: {err}");
                    }
                });
            }));
        }
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| TptError::new_err(format!("failed to build runtime: {e}")))?;
        py.allow_threads(|| {
            let mut inner = self.inner.lock().unwrap();
            runtime.block_on(inner.collect())
        })
        .map_err(to_pyerr)
    }

    fn map_exprs(&self, mapping: &Bound<'_, PyDict>) -> PyResult<()> {
        let mut pairs: Vec<(String, String)> = Vec::new();
        for (k, v) in mapping.iter() {
            let col = k.extract::<String>()?;
            let expr = v.extract::<String>()?;
            pairs.push((col, expr));
        }
        let mut inner = self.inner.lock().expect("pipeline mutex poisoned");
        let pair_refs: Vec<(&str, &str)> = pairs
            .iter()
            .map(|(a, b)| (a.as_str(), b.as_str()))
            .collect();
        inner.map_expr(&pair_refs);
        check_pending(&inner)
    }
}

#[pymethods]
impl PyPipeline {
    #[new]
    fn new() -> Self {
        PyPipeline {
            inner: Mutex::new(Pipeline::new()),
            progress: Mutex::new(None),
        }
    }

    /// Register a progress callback: `callback(event_dict)` is invoked for
    /// every source batch, stage output, sink write, and at completion.
    /// Event kinds: `source_batch`, `stage_batch`, `sink_batch`, `done`.
    fn on_progress(slf: Bound<'_, Self>, callback: PyObject) -> PyResult<Py<Self>> {
        {
            let cell = slf.borrow_mut();
            *cell.progress.lock().unwrap() = Some(callback);
        }
        Ok(slf.unbind())
    }

    /// Per-stage cumulative metrics from the last `execute()` run: a list of
    /// dicts with `name`, `rows_in`, `rows_out`, `batches`, `elapsed_ms`,
    /// `rows_per_sec` (empty before the first run).
    fn stage_stats(&self, py: Python<'_>) -> PyResult<Py<PyList>> {
        let inner = self.inner.lock().unwrap();
        let stats: Vec<Bound<'_, PyDict>> = inner
            .stage_stats()
            .iter()
            .map(|m| {
                let d = PyDict::new(py);
                d.set_item("name", m.name.clone())?;
                d.set_item("rows_in", m.rows_in)?;
                d.set_item("rows_out", m.rows_out)?;
                d.set_item("batches", m.batches)?;
                d.set_item("elapsed_ms", m.elapsed.as_secs_f64() * 1000.0)?;
                d.set_item("rows_per_sec", m.rows_per_sec())?;
                Ok::<_, PyErr>(d)
            })
            .collect::<PyResult<_>>()?;
        Ok(PyList::new(py, stats)?.unbind())
    }

    /// Read a CSV file. `chunk_size` rows are buffered per batch (0 = default).
    #[pyo3(signature = (path, chunk_size=None))]
    fn read_csv(slf: Bound<'_, Self>, path: &str, chunk_size: Option<usize>) -> PyResult<Py<Self>> {
        {
            let cell = slf.borrow_mut();
            let mut inner = cell.inner.lock().unwrap();
            if let Some(n) = chunk_size {
                inner.with_chunk_size(n);
            }
            check_pending(&inner)?;
            inner.read_csv(path);
            check_pending(&inner)?;
        }
        Ok(slf.unbind())
    }

    /// Keep only rows where `expr` evaluates to boolean true.
    fn filter(slf: Bound<'_, Self>, expr: &str) -> PyResult<Py<Self>> {
        {
            let cell = slf.borrow_mut();
            let mut inner = cell.inner.lock().unwrap();
            inner.filter_expr(expr);
            check_pending(&inner)?;
        }
        Ok(slf.unbind())
    }

    /// Project rows to `{output_column: expression}`; the row schema is
    /// replaced with exactly the mapped columns.
    fn map(slf: Bound<'_, Self>, mapping: &Bound<'_, PyDict>) -> PyResult<Py<Self>> {
        {
            let cell = slf.borrow_mut();
            cell.map_exprs(mapping)?;
        }
        Ok(slf.unbind())
    }

    /// Group by `columns`, then apply `.agg({'column': 'fn'})` on the returned
    /// group-by handle (fns: sum, avg, count, count_all, min, max).
    fn group_by<'py>(
        slf: Bound<'py, Self>,
        columns: &Bound<'_, PyList>,
    ) -> PyResult<Py<PyGroupBy>> {
        let cols: Vec<String> = columns.extract()?;
        let group_by = PyGroupBy {
            pipeline: slf.clone().unbind(),
            columns: cols,
        };
        Py::new(slf.py(), group_by)
    }

    /// Sort rows by `columns` (ascending unless `descending=True`).
    #[pyo3(signature = (columns, descending=false))]
    fn sort(
        slf: Bound<'_, Self>,
        columns: &Bound<'_, PyList>,
        descending: bool,
    ) -> PyResult<Py<Self>> {
        let cols: Vec<String> = columns.extract()?;
        {
            let cell = slf.borrow_mut();
            let mut inner = cell.inner.lock().unwrap();
            let col_refs: Vec<&str> = cols.iter().map(|s| s.as_str()).collect();
            if descending {
                inner.sort_by_desc(&col_refs);
            } else {
                inner.sort_by(&col_refs);
            }
        }
        Ok(slf.unbind())
    }

    /// Drop duplicate rows over `columns`.
    fn dedup(slf: Bound<'_, Self>, columns: &Bound<'_, PyList>) -> PyResult<Py<Self>> {
        let cols: Vec<String> = columns.extract()?;
        {
            let cell = slf.borrow_mut();
            let mut inner = cell.inner.lock().unwrap();
            let col_refs: Vec<&str> = cols.iter().map(|s| s.as_str()).collect();
            inner.dedup(&col_refs);
        }
        Ok(slf.unbind())
    }

    /// Write results to a CSV file (posix newlines, headers included).
    fn write_csv(slf: Bound<'_, Self>, path: &str) -> PyResult<Py<Self>> {
        {
            let cell = slf.borrow_mut();
            let mut inner = cell.inner.lock().unwrap();
            inner.write_csv(path);
            check_pending(&inner)?;
        }
        Ok(slf.unbind())
    }

    /// Run the pipeline. Returns a dict with `rows`, `batches`, `bytes_in`,
    /// `bytes_out`.
    fn execute(&self, py: Python<'_>) -> PyResult<Py<PyDict>> {
        // Scope the guard: the run below re-locks the (non-reentrant) mutex
        // after the GIL is released.
        {
            let mut inner = self.inner.lock().unwrap();
            check_pending(&inner)?;
            // Attach the Python progress callback (if registered) as a
            // telemetry hook; the GIL is held here, so re-entering Python
            // from the hook is safe.
            let callback = self
                .progress
                .lock()
                .unwrap()
                .as_ref()
                .map(|c| c.clone_ref(py));
            if let Some(callback) = callback {
                inner.on_progress(Arc::new(move |event| {
                    Python::with_gil(|py| {
                        let dict = event_to_dict(py, event);
                        if let Err(err) = callback.call1(py, (dict,)) {
                            // A raising callback must not abort the pipeline;
                            // surface the error and keep streaming.
                            eprintln!("tpt_streamforge: progress callback raised: {err}");
                        }
                    });
                }));
            }
        }
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| TptError::new_err(format!("failed to build runtime: {e}")))?;
        // Release the GIL so other Python threads keep running while the
        // pipeline streams. The progress hook re-acquires it per event.
        let stats = py
            .allow_threads(|| {
                let mut inner = self.inner.lock().unwrap();
                runtime.block_on(inner.execute())
            })
            .map_err(to_pyerr)?;
        let out = PyDict::new(py);
        out.set_item("rows", stats.rows)?;
        out.set_item("batches", stats.batches)?;
        out.set_item("bytes_in", stats.bytes_in)?;
        out.set_item("bytes_out", stats.bytes_out)?;
        Ok(out.unbind())
    }

    /// Project rows to the given columns.
    fn select(slf: Bound<'_, Self>, columns: &Bound<'_, PyList>) -> PyResult<Py<Self>> {
        let cols: Vec<String> = columns.extract()?;
        {
            let cell = slf.borrow_mut();
            let mut inner = cell.inner.lock().unwrap();
            let col_refs: Vec<&str> = cols.iter().map(|s| s.as_str()).collect();
            inner.select(&col_refs);
        }
        Ok(slf.unbind())
    }

    /// Hash join against a CSV file on disk: `join_type` is `"inner"`,
    /// `"left"`, or `"right"`.
    #[pyo3(signature = (right_path, left_keys, right_keys, join_type="inner"))]
    fn join_csv(
        slf: Bound<'_, Self>,
        right_path: &str,
        left_keys: &Bound<'_, PyList>,
        right_keys: &Bound<'_, PyList>,
        join_type: &str,
    ) -> PyResult<Py<Self>> {
        let join_type = match join_type {
            "inner" => JoinType::Inner,
            "left" => JoinType::Left,
            "right" => JoinType::Right,
            other => {
                return Err(TptError::new_err(format!(
                    "join_type must be inner|left|right, got {other:?}"
                )))
            }
        };
        let left: Vec<String> = left_keys.extract()?;
        let right: Vec<String> = right_keys.extract()?;
        {
            let cell = slf.borrow_mut();
            let mut inner = cell.inner.lock().unwrap();
            let l: Vec<&str> = left.iter().map(|s| s.as_str()).collect();
            let r: Vec<&str> = right.iter().map(|s| s.as_str()).collect();
            inner
                .join_csv(right_path, &l, &r, join_type)
                .map_err(to_pyerr)?;
        }
        Ok(slf.unbind())
    }

    /// Read newline-delimited JSON.
    #[pyo3(signature = (path, chunk_size=None))]
    fn read_jsonl(
        slf: Bound<'_, Self>,
        path: &str,
        chunk_size: Option<usize>,
    ) -> PyResult<Py<Self>> {
        {
            let cell = slf.borrow_mut();
            let mut inner = cell.inner.lock().unwrap();
            if let Some(n) = chunk_size {
                inner.with_chunk_size(n);
            }
            inner.read_jsonl(path);
        }
        Ok(slf.unbind())
    }

    /// Read a top-level JSON array of objects.
    #[pyo3(signature = (path, chunk_size=None))]
    fn read_json(
        slf: Bound<'_, Self>,
        path: &str,
        chunk_size: Option<usize>,
    ) -> PyResult<Py<Self>> {
        {
            let cell = slf.borrow_mut();
            let mut inner = cell.inner.lock().unwrap();
            if let Some(n) = chunk_size {
                inner.with_chunk_size(n);
            }
            inner.read_json(path);
        }
        Ok(slf.unbind())
    }

    /// Write results to newline-delimited JSON.
    fn write_jsonl(slf: Bound<'_, Self>, path: &str) -> PyResult<Py<Self>> {
        {
            let cell = slf.borrow_mut();
            cell.inner.lock().unwrap().write_jsonl(path);
        }
        Ok(slf.unbind())
    }

    /// Write results to a JSON array; `pretty=True` adds indentation.
    #[pyo3(signature = (path, pretty=false))]
    fn write_json(slf: Bound<'_, Self>, path: &str, pretty: bool) -> PyResult<Py<Self>> {
        {
            let cell = slf.borrow_mut();
            cell.inner.lock().unwrap().write_json(path, pretty);
        }
        Ok(slf.unbind())
    }

    /// Read a native `.tptcol` columnar file.
    fn read_columnar(slf: Bound<'_, Self>, path: &str) -> PyResult<Py<Self>> {
        {
            let cell = slf.borrow_mut();
            cell.inner.lock().unwrap().read_columnar(path);
        }
        Ok(slf.unbind())
    }

    /// Write results to a `.tptcol` columnar file (`use_zstd=True` compresses).
    #[pyo3(signature = (path, use_zstd=false))]
    fn write_columnar(slf: Bound<'_, Self>, path: &str, use_zstd: bool) -> PyResult<Py<Self>> {
        {
            let cell = slf.borrow_mut();
            cell.inner.lock().unwrap().write_columnar(path, use_zstd);
        }
        Ok(slf.unbind())
    }

    /// Stream the result of a SQLite SELECT query.
    fn read_sqlite(slf: Bound<'_, Self>, path: &str, query: &str) -> PyResult<Py<Self>> {
        {
            let cell = slf.borrow_mut();
            cell.inner.lock().unwrap().read_sqlite(path, query);
        }
        Ok(slf.unbind())
    }

    /// Write batches into a SQLite table (created from the first batch).
    fn write_sqlite(slf: Bound<'_, Self>, path: &str, table: &str) -> PyResult<Py<Self>> {
        {
            let cell = slf.borrow_mut();
            cell.inner.lock().unwrap().write_sqlite(path, table);
        }
        Ok(slf.unbind())
    }

    /// Stream the result of a PostgreSQL SELECT query.
    fn read_postgres(slf: Bound<'_, Self>, conn_string: &str, query: &str) -> PyResult<Py<Self>> {
        {
            let cell = slf.borrow_mut();
            cell.inner.lock().unwrap().read_postgres(conn_string, query);
        }
        Ok(slf.unbind())
    }

    /// Write batches into a PostgreSQL table (COPY-based bulk load).
    fn write_postgres(slf: Bound<'_, Self>, conn_string: &str, table: &str) -> PyResult<Py<Self>> {
        {
            let cell = slf.borrow_mut();
            cell.inner
                .lock()
                .unwrap()
                .write_postgres(conn_string, table);
        }
        Ok(slf.unbind())
    }

    /// Read an S3 (or S3-compatible) object. Credentials come from the
    /// `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` / `AWS_SESSION_TOKEN`
    /// environment variables.
    fn read_s3(slf: Bound<'_, Self>, bucket_url: &str, key: &str) -> PyResult<Py<Self>> {
        let creds = tpt_stream_core::CloudCredentials::from_env().map_err(to_pyerr)?;
        {
            let cell = slf.borrow_mut();
            let mut inner = cell.inner.lock().unwrap();
            inner.read_s3(bucket_url, key, &creds).map_err(to_pyerr)?;
        }
        Ok(slf.unbind())
    }

    /// Write results to an S3 (or S3-compatible) object (env credentials).
    fn write_s3(slf: Bound<'_, Self>, bucket_url: &str, key: &str) -> PyResult<Py<Self>> {
        let creds = tpt_stream_core::CloudCredentials::from_env().map_err(to_pyerr)?;
        {
            let cell = slf.borrow_mut();
            let mut inner = cell.inner.lock().unwrap();
            inner.write_s3(bucket_url, key, &creds).map_err(to_pyerr)?;
        }
        Ok(slf.unbind())
    }

    /// Read a Google Cloud Storage object via the S3-compatible XML API
    /// (env credentials: the HMAC key pair in `AWS_ACCESS_KEY_ID` /
    /// `AWS_SECRET_ACCESS_KEY`).
    fn read_gcs(slf: Bound<'_, Self>, bucket: &str, key: &str) -> PyResult<Py<Self>> {
        let creds = tpt_stream_core::CloudCredentials::from_env().map_err(to_pyerr)?;
        {
            let cell = slf.borrow_mut();
            let mut inner = cell.inner.lock().unwrap();
            inner.read_gcs(bucket, key, &creds).map_err(to_pyerr)?;
        }
        Ok(slf.unbind())
    }

    /// Write results to a Google Cloud Storage object (env credentials).
    fn write_gcs(slf: Bound<'_, Self>, bucket: &str, key: &str) -> PyResult<Py<Self>> {
        let creds = tpt_stream_core::CloudCredentials::from_env().map_err(to_pyerr)?;
        {
            let cell = slf.borrow_mut();
            let mut inner = cell.inner.lock().unwrap();
            inner.write_gcs(bucket, key, &creds).map_err(to_pyerr)?;
        }
        Ok(slf.unbind())
    }

    /// Read an Azure blob. Credentials come from `AZURE_STORAGE_ACCOUNT` /
    /// `AZURE_STORAGE_KEY` (or a connection string).
    fn read_azure(
        slf: Bound<'_, Self>,
        account_url: &str,
        container: &str,
        key: &str,
    ) -> PyResult<Py<Self>> {
        let creds = tpt_stream_core::AzureCredentials::from_env().map_err(to_pyerr)?;
        let store = tpt_stream_core::AzureBlobStore::new(account_url, container, &creds)
            .map_err(to_pyerr)?;
        {
            let cell = slf.borrow_mut();
            cell.inner.lock().unwrap().read_azure_blob(store, key);
        }
        Ok(slf.unbind())
    }

    /// Write results to an Azure block blob (env credentials).
    fn write_azure(
        slf: Bound<'_, Self>,
        account_url: &str,
        container: &str,
        key: &str,
    ) -> PyResult<Py<Self>> {
        let creds = tpt_stream_core::AzureCredentials::from_env().map_err(to_pyerr)?;
        let store = tpt_stream_core::AzureBlobStore::new(account_url, container, &creds)
            .map_err(to_pyerr)?;
        {
            let cell = slf.borrow_mut();
            cell.inner.lock().unwrap().write_azure_blob(store, key);
        }
        Ok(slf.unbind())
    }

    /// Stream a plain HTTP(S) URL. Format comes from the extension
    /// (`.csv` default, `.jsonl`, `.json`); `.gz` URLs are decompressed.
    fn read_http(slf: Bound<'_, Self>, url: &str) -> PyResult<Py<Self>> {
        {
            let cell = slf.borrow_mut();
            cell.inner.lock().unwrap().read_http(url);
        }
        Ok(slf.unbind())
    }

    /// Set the error policy for subsequent reads: `"strict"` (default),
    /// `"skip"`, or `"quarantine:<path>"` to capture malformed rows.
    fn on_error(slf: Bound<'_, Self>, policy: &str) -> PyResult<Py<Self>> {
        {
            let cell = slf.borrow_mut();
            let parsed = parse_error_policy(policy)?;
            cell.inner.lock().unwrap().on_error(parsed);
        }
        Ok(slf.unbind())
    }

    /// Attach data-quality checks: `rows_at_least`, `rows_at_most`,
    /// `no_nulls=[cols]`, `unique=[cols]`. A violation aborts `execute()`.
    #[pyo3(signature = (rows_at_least=None, rows_at_most=None, no_nulls=None, unique=None))]
    fn expect(
        slf: Bound<'_, Self>,
        rows_at_least: Option<u64>,
        rows_at_most: Option<u64>,
        no_nulls: Option<Vec<String>>,
        unique: Option<Vec<String>>,
    ) -> PyResult<Py<Self>> {
        let mut checks: Vec<Check> = Vec::new();
        if let Some(n) = rows_at_least {
            checks.push(Check::RowsAtLeast(n));
        }
        if let Some(n) = rows_at_most {
            checks.push(Check::RowsAtMost(n));
        }
        for col in no_nulls.unwrap_or_default() {
            checks.push(Check::NoNulls(col));
        }
        for col in unique.unwrap_or_default() {
            checks.push(Check::Unique(col));
        }
        if checks.is_empty() {
            return Err(TptError::new_err("expect: no checks given"));
        }
        {
            let cell = slf.borrow_mut();
            cell.inner.lock().unwrap().expect_checks(checks);
        }
        Ok(slf.unbind())
    }

    /// Human-readable stage plan, e.g. `"source -> filter -> sink"`.
    fn explain(&self) -> String {
        self.inner.lock().unwrap().explain()
    }

    /// Run the stages over the first `n` output rows and return them as a
    /// list of dicts (inspection only; consumes the source).
    #[pyo3(signature = (n=10))]
    fn preview(&self, py: Python<'_>, n: usize) -> PyResult<Vec<Py<PyDict>>> {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| TptError::new_err(format!("failed to build runtime: {e}")))?;
        let batches = py
            .allow_threads(|| {
                let mut inner = self.inner.lock().unwrap();
                runtime.block_on(inner.preview(n))
            })
            .map_err(to_pyerr)?;
        let mut out = Vec::new();
        for batch in batches {
            for row in 0..batch.num_rows() {
                let dict = PyDict::new(py);
                for column in batch.columns() {
                    let value = column.get(row).unwrap_or(tpt_stream_core::Value::Null);
                    dict.set_item(column.name(), value_to_py(py, &value)?)?;
                }
                out.push(dict.unbind());
            }
        }
        Ok(out)
    }

    /// Run the pipeline and return every output row as a list of dicts.
    /// Memory holds the whole result set.
    fn collect(&self, py: Python<'_>) -> PyResult<Vec<Py<PyDict>>> {
        let batches = self.collect_batches(py)?;
        let mut out = Vec::new();
        for batch in batches {
            for row in 0..batch.num_rows() {
                let dict = PyDict::new(py);
                for column in batch.columns() {
                    let value = column.get(row).unwrap_or(tpt_stream_core::Value::Null);
                    dict.set_item(column.name(), value_to_py(py, &value)?)?;
                }
                out.push(dict.unbind());
            }
        }
        Ok(out)
    }

    /// Run the pipeline and return a pandas DataFrame (requires `pandas`;
    /// goes through `collect()` + `pandas.DataFrame`, no Arrow dependency).
    fn to_pandas(&self, py: Python<'_>) -> PyResult<PyObject> {
        let rows = self.collect(py)?;
        let pd = py.import("pandas").map_err(|_| {
            TptError::new_err("to_pandas requires the 'pandas' package: pip install pandas")
        })?;
        let frame = pd
            .getattr("DataFrame")
            .and_then(|c| c.call1((rows,)))
            .map_err(py_err)?;
        Ok(frame.unbind())
    }

    /// Number of pipeline stages attached so far.
    fn num_stages(&self) -> usize {
        self.inner.lock().unwrap().num_stages()
    }

    fn __repr__(&self) -> String {
        format!(
            "Pipeline(stages={})",
            self.inner.lock().unwrap().num_stages()
        )
    }
}

/// Handle returned by `Pipeline.group_by(...)`.
#[pyclass(name = "GroupBy")]
struct PyGroupBy {
    pipeline: Py<PyPipeline>,
    columns: Vec<String>,
}

#[pymethods]
impl PyGroupBy {
    /// Apply aggregate specs `{column: 'sum' | 'avg' | 'count' | 'count_all'
    /// | 'min' | 'max'}` and return the pipeline.
    fn agg(&self, py: Python<'_>, aggs: &Bound<'_, PyDict>) -> PyResult<Py<PyPipeline>> {
        let mut specs: Vec<AggSpec> = Vec::new();
        for (k, v) in aggs.iter() {
            let column = k.extract::<String>()?;
            let func = v.extract::<String>()?;
            let spec = match func.as_str() {
                "sum" => AggSpec::sum(&column),
                "avg" => AggSpec::avg(&column),
                "count" => AggSpec::count(&column),
                "count_all" => AggSpec::count_all(&column),
                "min" => AggSpec::min(&column),
                "max" => AggSpec::max(&column),
                other => {
                    return Err(TptError::new_err(format!(
                        "unknown aggregate function {other:?} (expected sum|avg|count|count_all|min|max)"
                    )))
                }
            };
            specs.push(spec);
        }
        {
            let pipeline = self.pipeline.bind(py).borrow();
            let mut inner = pipeline.inner.lock().unwrap();
            let key_refs: Vec<&str> = self.columns.iter().map(|s| s.as_str()).collect();
            inner.aggregate(&key_refs, &specs);
            check_pending(&inner)?;
        }
        Ok(self.pipeline.clone_ref(py))
    }
}

#[pymodule]
fn _native(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyPipeline>()?;
    m.add_class::<PyGroupBy>()?;
    m.add("TptError", m.py().get_type::<TptError>())?;
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    Ok(())
}
