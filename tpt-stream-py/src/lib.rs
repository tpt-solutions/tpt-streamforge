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
use tpt_stream_core::Pipeline;

create_exception!(tpt_streamforge, TptError, PyException);

fn to_pyerr(err: tpt_stream_core::Error) -> PyErr {
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

#[pyclass(name = "Pipeline")]
struct PyPipeline {
    inner: Mutex<Pipeline>,
    progress: Mutex<Option<PyObject>>,
}

impl PyPipeline {
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
        let mut inner = self.inner.lock().unwrap();
        check_pending(&inner)?;
        // Attach the Python progress callback (if registered) as a telemetry
        // hook. `execute` runs on this thread with the GIL held, so the hook
        // can re-enter Python directly.
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
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| TptError::new_err(format!("failed to build runtime: {e}")))?;
        let stats = runtime.block_on(inner.execute()).map_err(to_pyerr)?;
        drop(inner);
        let out = PyDict::new(py);
        out.set_item("rows", stats.rows)?;
        out.set_item("batches", stats.batches)?;
        out.set_item("bytes_in", stats.bytes_in)?;
        out.set_item("bytes_out", stats.bytes_out)?;
        Ok(out.unbind())
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
