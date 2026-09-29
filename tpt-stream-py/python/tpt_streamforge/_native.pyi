"""Type stubs for the PyO3 extension module (see `tpt-stream-py/src/lib.rs`).

Hand-written to match the `#[pymethods]` surface exactly; keep the two in sync.
"""

from __future__ import annotations

from typing import Any, Callable, TypedDict

class StageStat(TypedDict):
    name: str
    rows_in: int
    rows_out: int
    batches: int
    elapsed_ms: float
    rows_per_sec: float

class ExecuteStats(TypedDict):
    rows: int
    batches: int
    bytes_in: int
    bytes_out: int

Row = dict[str, Any]
ProgressCallback = Callable[[dict[str, Any]], None]

class TptError(Exception):
    """Raised for every engine error (parse, I/O, schema, data quality)."""

class GroupBy:
    """Handle returned by `Pipeline.group_by(...)`."""

    def agg(self, aggs: dict[str, str]) -> Pipeline:
        """Apply `{column: 'sum'|'avg'|'count'|'count_all'|'min'|'max'}`.

        Output columns are named `{fn}_{column}`; for `count_all` the key is
        used verbatim as the output column name.
        """
        ...

class Pipeline:
    """Streaming ETL pipeline.

    Source/sink/stage builders all return the same instance, so calls chain:

        Pipeline().read_csv("in.csv").filter("n > 1").write_csv("out.csv")
    """

    def __init__(self) -> None: ...
    # -- sources ------------------------------------------------------------
    def read_csv(self, path: str, chunk_size: int | None = None) -> Pipeline:
        """Read a CSV file. `chunk_size` rows are buffered per batch."""
        ...
    def read_jsonl(self, path: str, chunk_size: int | None = None) -> Pipeline: ...
    def read_json(self, path: str, chunk_size: int | None = None) -> Pipeline: ...
    def read_columnar(self, path: str) -> Pipeline: ...
    def read_sqlite(self, path: str, query: str) -> Pipeline: ...
    def read_postgres(self, conn_string: str, query: str) -> Pipeline: ...
    def read_s3(self, bucket_url: str, key: str) -> Pipeline:
        """Read an S3 object; credentials come from the `AWS_*` env vars."""
        ...
    def read_gcs(self, bucket: str, key: str) -> Pipeline: ...
    def read_azure(self, account_url: str, container: str, key: str) -> Pipeline: ...
    def read_http(self, url: str) -> Pipeline:
        """Stream a plain HTTP(S) URL; format comes from the extension."""
        ...
    # -- sinks --------------------------------------------------------------
    def write_csv(self, path: str) -> Pipeline: ...
    def write_jsonl(self, path: str) -> Pipeline: ...
    def write_json(self, path: str, pretty: bool = False) -> Pipeline: ...
    def write_columnar(self, path: str, use_zstd: bool = False) -> Pipeline: ...
    def write_sqlite(self, path: str, table: str) -> Pipeline: ...
    def write_postgres(self, conn_string: str, table: str) -> Pipeline: ...
    def write_s3(self, bucket_url: str, key: str) -> Pipeline: ...
    def write_gcs(self, bucket: str, key: str) -> Pipeline: ...
    def write_azure(self, account_url: str, container: str, key: str) -> Pipeline: ...
    # -- stages -------------------------------------------------------------
    def filter(self, expr: str) -> Pipeline: ...
    def map(self, mapping: dict[str, str]) -> Pipeline:
        """Project rows to `{output_column: expression}` (replaces the schema)."""
        ...
    def select(self, columns: list[str]) -> Pipeline: ...
    def sort(self, columns: list[str], descending: bool = False) -> Pipeline: ...
    def dedup(self, columns: list[str]) -> Pipeline:
        """Keep the first row per key over `columns`."""
        ...
    def join_csv(
        self,
        right_path: str,
        left_keys: list[str],
        right_keys: list[str],
        join_type: str = "inner",
    ) -> Pipeline:
        """`join_type` is `"inner"`, `"left"`, or `"right"`."""
        ...
    def group_by(self, columns: list[str]) -> GroupBy: ...
    def limit(self, n: int) -> Pipeline:
        """Pass through only the first `n` rows."""
        ...
    def expect(
        self,
        rows_at_least: int | None = None,
        rows_at_most: int | None = None,
        no_nulls: list[str] | None = None,
        unique: list[str] | None = None,
        ranges: dict[str, tuple[float | None, float | None]] | None = None,
        one_of: dict[str, list[Any]] | None = None,
        types: dict[str, str] | None = None,
    ) -> Pipeline:
        """Attach data-quality checks; a violation aborts `execute()`.

        `ranges={"age": (0, 120)}` (either bound may be `None`; nulls skipped),
        `one_of={"status": ["a", "b"]}`, `types={"id": "int32"}` (one of int32,
        int64, float32, float64, utf8, bool, date, timestamp).
        """
        ...
    def sample(self, fraction: float, key: list[str], seed: int = 0) -> Pipeline:
        """Keep a deterministic `fraction` (0..=1) of rows by hashing `key` columns.

        The same input and `seed` always give the same sample, and rows that
        share a key are kept or dropped together.
        """
        ...
    def on_error(self, policy: str) -> Pipeline:
        """`"strict"` (default), `"skip"`, or `"quarantine:<path>"`."""
        ...
    def dead_letter(self, path: str) -> Pipeline:
        """Capture rows a *stage* rejects in `path` (CSV) instead of aborting.

        Not supported with stateful stages (aggregate/sort/dedup/join/expect).
        """
        ...
    def with_retry(
        self,
        attempts: int,
        base_delay_ms: int = 100,
        max_delay_ms: int = 10000,
    ) -> Pipeline:
        """Retry transient network failures for the *next* `read_*`/`write_*`.

        `attempts` counts total tries, so `1` disables retrying (the default).
        One-shot: consumed by the next network source/sink, so a policy never
        silently leaks onto a later one.
        """
        ...
    def on_progress(self, callback: ProgressCallback) -> Pipeline:
        """Call `callback(event_dict)` for every telemetry event."""
        ...
    # -- running / inspecting ----------------------------------------------
    def execute(self) -> ExecuteStats:
        """Run the pipeline. Releases the GIL; sources are one-shot.

        `rows`/`batches` count what the **source** produced; how many rows came
        out is the sum of `stage_stats()[-1]["rows_out"]` (or the last
        `sink_batch` telemetry event's `total_rows`).
        """
        ...
    def collect(self) -> list[Row]:
        """Run the pipeline and return every output row (holds it in memory)."""
        ...
    def to_pandas(self) -> Any:
        """`collect()` + `pandas.DataFrame` (requires the `pandas` package)."""
        ...
    def preview(self, n: int = 10) -> list[Row]:
        """Run over the first `n` output rows (inspection only)."""
        ...
    def explain(self) -> str:
        """Human-readable stage plan, e.g. `"csv source -> filter -> csv sink"`."""
        ...
    def stage_stats(self) -> list[StageStat]:
        """Per-stage cumulative metrics from the last `execute()` run."""
        ...
    def dead_letter_rows(self) -> int:
        """Rows captured by the dead-letter queue so far (0 if none)."""
        ...
    def num_stages(self) -> int: ...
