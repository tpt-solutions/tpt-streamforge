"""Runnable tpt-streamforge example: build a per-region sales report.

Creates a small CSV, streams it through filter -> map -> group-by -> sort,
writes the report, and prints the plan, telemetry, and a preview.

    python tpt-stream-py/examples/sales_report.py

Requires the package to be installed (`pip install tpt-streamforge`, or
`maturin develop -m tpt-stream-py/Cargo.toml` from the repo root). Nothing
here touches the network, and the whole run stays inside a fixed-size chunk
buffer (65,536 rows by default).
"""

from __future__ import annotations

import tempfile
from pathlib import Path

from tpt_streamforge import Pipeline

# 1,000 fabricated orders: region rotates, some rows have amount == 0.
SAMPLE_ROWS = 1_000


def write_sample(path: Path) -> None:
    regions = ["emea", "amer", "apac", "latam"]
    lines = ["order_id,region,amount,status"]
    for order_id in range(1, SAMPLE_ROWS + 1):
        region = regions[order_id % len(regions)]
        amount = 0 if order_id % 50 == 0 else round((order_id * 7 % 400) / 4, 2)
        status = "refunded" if amount == 0 else "paid"
        lines.append(f"{order_id},{region},{amount},{status}")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        work = Path(tmp)
        source = work / "orders.csv"
        report = work / "report.csv"
        write_sample(source)

        events: list[dict] = []

        pipeline = (
            Pipeline()
            .read_csv(str(source))
            .on_error("strict")
            .filter("amount > 0")
            .map({"region": "region", "gross": "amount * 1.2"})
            .expect(rows_at_least=1, no_nulls=["region"])
            .group_by(["region"])
            .agg({"gross": "sum", "count_all": "count_all"})
            .sort(["sum_gross"], descending=True)
            .select(["region", "sum_gross", "count_all"])
            .write_csv(str(report))
        )

        # Telemetry: one callback for the whole run.
        pipeline = pipeline.on_progress(events.append)

        print("plan:", " -> ".join(pipeline.explain().split(" -> ")))
        stats = pipeline.execute()
        print(
            f"streamed {stats['rows']} rows in {stats['batches']} batch(es): "
            f"{stats['bytes_in']} bytes in, {stats['bytes_out']} bytes out"
        )

        print("\nstage telemetry:")
        for stage in pipeline.stage_stats():
            print(
                f"  {stage['name']:<12} {stage['rows_in']:>6} in / "
                f"{stage['rows_out']:>6} out  "
                f"{stage['rows_per_sec']:>10.0f} rows/s"
            )
        print(f"  ({len(events)} progress events)")

        print("\nreport:")
        print(report.read_text(encoding="utf-8").rstrip())

        # Anything the pipeline produced can also be pulled back into Python
        # as plain dicts (or into pandas with `to_pandas()`).
        preview = (
            Pipeline()
            .read_csv(str(report))
            .sort(["sum_gross"], descending=True)
            .preview(2)
        )
        print("\npreview(2) as dicts:", preview)


if __name__ == "__main__":
    main()
