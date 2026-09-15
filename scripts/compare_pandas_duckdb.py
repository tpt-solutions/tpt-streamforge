"""Benchmark the same operations as tpt-stream-core's `stages` criterion
suite against pandas and DuckDB, on the identical 1M-row CSV fixture.

Used to produce the README performance comparison. Run with the project
virtualenv:

    python scripts/compare_pandas_duckdb.py
"""

import io
import os
import sys
import time

ROWS = 1_000_000
TMP = os.environ.get("TEMP", "/tmp")
CSV = os.path.join(TMP, "tpt-streamforge-stages-1m.csv")
OUT = os.path.join(TMP, "tpt-streamforge-stages-out.csv")


def ensure_fixture():
    """Same bytes the Rust `stages` bench generates (id,k,score,name,flag)."""
    if os.path.exists(CSV):
        return
    with open(CSV, "w", newline="\n") as f:
        f.write("id,k,score,name,flag\n")
        buf = io.StringIO()
        for i in range(ROWS):
            buf.write(
                f"{i},{i % 50},{(i * 7919) % 100_000 / 100:.2f},user{i},"
                f"{'true' if i % 3 == 0 else 'false'}\n"
            )
            if buf.tell() > 1 << 20:
                f.write(buf.getvalue())
                buf = io.StringIO()
        f.write(buf.getvalue())


def timed(fn, repeat=3):
    """Run `fn` `repeat` times; return (result, best elapsed seconds)."""
    best = float("inf")
    result = None
    for _ in range(repeat):
        start = time.perf_counter()
        result = fn()
        best = min(best, time.perf_counter() - start)
    return result, best


def bench_pandas():
    import pandas as pd

    rows = {}

    df, elapsed = timed(lambda: pd.read_csv(CSV))
    assert len(df) == ROWS
    rows["read_csv"] = elapsed

    def write_back():
        pd.read_csv(CSV).to_csv(OUT, index=False, lineterminator="\n")

    rows["csv_to_csv"] = timed(write_back)[1]

    def filter_op():
        df = pd.read_csv(CSV)
        return df[df["score"] > 500]

    filtered, elapsed = timed(filter_op)
    assert len(filtered) > 0
    rows["filter"] = elapsed

    def map_op():
        df = pd.read_csv(CSV)
        return df.assign(doubled=df["score"] * 2)

    rows["map"] = timed(map_op)[1]

    def select_op():
        df = pd.read_csv(CSV)
        return df[["id", "score", "name"]]

    rows["select"] = timed(select_op)[1]

    def agg_op():
        df = pd.read_csv(CSV)
        return df.groupby("k").agg(
            sum_score=("score", "sum"), avg_score=("score", "mean"), n=("id", "size")
        )

    agg, elapsed = timed(agg_op)
    assert len(agg) == 50
    rows["aggregate"] = elapsed

    def sort_op():
        return pd.read_csv(CSV).sort_values("score")

    rows["sort"] = timed(sort_op)[1]

    def dedup_op():
        return pd.read_csv(CSV).drop_duplicates(subset=["name"])

    deduped, elapsed = timed(dedup_op)
    assert len(deduped) == ROWS
    rows["dedup"] = elapsed

    return rows


def bench_duckdb():
    """DuckDB timings measure query execution only (no Python-side result
    materialization): `execute()` runs the query eagerly and the timed
    callable stops there; `fetchall()` happens outside the timer and only
    feeds assertions."""
    import duckdb

    con = duckdb.connect()
    rows = {}

    def read():
        con.execute(f"SELECT * FROM read_csv_auto('{CSV}')")

    _, elapsed = timed(read)
    assert con.execute(f"SELECT COUNT(*) FROM read_csv_auto('{CSV}')").fetchone()[0] == ROWS
    rows["read_csv"] = elapsed

    def write_back():
        con.execute(
            f"COPY (SELECT * FROM read_csv_auto('{CSV}')) TO '{OUT}' (HEADER, DELIMITER ',')"
        )

    rows["csv_to_csv"] = timed(write_back)[1]

    def filter_op():
        con.execute(f"SELECT * FROM read_csv_auto('{CSV}') WHERE score > 500")

    _, elapsed = timed(filter_op)
    n = con.execute(
        f"SELECT COUNT(*) FROM read_csv_auto('{CSV}') WHERE score > 500"
    ).fetchone()[0]
    assert n > 0
    rows["filter"] = elapsed

    def map_op():
        con.execute(
            f"SELECT id, k, score, name, flag, score * 2 AS doubled FROM read_csv_auto('{CSV}')"
        )

    rows["map"] = timed(map_op)[1]

    def select_op():
        con.execute(f"SELECT id, score, name FROM read_csv_auto('{CSV}')")

    rows["select"] = timed(select_op)[1]

    def agg_op():
        con.execute(
            f"SELECT k, SUM(score), AVG(score), COUNT(*) FROM read_csv_auto('{CSV}') GROUP BY k"
        )

    _, elapsed = timed(agg_op)
    n = con.execute(
        f"SELECT COUNT(DISTINCT k) FROM read_csv_auto('{CSV}')"
    ).fetchone()[0]
    assert n == 50
    rows["aggregate"] = elapsed

    def sort_op():
        con.execute(f"SELECT * FROM read_csv_auto('{CSV}') ORDER BY score")

    rows["sort"] = timed(sort_op)[1]

    def dedup_op():
        con.execute(f"SELECT DISTINCT ON (name) * FROM read_csv_auto('{CSV}')")

    _, elapsed = timed(dedup_op)
    n = con.execute(
        f"SELECT COUNT(*) FROM (SELECT DISTINCT name FROM read_csv_auto('{CSV}'))"
    ).fetchone()[0]
    assert n == ROWS
    rows["dedup"] = elapsed

    con.close()
    return rows


OPS = [
    ("read_csv", "read 1M-row CSV (5 columns)"),
    ("csv_to_csv", "CSV -> CSV copy"),
    ("filter", "filter score > 500"),
    ("map", "map score * 2 (new column)"),
    ("select", "project 3 of 5 columns"),
    ("aggregate", "GROUP BY k: SUM / AVG / COUNT"),
    ("sort", "sort by score"),
    ("dedup", "dedup by unique key"),
]


def main():
    ensure_fixture()
    which = sys.argv[1:] or ["pandas", "duckdb"]
    results = {}
    if "pandas" in which:
        results["pandas"] = bench_pandas()
    if "duckdb" in which:
        results["duckdb"] = bench_duckdb()

    header = f"{'operation':<34}" + "".join(f"{engine + ' (s)':>14}" for engine in which)
    print(f"\n{header}")
    print("-" * len(header))
    for key, label in OPS:
        line = f"{label:<34}"
        for engine in which:
            line += f"{results[engine][key]:>14.3f}"
        print(line)


if __name__ == "__main__":
    main()
