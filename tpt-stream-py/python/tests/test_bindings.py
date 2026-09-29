"""Tests for the expanded Python bindings: new sources/sinks, join, error
policies, expect checks, preview/explain/collect, and pandas export."""

import importlib.resources

import pytest

from tpt_streamforge import Pipeline, TptError


def write_text(path, content):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8", newline="")


CSV = "id,name,amount\n1,alice,10\n2,bob,20\n3,carol,30\n"
JOIN_RIGHT = "id,label\n1,left-a\n3,left-c\n"


def test_select_and_collect(tmp_path):
    src = tmp_path / "in.csv"
    write_text(src, CSV)

    rows = (
        Pipeline()
        .read_csv(str(src))
        .select(["name", "amount"])
        .collect()
    )
    assert rows == [
        {"name": "alice", "amount": 10},
        {"name": "bob", "amount": 20},
        {"name": "carol", "amount": 30},
    ]


def test_join_csv(tmp_path):
    left = tmp_path / "left.csv"
    right = tmp_path / "right.csv"
    out = tmp_path / "out.csv"
    write_text(left, CSV)
    write_text(right, JOIN_RIGHT)

    (
        Pipeline()
        .read_csv(str(left))
        .join_csv(str(right), ["id"], ["id"], "inner")
        .sort(["id"])
        .write_csv(str(out))
        .execute()
    )

    lines = out.read_text(encoding="utf-8").splitlines()
    # Colliding join keys get a documented `_r` suffix on the right side.
    assert lines[0] == "id,name,amount,id_r,label"
    assert lines[1].replace(" ", "").startswith("1,alice,10,1,left-a")
    assert lines[2].replace(" ", "").startswith("3,carol,30,3,left-c")


def test_join_invalid_type(tmp_path):
    src = tmp_path / "in.csv"
    write_text(src, CSV)
    p = Pipeline().read_csv(str(src))
    with pytest.raises(TptError, match="join_type"):
        p.join_csv(str(src), ["id"], ["id"], "outer")


def test_jsonl_roundtrip(tmp_path):
    src = tmp_path / "in.jsonl"
    out = tmp_path / "out.jsonl"
    write_text(src, '{"a":1,"b":"x"}\n{"a":2,"b":"y"}\n')

    (
        Pipeline()
        .read_jsonl(str(src))
        .write_jsonl(str(out))
        .execute()
    )
    assert out.read_text(encoding="utf-8").splitlines() == [
        '{"a":1,"b":"x"}',
        '{"a":2,"b":"y"}',
    ]


def test_json_array_write_pretty(tmp_path):
    src = tmp_path / "in.csv"
    out = tmp_path / "out.json"
    write_text(src, "a\n1\n2\n")

    (
        Pipeline()
        .read_csv(str(src))
        .write_json(str(out), pretty=True)
        .execute()
    )
    text = out.read_text(encoding="utf-8")
    assert text.startswith("[\n")


def test_columnar_roundtrip(tmp_path):
    src = tmp_path / "in.csv"
    col = tmp_path / "data.tptcol"
    out = tmp_path / "out.csv"
    write_text(src, CSV)

    (
        Pipeline()
        .read_csv(str(src))
        .write_columnar(str(col), use_zstd=True)
        .execute()
    )
    (
        Pipeline()
        .read_columnar(str(col))
        .write_csv(str(out))
        .execute()
    )
    assert out.read_text(encoding="utf-8") == CSV


def test_sqlite_roundtrip(tmp_path):
    pytest.importorskip("tpt_streamforge")  # extension must be importable
    src = tmp_path / "in.csv"
    db = tmp_path / "out.sqlite"
    out = tmp_path / "back.csv"
    write_text(src, "id,v\n1,10\n2,20\n")

    (
        Pipeline()
        .read_csv(str(src))
        .write_sqlite(str(db), "vals")
        .execute()
    )
    (
        Pipeline()
        .read_sqlite(str(db), "SELECT id, v FROM vals ORDER BY id")
        .write_csv(str(out))
        .execute()
    )
    assert out.read_text(encoding="utf-8") == "id,v\n1,10\n2,20\n"


def test_on_error_skip_and_quarantine(tmp_path):
    src = tmp_path / "ragged.csv"
    out = tmp_path / "out.csv"
    bad = tmp_path / "bad.csv"
    write_text(src, "a,b\n1,x\n2\n3,z\n")

    (
        Pipeline()
        .on_error("quarantine:" + str(bad))
        .read_csv(str(src))
        .write_csv(str(out))
        .execute()
    )
    assert out.read_text(encoding="utf-8") == "a,b\n1,x\n3,z\n"
    assert bad.read_text(encoding="utf-8") == "a,b\n2\n"


def test_on_error_strict_is_default(tmp_path):
    src = tmp_path / "ragged.csv"
    write_text(src, "a,b\n1,x\n2\n")

    p = Pipeline().read_csv(str(src)).write_csv(str(tmp_path / "out.csv"))
    with pytest.raises(TptError, match="line 3"):
        p.execute()


def test_on_error_invalid_policy(tmp_path):
    src = tmp_path / "in.csv"
    write_text(src, CSV)
    p = Pipeline().read_csv(str(src))
    with pytest.raises(TptError, match="error policy"):
        p.on_error("ignore")


def test_expect_checks_pass_and_fail(tmp_path):
    src = tmp_path / "in.csv"
    write_text(src, "id\n1\n2\n3\n")
    out = tmp_path / "out.csv"

    (
        Pipeline()
        .read_csv(str(src))
        .expect(rows_at_least=1, rows_at_most=10, unique=["id"])
        .write_csv(str(out))
        .execute()
    )
    assert out.exists()

    bad = tmp_path / "bad.csv"
    write_text(bad, "id\n1\n1\n")
    p = (
        Pipeline()
        .read_csv(str(bad))
        .expect(unique=["id"])
        .write_csv(str(tmp_path / "nope.csv"))
    )
    with pytest.raises(TptError, match="data quality"):
        p.execute()


def test_explain_and_preview(tmp_path):
    src = tmp_path / "in.csv"
    write_text(src, CSV)

    p = Pipeline().read_csv(str(src)).filter("amount >= 20")
    assert p.explain() == "source -> filter"

    rows = p.preview(1)
    assert rows == [{"id": 2, "name": "bob", "amount": 20}]


def test_execute_after_preview_is_rejected(tmp_path):
    src = tmp_path / "in.csv"
    write_text(src, CSV)
    p = Pipeline().read_csv(str(src))
    p.preview(1)
    with pytest.raises(TptError, match="one-shot"):
        p.write_csv(str(tmp_path / "out.csv")).execute()


def test_to_pandas(tmp_path):
    pytest.importorskip("pandas")
    src = tmp_path / "in.csv"
    write_text(src, CSV)

    frame = Pipeline().read_csv(str(src)).to_pandas()
    assert list(frame.columns) == ["id", "name", "amount"]
    assert len(frame) == 3
    assert frame["amount"].tolist() == [10, 20, 30]


def test_limit_keeps_the_first_rows(tmp_path):
    src = tmp_path / "in.csv"
    out = tmp_path / "out.csv"
    write_text(src, CSV)

    p = Pipeline().read_csv(str(src)).limit(2)
    stats = p.write_csv(str(out)).execute()
    # `rows` counts what the source produced; the limit decides what lands.
    assert stats["rows"] == 3
    assert out.read_text(encoding="utf-8") == "id,name,amount\n1,alice,10\n2,bob,20\n"
    assert [s for s in p.stage_stats() if s["name"] == "limit"][0]["rows_out"] == 2


def test_dead_letter_file_is_created(tmp_path):
    src = tmp_path / "in.csv"
    dlq = tmp_path / "rejected.csv"
    write_text(src, CSV)

    p = Pipeline().read_csv(str(src)).dead_letter(str(dlq))
    assert p.dead_letter_rows() == 0
    p.filter("amount > 0").write_csv(str(tmp_path / "out.csv")).execute()
    # Nothing failed, so the queue exists but stayed empty.
    assert dlq.read_text(encoding="utf-8") == ""


def test_dead_letter_rejects_an_unwritable_path(tmp_path):
    src = tmp_path / "in.csv"
    write_text(src, CSV)
    with pytest.raises(TptError):
        Pipeline().read_csv(str(src)).dead_letter(str(tmp_path / "no" / "such" / "q.csv"))


def test_count_all_names_the_output_column_after_the_key(tmp_path):
    """`agg`'s dict key doubles as the output name for `count_all`.

    That differs from the CLI/WASM bindings, which always emit `count_all`; the
    READMEs document the difference, and this test pins the Python behavior.
    """
    src = tmp_path / "in.csv"
    write_text(src, "k,v\na,1\na,2\nb,3\n")

    # A fresh key keeps the grouping column and adds a row count.
    rows = (
        Pipeline()
        .read_csv(str(src))
        .group_by(["k"])
        .agg({"n": "count_all"})
        .sort(["k"])
        .collect()
    )
    assert rows == [{"k": "a", "n": 2}, {"k": "b", "n": 1}]


def test_with_retry_is_chainable_and_optional(tmp_path):
    src = tmp_path / "in.csv"
    out = tmp_path / "out.csv"
    write_text(src, CSV)

    # with_retry only affects the next network source/sink; a local CSV run
    # must be unaffected.
    stats = (
        Pipeline()
        .with_retry(3, base_delay_ms=1, max_delay_ms=5)
        .read_csv(str(src))
        .write_csv(str(out))
        .execute()
    )
    assert stats["rows"] == 3
    assert out.read_text(encoding="utf-8") == CSV


def test_shipped_type_stub_matches_the_runtime_api():
    """`_native.pyi` must exist and mention every public builder method."""
    import tpt_streamforge

    stub = (importlib.resources.files(tpt_streamforge) / "_native.pyi").read_text()
    for name in [
        "read_csv", "write_csv", "filter", "map", "select", "sort", "dedup",
        "join_csv", "group_by", "limit", "dead_letter", "dead_letter_rows",
        "with_retry", "expect", "sample", "on_error", "on_progress", "execute",
        "collect", "to_pandas", "preview", "explain", "stage_stats",
        "read_s3", "write_s3", "read_gcs", "write_gcs", "read_azure",
        "write_azure", "read_postgres", "write_postgres", "read_sqlite",
        "write_sqlite", "read_jsonl", "write_jsonl", "read_json", "write_json",
        "read_columnar", "write_columnar", "read_http",
    ]:
        assert f"def {name}(" in stub, f"{name} missing from _native.pyi"
    # PEP 561 marker must ship alongside it.
    assert (importlib.resources.files(tpt_streamforge) / "py.typed").is_file()


def _run_expect(tmp_path, csv, **checks):
    src = tmp_path / "c.csv"
    write_text(src, csv)
    return (
        Pipeline()
        .read_csv(str(src))
        .expect(**checks)
        .write_csv(str(tmp_path / "o.csv"))
        .execute()
    )


def test_expect_range_one_of_and_type(tmp_path):
    ok = "\n".join(["id,status,score", "1,a,0.5", "2,b,1.5", "3,a,", ""])
    _run_expect(tmp_path, ok, ranges={"score": (0, 2)}, one_of={"status": ["a", "b"]},
                types={"id": "int32", "status": "utf8"})
    _run_expect(tmp_path, ok, ranges={"id": (1, None)})

    with pytest.raises(TptError, match="out of range"):
        _run_expect(tmp_path, ok, ranges={"score": (None, 1.0)})
    with pytest.raises(TptError, match="not allowed"):
        _run_expect(tmp_path, ok, one_of={"status": ["a"]})
    with pytest.raises(TptError, match="has type"):
        _run_expect(tmp_path, ok, types={"id": "utf8"})
    with pytest.raises(TptError, match="unknown type"):
        _run_expect(tmp_path, ok, types={"id": "wat"})


def test_sample_is_deterministic_and_roughly_the_fraction(tmp_path):
    src = tmp_path / "big.csv"
    n = 20000
    write_text(src, "\n".join(["id,v"] + [f"{i},{i}" for i in range(n)] + [""]))

    def sampled(fraction, seed):
        return {
            r["id"]
            for r in Pipeline().read_csv(str(src)).sample(fraction, ["id"], seed=seed).collect()
        }

    a = sampled(0.25, 7)
    assert a == sampled(0.25, 7)
    assert a != sampled(0.25, 8)
    assert abs(len(a) / n - 0.25) < 0.02
    assert sampled(0.0, 1) == set()
    assert len(sampled(1.0, 1)) == n
    with pytest.raises(TptError):
        Pipeline().read_csv(str(src)).sample(1.5, ["id"])
    with pytest.raises(TptError):
        Pipeline().read_csv(str(src)).sample(0.5, [])
