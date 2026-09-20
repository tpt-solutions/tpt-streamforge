"""Tests for the expanded Python bindings: new sources/sinks, join, error
policies, expect checks, preview/explain/collect, and pandas export."""

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
