import pytest

from tpt_streamforge import Pipeline, TptError


def write_csv(path, content):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8", newline="")


def test_filter_map_write(tmp_path):
    src = tmp_path / "in.csv"
    out = tmp_path / "out.csv"
    write_csv(src, "id,name,score\n1,alice,90\n2,bob,40\n3,carol,72\n")

    stats = (
        Pipeline()
        .read_csv(str(src))
        .filter("score > 60 and length(name) > 3")
        .map({"id": "id", "label": "upper(name)"})
        .write_csv(str(out))
        .execute()
    )

    assert stats["rows"] == 3
    assert stats["batches"] == 1
    assert out.read_text(encoding="utf-8") == "id,label\n1,ALICE\n3,CAROL\n"


def test_group_by_agg_sort(tmp_path):
    src = tmp_path / "in.csv"
    out = tmp_path / "out.csv"
    write_csv(
        src,
        "k,v\n0,10\n1,5\n0,7\n2,1\n2,2\n1,3\n",
    )

    (
        Pipeline()
        .read_csv(str(src))
        .group_by(["k"])
        .agg({"v": "sum"})
        .sort(["sum_v"], descending=True)
        .write_csv(str(out))
        .execute()
    )

    lines = out.read_text(encoding="utf-8").splitlines()
    assert lines[0] == "k,sum_v"
    assert lines[1] == "0,17"
    assert lines[2] == "1,8"
    assert lines[3] == "2,3"


def test_count_all_and_avg(tmp_path):
    src = tmp_path / "in.csv"
    out = tmp_path / "out.csv"
    write_csv(src, "g,x\na,1\na,3\nb,10\n")

    p = (
        Pipeline()
        .read_csv(str(src))
        .group_by(["g"])
        .agg({"count_all": "count_all", "x": "avg"})
        .write_csv(str(out))
    )
    assert p.num_stages() == 1
    p.execute()

    lines = out.read_text(encoding="utf-8").splitlines()
    assert lines[0] == "g,count_all,avg_x"
    assert sorted(lines[1:]) == ["a,2,2", "b,1,10"]  # agg order is not guaranteed


def test_dedup(tmp_path):
    src = tmp_path / "in.csv"
    out = tmp_path / "out.csv"
    write_csv(src, "id,v\n1,a\n1,a\n2,b\n1,a\n")

    (
        Pipeline()
        .read_csv(str(src))
        .dedup(["id", "v"])
        .write_csv(str(out))
        .execute()
    )

    lines = out.read_text(encoding="utf-8").splitlines()
    assert lines == ["id,v", "1,a", "2,b"]


def test_invalid_expression_raises(tmp_path):
    src = tmp_path / "in.csv"
    write_csv(src, "a\n1\n")

    p = Pipeline().read_csv(str(src))
    with pytest.raises(TptError, match="expression"):
        p.filter("a >")  # parse error is raised immediately
    with pytest.raises(TptError, match="expression"):
        p.map({"b": "a +"})


def test_unknown_agg_fn_raises(tmp_path):
    src = tmp_path / "in.csv"
    write_csv(src, "g,x\na,1\n")

    p = Pipeline().read_csv(str(src)).group_by(["g"])
    with pytest.raises(TptError, match="unknown aggregate"):
        p.agg({"x": "median"})