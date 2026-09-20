"""Date/timestamp behaviour in the Python wrapper."""

from tpt_streamforge import Pipeline


def write_text(path, content):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8", newline="")


def test_dates_infer_and_survive_roundtrip(tmp_path):
    src = tmp_path / "in.csv"
    out = tmp_path / "out.csv"
    write_text(src, "day,v\n2024-01-15,1\n2024-03-01,2\n")

    rows = (
        Pipeline()
        .read_csv(str(src))
        .filter("day >= '2024-02-01'")
        .collect()
    )
    # Dates compare against ISO strings and render as ISO strings.
    assert rows == [{"day": "2024-03-01", "v": 2}]

    (
        Pipeline()
        .read_csv(str(src))
        .write_csv(str(out))
        .execute()
    )
    assert out.read_text(encoding="utf-8") == "day,v\n2024-01-15,1\n2024-03-01,2\n"


def test_timestamps_in_json_output(tmp_path):
    src = tmp_path / "in.csv"
    out = tmp_path / "out.json"
    write_text(src, "at\n2024-01-01T12:00:00Z\n")

    (
        Pipeline()
        .read_csv(str(src))
        .write_json(str(out))
        .execute()
    )
    assert out.read_text(encoding="utf-8") == '[{"at":"2024-01-01T12:00:00Z"}]'


def test_sort_by_date_column(tmp_path):
    src = tmp_path / "in.csv"
    out = tmp_path / "out.csv"
    write_text(src, "day\n2024-03-01\n2023-01-01\n2024-01-15\n")

    (
        Pipeline()
        .read_csv(str(src))
        .sort(["day"])
        .write_csv(str(out))
        .execute()
    )
    assert (
        out.read_text(encoding="utf-8")
        == "day\n2023-01-01\n2024-01-15\n2024-03-01\n"
    )
