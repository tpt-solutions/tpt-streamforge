"""Telemetry API tests: `on_progress` hook and `stage_stats`."""

from tpt_streamforge import Pipeline


def write_csv(path, content):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8", newline="")


def test_on_progress_receives_all_event_kinds(tmp_path):
    src = tmp_path / "in.csv"
    out = tmp_path / "out.csv"
    write_csv(src, "id,score\n1,10\n2,50\n3,90\n")

    events = []
    stats = (
        Pipeline()
        .read_csv(str(src), chunk_size=2)
        .on_progress(events.append)
        .filter("score >= 50")
        .write_csv(str(out))
        .execute()
    )

    kinds = [e["event"] for e in events]
    assert kinds.count("source_batch") == 2  # 3 rows / chunk 2
    assert kinds.count("stage_batch") == 2
    assert kinds.count("sink_batch") == 2
    assert kinds[-1] == "done"

    source = next(e for e in events if e["event"] == "source_batch")
    assert source == {"event": "source_batch", "rows": 2, "total_rows": 2, "batches": 1}

    stage = next(e for e in events if e["event"] == "stage_batch")
    assert stage["stage"] == "filter"
    # First chunk carries rows (1,10) and (2,50); only the latter survives.
    assert stage["rows_in"] == 2
    assert stage["rows_out"] == 1

    sink = next(e for e in events if e["event"] == "sink_batch")
    assert sink["rows"] == 1

    done = events[-1]
    assert done["rows"] == stats["rows"] == 3
    assert done["batches"] == stats["batches"] == 2
    assert done["elapsed_ms"] >= 0


def test_stage_stats_after_execute(tmp_path):
    src = tmp_path / "in.csv"
    out = tmp_path / "out.csv"
    write_csv(src, "id,v\n1,10\n2,20\n3,30\n")

    pipeline = (
        Pipeline()
        .read_csv(str(src))
        .filter("v >= 20")
        .write_csv(str(out))
    )
    assert pipeline.stage_stats() == []  # empty before the first run

    pipeline.execute()
    stats = pipeline.stage_stats()
    assert len(stats) == 1
    assert stats[0]["name"] == "filter"
    assert stats[0]["rows_in"] == 3
    assert stats[0]["rows_out"] == 2
    assert stats[0]["batches"] == 1
    assert stats[0]["elapsed_ms"] >= 0
    assert stats[0]["rows_per_sec"] >= 0


def test_progress_callback_errors_do_not_crash_execute(tmp_path):
    src = tmp_path / "in.csv"
    out = tmp_path / "out.csv"
    write_csv(src, "id\n1\n2\n")

    def boom(_event):
        raise RuntimeError("callback exploded")

    stats = (
        Pipeline()
        .read_csv(str(src))
        .on_progress(boom)
        .write_csv(str(out))
        .execute()
    )
    assert stats["rows"] == 2
