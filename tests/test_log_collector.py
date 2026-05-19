import json

from src.collector.log_collector import collect_landing_files


def test_collector_archives_file_and_writes_bad_records(tmp_path):
    landing_dir = tmp_path / "landing"
    archive_dir = tmp_path / "archive"
    bad_dir = tmp_path / "bad"
    landing_dir.mkdir()

    input_path = landing_dir / "ad_events_test.jsonl"
    input_path.write_text(
        "\n".join(
            [
                json.dumps({"event_id": "evt-001", "user_id": "user-001"}),
                "{bad-json",
                json.dumps({"event_id": "evt-002"}),
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    result = collect_landing_files(
        landing_dir=landing_dir,
        archive_dir=archive_dir,
        bad_dir=bad_dir,
        dry_run=True,
    )

    assert result.processed_files == 1
    assert result.archived_files == 1
    assert result.sent_records == 1
    assert result.bad_records == 2
    assert not input_path.exists()
    assert (archive_dir / "ad_events_test.jsonl").exists()

    bad_files = list(bad_dir.glob("*.bad.jsonl"))
    assert len(bad_files) == 1
    bad_lines = bad_files[0].read_text(encoding="utf-8").splitlines()
    assert len(bad_lines) == 2


def test_collector_ignores_empty_landing_dir(tmp_path):
    result = collect_landing_files(
        landing_dir=tmp_path / "landing",
        archive_dir=tmp_path / "archive",
        bad_dir=tmp_path / "bad",
        dry_run=True,
    )

    assert result.processed_files == 0
    assert result.archived_files == 0
    assert result.sent_records == 0
    assert result.bad_records == 0
