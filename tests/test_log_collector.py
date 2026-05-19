import json
from dataclasses import dataclass
from typing import Any

from src.collector.log_collector import collect_landing_files


@dataclass(frozen=True)
class _KafkaMetadata:
    topic: str
    partition: int
    offset: int


class _SendFuture:
    def __init__(self, metadata: _KafkaMetadata) -> None:
        self._metadata = metadata

    def get(self, timeout: int) -> _KafkaMetadata:
        assert timeout == 30
        return self._metadata


class _FakeProducer:
    def __init__(self) -> None:
        self.sent: list[tuple[str, bytes, bytes]] = []

    def send(self, topic: str, key: bytes, value: bytes) -> _SendFuture:
        self.sent.append((topic, key, value))
        return _SendFuture(_KafkaMetadata(topic=topic, partition=0, offset=len(self.sent) - 1))

    def flush(self) -> None:
        pass

    def close(self) -> None:
        pass


class _FakeBronzeWriter:
    def __init__(self) -> None:
        self.records: list[dict[str, Any]] = []
        self.closed = False

    def insert_ad_event(self, **record: Any) -> None:
        self.records.append(record)

    def close(self) -> None:
        self.closed = True


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
    assert result.bronze_records == 0
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
    assert result.bronze_records == 0


def test_collector_writes_bronze_records_after_kafka_publish(tmp_path):
    landing_dir = tmp_path / "landing"
    archive_dir = tmp_path / "archive"
    bad_dir = tmp_path / "bad"
    landing_dir.mkdir()

    input_path = landing_dir / "ad_events_test.jsonl"
    input_path.write_text(
        "\n".join(
            [
                json.dumps(
                    {
                        "event_id": "evt-001",
                        "event_time": "2026-05-20T00:00:00Z",
                        "ingestion_time": "2026-05-20T00:00:02Z",
                        "user_id": "user-001",
                    }
                ),
                json.dumps(
                    {
                        "event_id": "evt-002",
                        "event_time": "2026-05-20T00:01:00Z",
                        "ingestion_time": "2026-05-20T00:01:02Z",
                        "user_id": "user-002",
                    }
                ),
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    producer = _FakeProducer()
    bronze_writer = _FakeBronzeWriter()

    result = collect_landing_files(
        landing_dir=landing_dir,
        archive_dir=archive_dir,
        bad_dir=bad_dir,
        raw_topic="ad-events-raw",
        enable_bronze=True,
        producer=producer,
        bronze_writer=bronze_writer,
        collector_run_id="test-run-001",
    )

    assert result.sent_records == 2
    assert result.bad_records == 0
    assert result.bronze_records == 2
    assert result.collector_run_id == "test-run-001"
    assert len(producer.sent) == 2
    assert producer.sent[0][1] == b"user-001"
    assert producer.sent[1][1] == b"user-002"
    assert len(bronze_writer.records) == 2
    assert bronze_writer.records[0]["payload"]["event_id"] == "evt-001"
    assert bronze_writer.records[0]["source_file"] == "ad_events_test.jsonl"
    assert bronze_writer.records[0]["source_line_number"] == 1
    assert bronze_writer.records[0]["collector_run_id"] == "test-run-001"
    assert bronze_writer.records[0]["kafka_key"] == "user-001"
    assert bronze_writer.records[0]["kafka_topic"] == "ad-events-raw"
    assert bronze_writer.records[0]["kafka_partition"] == 0
    assert bronze_writer.records[0]["kafka_offset"] == 0
    assert bronze_writer.closed is False
