"""Collect JSONL application logs and publish them to Kafka.

The collector owns Kafka publishing. The event generator only writes files.
"""

from __future__ import annotations

import argparse
import json
import shutil
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol


DEFAULT_LANDING_DIR = Path("data/landing")
DEFAULT_ARCHIVE_DIR = Path("data/archive")
DEFAULT_BAD_DIR = Path("data/bad")
DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092"
DEFAULT_RAW_TOPIC = "ad-events-raw"


class Producer(Protocol):
    def send(self, topic: str, key: bytes, value: bytes) -> Any:
        ...

    def flush(self) -> Any:
        ...

    def close(self) -> Any:
        ...


@dataclass(frozen=True)
class CollectorResult:
    processed_files: int
    archived_files: int
    sent_records: int
    bad_records: int


def collect_landing_files(
    landing_dir: Path = DEFAULT_LANDING_DIR,
    archive_dir: Path = DEFAULT_ARCHIVE_DIR,
    bad_dir: Path = DEFAULT_BAD_DIR,
    bootstrap_servers: str = DEFAULT_BOOTSTRAP_SERVERS,
    raw_topic: str = DEFAULT_RAW_TOPIC,
    dry_run: bool = False,
) -> CollectorResult:
    landing_dir.mkdir(parents=True, exist_ok=True)
    archive_dir.mkdir(parents=True, exist_ok=True)
    bad_dir.mkdir(parents=True, exist_ok=True)

    producer = None if dry_run else _create_kafka_producer(bootstrap_servers)
    processed_files = 0
    archived_files = 0
    sent_records = 0
    bad_records = 0

    try:
        for input_path in sorted(landing_dir.glob("*.jsonl")):
            processed_files += 1
            file_result = _process_file(
                input_path=input_path,
                archive_dir=archive_dir,
                bad_dir=bad_dir,
                producer=producer,
                raw_topic=raw_topic,
                dry_run=dry_run,
            )
            archived_files += 1
            sent_records += file_result.sent_records
            bad_records += file_result.bad_records
    finally:
        if producer is not None:
            producer.flush()
            producer.close()

    return CollectorResult(
        processed_files=processed_files,
        archived_files=archived_files,
        sent_records=sent_records,
        bad_records=bad_records,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Collect JSONL logs and publish them to Kafka.")
    parser.add_argument("--landing-dir", type=Path, default=DEFAULT_LANDING_DIR)
    parser.add_argument("--archive-dir", type=Path, default=DEFAULT_ARCHIVE_DIR)
    parser.add_argument("--bad-dir", type=Path, default=DEFAULT_BAD_DIR)
    parser.add_argument("--bootstrap-servers", default=DEFAULT_BOOTSTRAP_SERVERS)
    parser.add_argument("--raw-topic", default=DEFAULT_RAW_TOPIC)
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Parse and archive files without sending records to Kafka.",
    )
    args = parser.parse_args()

    result = collect_landing_files(
        landing_dir=args.landing_dir,
        archive_dir=args.archive_dir,
        bad_dir=args.bad_dir,
        bootstrap_servers=args.bootstrap_servers,
        raw_topic=args.raw_topic,
        dry_run=args.dry_run,
    )
    print(
        "collector finished: "
        f"processed_files={result.processed_files}, "
        f"archived_files={result.archived_files}, "
        f"sent_records={result.sent_records}, "
        f"bad_records={result.bad_records}"
    )


@dataclass(frozen=True)
class _FileResult:
    sent_records: int
    bad_records: int


def _process_file(
    input_path: Path,
    archive_dir: Path,
    bad_dir: Path,
    producer: Producer | None,
    raw_topic: str,
    dry_run: bool,
) -> _FileResult:
    sent_records = 0
    bad_records: list[dict[str, Any]] = []

    with input_path.open("r", encoding="utf-8") as file:
        for line_number, raw_line in enumerate(file, start=1):
            stripped_line = raw_line.strip()
            if not stripped_line:
                continue

            try:
                payload = json.loads(stripped_line)
                user_id = _extract_user_id(payload)
            except (json.JSONDecodeError, ValueError, TypeError) as exc:
                bad_records.append(
                    _bad_record(
                        input_path=input_path,
                        line_number=line_number,
                        raw_line=stripped_line,
                        error=str(exc),
                    )
                )
                continue

            if not dry_run:
                if producer is None:
                    raise RuntimeError("producer is required unless dry_run is enabled")
                producer.send(
                    raw_topic,
                    key=user_id.encode("utf-8"),
                    value=stripped_line.encode("utf-8"),
                )

            sent_records += 1

    if bad_records:
        _write_bad_records(input_path=input_path, bad_dir=bad_dir, records=bad_records)

    archive_path = _unique_path(archive_dir / input_path.name)
    shutil.move(str(input_path), archive_path)

    return _FileResult(sent_records=sent_records, bad_records=len(bad_records))


def _create_kafka_producer(bootstrap_servers: str) -> Producer:
    try:
        from kafka import KafkaProducer
    except ImportError as exc:
        raise RuntimeError(
            "kafka-python is required. Install dependencies with: "
            "python3 -m pip install -r requirements.txt"
        ) from exc

    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        acks="all",
        retries=3,
        linger_ms=10,
    )


def _extract_user_id(payload: Any) -> str:
    if not isinstance(payload, dict):
        raise ValueError("record must be a JSON object")

    user_id = payload.get("user_id")
    if user_id is None or user_id == "":
        raise ValueError("user_id is required for Kafka message key")

    return str(user_id)


def _bad_record(input_path: Path, line_number: int, raw_line: str, error: str) -> dict[str, Any]:
    return {
        "source_file": input_path.name,
        "line_number": line_number,
        "raw_line": raw_line,
        "error": error,
        "collected_at": datetime.now(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z"),
    }


def _write_bad_records(input_path: Path, bad_dir: Path, records: list[dict[str, Any]]) -> Path:
    bad_path = _unique_path(bad_dir / f"{input_path.stem}.bad.jsonl")
    with bad_path.open("w", encoding="utf-8") as file:
        for record in records:
            file.write(json.dumps(record, sort_keys=True, separators=(",", ":")))
            file.write("\n")
    return bad_path


def _unique_path(path: Path) -> Path:
    if not path.exists():
        return path

    timestamp = datetime.now(UTC).strftime("%Y%m%d_%H%M%S_%f")
    return path.with_name(f"{path.stem}_{timestamp}{path.suffix}")


if __name__ == "__main__":
    main()
