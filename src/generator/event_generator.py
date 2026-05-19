"""Generate advertising event logs as JSONL files.

The generator mimics application log output and does not know about Kafka.
"""

from __future__ import annotations

import argparse
import json
import random
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from src.common.event_schema import EventType


DEFAULT_OUTPUT_DIR = Path("data/landing")
EVENT_TYPES = (
    EventType.IMPRESSION.value,
    EventType.CLICK.value,
    EventType.CONVERSION.value,
)
CATEGORIES = ("sports", "finance", "travel", "gaming", "shopping")
DEVICE_TYPES = ("mobile", "desktop", "tablet")
OPERATING_SYSTEMS = ("ios", "android", "windows", "macos", "linux")
COUNTRIES = ("KR", "US", "JP", "SG", "DE")
PLACEMENTS = ("feed", "search", "banner", "video", "story")


def generate_events(
    count: int,
    invalid_rate: float = 0.03,
    duplicate_rate: float = 0.03,
    late_rate: float = 0.05,
    seed: int | None = None,
) -> list[dict[str, Any]]:
    if count < 0:
        raise ValueError("count must be greater than or equal to 0")
    _validate_rate("invalid_rate", invalid_rate)
    _validate_rate("duplicate_rate", duplicate_rate)
    _validate_rate("late_rate", late_rate)

    rng = random.Random(seed)
    now = datetime.now(UTC)
    events: list[dict[str, Any]] = []
    duplicate_candidates: list[dict[str, Any]] = []

    for index in range(count):
        event = _build_valid_event(index=index, base_time=now, rng=rng)

        if duplicate_candidates and rng.random() < duplicate_rate:
            event = _make_duplicate_event(event, rng.choice(duplicate_candidates), rng)
        elif rng.random() < invalid_rate:
            event = _make_invalid_event(event, rng)
        elif rng.random() < late_rate:
            event = _make_late_event(event, rng)

        if _is_duplicate_candidate(event):
            duplicate_candidates.append(event)

        events.append(event)

    return events


def write_jsonl(events: list[dict[str, Any]], output_dir: Path = DEFAULT_OUTPUT_DIR) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
    output_path = output_dir / f"ad_events_{timestamp}.jsonl"

    with output_path.open("w", encoding="utf-8") as file:
        for event in events:
            file.write(json.dumps(event, sort_keys=True, separators=(",", ":")))
            file.write("\n")

    return output_path


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate advertising event JSONL logs.")
    parser.add_argument("--count", type=int, default=1000, help="Number of events to generate.")
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help="Directory where JSONL files are written.",
    )
    parser.add_argument("--invalid-rate", type=float, default=0.03)
    parser.add_argument("--duplicate-rate", type=float, default=0.03)
    parser.add_argument("--late-rate", type=float, default=0.05)
    parser.add_argument("--seed", type=int, default=None)
    args = parser.parse_args()

    events = generate_events(
        count=args.count,
        invalid_rate=args.invalid_rate,
        duplicate_rate=args.duplicate_rate,
        late_rate=args.late_rate,
        seed=args.seed,
    )
    output_path = write_jsonl(events, args.output_dir)
    print(f"generated {len(events)} events: {output_path}")


def _build_valid_event(index: int, base_time: datetime, rng: random.Random) -> dict[str, Any]:
    event_type = EVENT_TYPES[index % len(EVENT_TYPES)] if index < len(EVENT_TYPES) else _pick_event_type(rng)
    ingestion_time = base_time + timedelta(seconds=index)
    event_time = ingestion_time - timedelta(seconds=rng.randint(0, 180))

    campaign_number = rng.randint(1, 20)
    advertiser_number = ((campaign_number - 1) // 4) + 1
    user_number = rng.randint(1, 500)
    ad_number = rng.randint(1, 80)

    cost = _event_cost(event_type, rng)
    revenue = _event_revenue(event_type, rng)

    event: dict[str, Any] = {
        "event_id": f"evt-{uuid.uuid4()}",
        "event_type": event_type,
        "event_time": _format_datetime(event_time),
        "ingestion_time": _format_datetime(ingestion_time),
        "user_id": f"user-{user_number:05d}",
        "session_id": f"session-{rng.randint(1, 10000):06d}",
        "campaign_id": f"campaign-{campaign_number:03d}",
        "ad_id": f"ad-{ad_number:04d}",
        "advertiser_id": f"advertiser-{advertiser_number:03d}",
        "category": rng.choice(CATEGORIES),
        "device_type": rng.choice(DEVICE_TYPES),
        "os": rng.choice(OPERATING_SYSTEMS),
        "country": rng.choice(COUNTRIES),
        "placement": rng.choice(PLACEMENTS),
        "cost": cost,
        "revenue": revenue,
    }

    if event_type == EventType.CONVERSION.value:
        event["conversion_id"] = f"conv-{uuid.uuid4()}"
        event["conversion_value"] = revenue
        event["attributed_click_id"] = f"evt-{uuid.uuid4()}" if rng.random() < 0.75 else None

    return event


def _pick_event_type(rng: random.Random) -> str:
    return rng.choices(
        population=EVENT_TYPES,
        weights=(0.75, 0.20, 0.05),
        k=1,
    )[0]


def _event_cost(event_type: str, rng: random.Random) -> str:
    if event_type == EventType.IMPRESSION.value:
        return _money(rng.uniform(0.001, 0.02))
    if event_type == EventType.CLICK.value:
        return _money(rng.uniform(0.10, 2.50))
    return _money(rng.uniform(0.50, 5.00))


def _event_revenue(event_type: str, rng: random.Random) -> str:
    if event_type != EventType.CONVERSION.value:
        return "0.000000"
    return _money(rng.uniform(5.00, 150.00))


def _money(value: float) -> str:
    return f"{value:.6f}"


def _make_invalid_event(event: dict[str, Any], rng: random.Random) -> dict[str, Any]:
    invalid_event = dict(event)
    mutation = rng.choice(("missing_event_id", "bad_event_type", "negative_cost", "missing_conversion_id"))

    if mutation == "missing_event_id":
        invalid_event["event_id"] = None
    elif mutation == "bad_event_type":
        invalid_event["event_type"] = "view"
    elif mutation == "negative_cost":
        invalid_event["cost"] = "-1.000000"
    else:
        invalid_event["event_type"] = EventType.CONVERSION.value
        invalid_event["conversion_id"] = None
        invalid_event["conversion_value"] = invalid_event.get("revenue", "0.000000")

    return invalid_event


def _make_duplicate_event(
    event: dict[str, Any],
    source_event: dict[str, Any],
    rng: random.Random,
) -> dict[str, Any]:
    duplicate_event = dict(event)
    duplicate_event["event_id"] = source_event["event_id"]

    if (
        source_event.get("event_type") == EventType.CONVERSION.value
        and source_event.get("conversion_id")
        and rng.random() < 0.5
    ):
        duplicate_event["event_type"] = EventType.CONVERSION.value
        duplicate_event["conversion_id"] = source_event["conversion_id"]
        duplicate_event["conversion_value"] = duplicate_event.get("revenue", "0.000000")

    return duplicate_event


def _make_late_event(event: dict[str, Any], rng: random.Random) -> dict[str, Any]:
    late_event = dict(event)
    ingestion_time = _parse_generated_datetime(str(late_event["ingestion_time"]))
    delay_minutes = rng.randint(11, 180)
    late_event["event_time"] = _format_datetime(ingestion_time - timedelta(minutes=delay_minutes))
    return late_event


def _is_duplicate_candidate(event: dict[str, Any]) -> bool:
    return bool(event.get("event_id")) and event.get("event_type") in EVENT_TYPES


def _format_datetime(value: datetime) -> str:
    return value.astimezone(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _parse_generated_datetime(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _validate_rate(name: str, value: float) -> None:
    if value < 0 or value > 1:
        raise ValueError(f"{name} must be between 0 and 1")


if __name__ == "__main__":
    main()
