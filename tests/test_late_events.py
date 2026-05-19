from datetime import datetime

from src.generator.event_generator import generate_events


def test_generator_can_create_late_events():
    events = generate_events(
        count=20,
        invalid_rate=0,
        duplicate_rate=0,
        late_rate=1,
        seed=11,
    )

    late_events = [
        event
        for event in events
        if _delay_seconds(event["ingestion_time"], event["event_time"]) > 600
    ]

    assert len(late_events) == 20


def _delay_seconds(ingestion_time: str, event_time: str) -> float:
    ingestion = datetime.fromisoformat(ingestion_time.replace("Z", "+00:00"))
    event = datetime.fromisoformat(event_time.replace("Z", "+00:00"))
    return (ingestion - event).total_seconds()
