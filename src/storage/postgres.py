"""PostgreSQL storage helpers."""

from __future__ import annotations

from typing import Any


class PostgresBronzeWriter:
    """Writes successfully published raw events into PostgreSQL Bronze storage."""

    def __init__(self, dsn: str) -> None:
        try:
            import psycopg2
        except ImportError as exc:
            raise RuntimeError(
                "psycopg2-binary is required for PostgreSQL Bronze writes. "
                "Install dependencies with: python3 -m pip install -r requirements.txt"
            ) from exc

        self._connection = psycopg2.connect(dsn)
        self._ensure_bronze_schema()

    def insert_ad_event(
        self,
        *,
        payload: dict[str, Any],
        source_file: str,
        source_line_number: int,
        collector_run_id: str,
        kafka_key: str,
        kafka_topic: str,
        kafka_partition: int | None,
        kafka_offset: int | None,
    ) -> None:
        from psycopg2.extras import Json

        with self._connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO bronze_ad_events (
                    event_id,
                    raw_payload,
                    source_file,
                    source_line_number,
                    collector_run_id,
                    kafka_key,
                    kafka_topic,
                    kafka_partition,
                    kafka_offset,
                    event_time,
                    ingestion_time
                )
                VALUES (
                    %(event_id)s,
                    %(raw_payload)s,
                    %(source_file)s,
                    %(source_line_number)s,
                    %(collector_run_id)s,
                    %(kafka_key)s,
                    %(kafka_topic)s,
                    %(kafka_partition)s,
                    %(kafka_offset)s,
                    NULLIF(%(event_time)s, '')::timestamptz,
                    NULLIF(%(ingestion_time)s, '')::timestamptz
                )
                """,
                {
                    "event_id": payload.get("event_id"),
                    "raw_payload": Json(payload),
                    "source_file": source_file,
                    "source_line_number": source_line_number,
                    "collector_run_id": collector_run_id,
                    "kafka_key": kafka_key,
                    "kafka_topic": kafka_topic,
                    "kafka_partition": kafka_partition,
                    "kafka_offset": kafka_offset,
                    "event_time": payload.get("event_time") or "",
                    "ingestion_time": payload.get("ingestion_time") or "",
                },
            )
        self._connection.commit()

    def close(self) -> None:
        self._connection.close()

    def _ensure_bronze_schema(self) -> None:
        with self._connection.cursor() as cursor:
            cursor.execute(
                """
                ALTER TABLE bronze_ad_events
                    ADD COLUMN IF NOT EXISTS source_line_number INTEGER,
                    ADD COLUMN IF NOT EXISTS collector_run_id TEXT,
                    ADD COLUMN IF NOT EXISTS kafka_key TEXT
                """
            )
            cursor.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_bronze_ad_events_collector_run_id
                    ON bronze_ad_events (collector_run_id)
                """
            )
            cursor.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_bronze_ad_events_source_position
                    ON bronze_ad_events (source_file, source_line_number)
                """
            )
        self._connection.commit()
