"""Validation helpers for advertising event payloads."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Any

from src.common.event_schema import AdEvent, EventType


REQUIRED_FIELDS = (
    "event_id",
    "event_type",
    "event_time",
    "ingestion_time",
    "user_id",
    "campaign_id",
    "ad_id",
    "advertiser_id",
    "cost",
    "revenue",
)


@dataclass(frozen=True)
class ValidationResult:
    is_valid: bool
    errors: tuple[str, ...]


def validate_event_payload(payload: dict[str, Any]) -> ValidationResult:
    errors: list[str] = []

    for field_name in REQUIRED_FIELDS:
        if _is_missing(payload.get(field_name)):
            errors.append(f"{field_name} is required")

    event_type_value = payload.get("event_type")
    if not _is_missing(event_type_value):
        try:
            EventType(str(event_type_value))
        except ValueError:
            errors.append("event_type must be one of: impression, click, conversion")

    for time_field in ("event_time", "ingestion_time"):
        value = payload.get(time_field)
        if not _is_missing(value) and _parse_datetime(value) is None:
            errors.append(f"{time_field} must be a valid ISO-8601 datetime")

    for money_field in ("cost", "revenue"):
        value = payload.get(money_field)
        if not _is_missing(value):
            amount = _parse_decimal(value)
            if amount is None:
                errors.append(f"{money_field} must be numeric")
            elif not amount.is_finite():
                errors.append(f"{money_field} must be finite")
            elif amount < 0:
                errors.append(f"{money_field} must be greater than or equal to 0")

    if event_type_value == EventType.CONVERSION.value and _is_missing(payload.get("conversion_id")):
        errors.append("conversion_id is required for conversion events")

    conversion_value = payload.get("conversion_value")
    if not _is_missing(conversion_value):
        parsed_conversion_value = _parse_decimal(conversion_value)
        if parsed_conversion_value is None:
            errors.append("conversion_value must be numeric")
        elif not parsed_conversion_value.is_finite():
            errors.append("conversion_value must be finite")
        elif parsed_conversion_value < 0:
            errors.append("conversion_value must be greater than or equal to 0")

    return ValidationResult(is_valid=not errors, errors=tuple(errors))


def parse_ad_event(payload: dict[str, Any]) -> AdEvent:
    result = validate_event_payload(payload)
    if not result.is_valid:
        raise ValueError("; ".join(result.errors))

    return AdEvent(
        event_id=str(payload["event_id"]),
        event_type=EventType(str(payload["event_type"])),
        event_time=_require_datetime(payload["event_time"]),
        ingestion_time=_require_datetime(payload["ingestion_time"]),
        user_id=str(payload["user_id"]),
        session_id=_optional_str(payload.get("session_id")),
        campaign_id=str(payload["campaign_id"]),
        ad_id=str(payload["ad_id"]),
        advertiser_id=str(payload["advertiser_id"]),
        category=_optional_str(payload.get("category")),
        device_type=_optional_str(payload.get("device_type")),
        os=_optional_str(payload.get("os")),
        country=_optional_str(payload.get("country")),
        placement=_optional_str(payload.get("placement")),
        cost=_require_decimal(payload["cost"]),
        revenue=_require_decimal(payload["revenue"]),
        conversion_id=_optional_str(payload.get("conversion_id")),
        conversion_value=_optional_decimal(payload.get("conversion_value")),
        attributed_click_id=_optional_str(payload.get("attributed_click_id")),
        raw_payload=dict(payload),
    )


def _is_missing(value: Any) -> bool:
    return value is None or value == ""


def _optional_str(value: Any) -> str | None:
    if _is_missing(value):
        return None
    return str(value)


def _parse_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value
    if not isinstance(value, str):
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def _require_datetime(value: Any) -> datetime:
    parsed = _parse_datetime(value)
    if parsed is None:
        raise ValueError(f"invalid datetime: {value}")
    return parsed


def _parse_decimal(value: Any) -> Decimal | None:
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


def _require_decimal(value: Any) -> Decimal:
    parsed = _parse_decimal(value)
    if parsed is None:
        raise ValueError(f"invalid decimal: {value}")
    return parsed


def _optional_decimal(value: Any) -> Decimal | None:
    if _is_missing(value):
        return None
    return _require_decimal(value)
