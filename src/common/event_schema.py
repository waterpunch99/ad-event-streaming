"""Advertising event schema used by Python generators and tests."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from enum import StrEnum
from typing import Any


class EventType(StrEnum):
    IMPRESSION = "impression"
    CLICK = "click"
    CONVERSION = "conversion"


@dataclass(frozen=True)
class AdEvent:
    event_id: str
    event_type: EventType
    event_time: datetime
    ingestion_time: datetime
    user_id: str
    session_id: str | None
    campaign_id: str
    ad_id: str
    advertiser_id: str
    category: str | None
    device_type: str | None
    os: str | None
    country: str | None
    placement: str | None
    cost: Decimal
    revenue: Decimal
    conversion_id: str | None = None
    conversion_value: Decimal | None = None
    attributed_click_id: str | None = None
    raw_payload: dict[str, Any] | None = None
