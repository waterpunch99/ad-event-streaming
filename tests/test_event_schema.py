from decimal import Decimal

import pytest

from src.common.event_schema import EventType
from src.common.validators import parse_ad_event, validate_event_payload


def valid_payload(**overrides):
    payload = {
        "event_id": "evt-001",
        "event_type": "impression",
        "event_time": "2026-05-19T08:00:00Z",
        "ingestion_time": "2026-05-19T08:00:03Z",
        "user_id": "user-001",
        "session_id": "session-001",
        "campaign_id": "campaign-001",
        "ad_id": "ad-001",
        "advertiser_id": "advertiser-001",
        "category": "sports",
        "device_type": "mobile",
        "os": "ios",
        "country": "KR",
        "placement": "feed",
        "cost": "12.345",
        "revenue": "0",
    }
    payload.update(overrides)
    return payload


def test_valid_impression_payload_passes_validation():
    result = validate_event_payload(valid_payload())

    assert result.is_valid
    assert result.errors == ()


def test_parse_ad_event_returns_typed_dataclass():
    event = parse_ad_event(valid_payload(cost="1.25", revenue="3.50"))

    assert event.event_type == EventType.IMPRESSION
    assert event.cost == Decimal("1.25")
    assert event.revenue == Decimal("3.50")
    assert event.raw_payload is not None


@pytest.mark.parametrize(
    "field_name",
    [
        "event_id",
        "event_type",
        "event_time",
        "ingestion_time",
        "user_id",
        "campaign_id",
        "ad_id",
        "advertiser_id",
    ],
)
def test_required_fields_cannot_be_null(field_name):
    result = validate_event_payload(valid_payload(**{field_name: None}))

    assert not result.is_valid
    assert f"{field_name} is required" in result.errors


def test_event_type_must_be_supported():
    result = validate_event_payload(valid_payload(event_type="view"))

    assert not result.is_valid
    assert "event_type must be one of: impression, click, conversion" in result.errors


@pytest.mark.parametrize("money_field", ["cost", "revenue"])
def test_money_fields_cannot_be_negative(money_field):
    result = validate_event_payload(valid_payload(**{money_field: "-0.01"}))

    assert not result.is_valid
    assert f"{money_field} must be greater than or equal to 0" in result.errors


@pytest.mark.parametrize("money_field", ["cost", "revenue"])
def test_money_fields_must_be_numeric(money_field):
    result = validate_event_payload(valid_payload(**{money_field: "not-a-number"}))

    assert not result.is_valid
    assert f"{money_field} must be numeric" in result.errors


@pytest.mark.parametrize("money_field", ["cost", "revenue"])
def test_money_fields_must_be_finite(money_field):
    result = validate_event_payload(valid_payload(**{money_field: "NaN"}))

    assert not result.is_valid
    assert f"{money_field} must be finite" in result.errors


def test_conversion_event_requires_conversion_id():
    result = validate_event_payload(valid_payload(event_type="conversion", conversion_id=None))

    assert not result.is_valid
    assert "conversion_id is required for conversion events" in result.errors


def test_conversion_event_allows_attributed_click_id():
    result = validate_event_payload(
        valid_payload(
            event_type="conversion",
            conversion_id="conv-001",
            conversion_value="25.50",
            attributed_click_id="click-001",
            revenue="25.50",
        )
    )

    assert result.is_valid


def test_invalid_datetime_fails_validation():
    result = validate_event_payload(valid_payload(event_time="not-a-datetime"))

    assert not result.is_valid
    assert "event_time must be a valid ISO-8601 datetime" in result.errors


def test_parse_invalid_payload_raises_value_error():
    with pytest.raises(ValueError):
        parse_ad_event(valid_payload(event_id=None))
