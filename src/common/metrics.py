"""Metric calculation helpers shared by tests and batch utilities."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal


@dataclass(frozen=True)
class MetricRates:
    ctr: Decimal
    cvr: Decimal
    roas: Decimal


def safe_divide(numerator: int | Decimal, denominator: int | Decimal) -> Decimal:
    denominator_value = Decimal(str(denominator))
    if denominator_value == 0:
        return Decimal("0")
    return Decimal(str(numerator)) / denominator_value


def calculate_metric_rates(
    impressions: int,
    clicks: int,
    conversions: int,
    cost: Decimal,
    revenue: Decimal,
) -> MetricRates:
    return MetricRates(
        ctr=safe_divide(clicks, impressions),
        cvr=safe_divide(conversions, clicks),
        roas=safe_divide(revenue, cost),
    )
