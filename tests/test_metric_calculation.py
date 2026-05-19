from decimal import Decimal

from src.common.metrics import calculate_metric_rates, safe_divide


def test_safe_divide_returns_zero_when_denominator_is_zero():
    assert safe_divide(10, 0) == Decimal("0")
    assert safe_divide(Decimal("10.0"), Decimal("0")) == Decimal("0")


def test_metric_rates_are_calculated_from_counts_and_money():
    rates = calculate_metric_rates(
        impressions=100,
        clicks=25,
        conversions=5,
        cost=Decimal("20.00"),
        revenue=Decimal("80.00"),
    )

    assert rates.ctr == Decimal("0.25")
    assert rates.cvr == Decimal("0.2")
    assert rates.roas == Decimal("4")


def test_metric_rates_handle_all_zero_denominators():
    rates = calculate_metric_rates(
        impressions=0,
        clicks=0,
        conversions=0,
        cost=Decimal("0"),
        revenue=Decimal("10.00"),
    )

    assert rates.ctr == Decimal("0")
    assert rates.cvr == Decimal("0")
    assert rates.roas == Decimal("0")
