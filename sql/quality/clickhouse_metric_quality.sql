-- ClickHouse Gold metric quality checks.
-- Range checks should return zero violations in a healthy mart.

SELECT
    'gold_campaign_metrics_minutely_negative_rates' AS check_name,
    count() AS violations
FROM gold_campaign_metrics_minutely
WHERE ctr < 0 OR cvr < 0 OR roas < 0;

SELECT
    'gold_campaign_metrics_minutely_negative_money' AS check_name,
    count() AS violations
FROM gold_campaign_metrics_minutely
WHERE cost < 0 OR revenue < 0;

SELECT
    'gold_campaign_metrics_minutely_ctr_division_by_zero' AS check_name,
    count() AS violations
FROM gold_campaign_metrics_minutely
WHERE impressions = 0 AND ctr != 0;

SELECT
    'gold_campaign_metrics_minutely_cvr_division_by_zero' AS check_name,
    count() AS violations
FROM gold_campaign_metrics_minutely
WHERE clicks = 0 AND cvr != 0;

SELECT
    'gold_campaign_metrics_minutely_roas_division_by_zero' AS check_name,
    count() AS violations
FROM gold_campaign_metrics_minutely
WHERE cost = 0 AND roas != 0;

SELECT
    'gold_campaign_metrics_daily_negative_rates' AS check_name,
    count() AS violations
FROM gold_campaign_metrics_daily
WHERE ctr < 0 OR cvr < 0 OR roas < 0;

SELECT
    'gold_advertiser_mart_daily_negative_rates' AS check_name,
    count() AS violations
FROM gold_advertiser_mart_daily
WHERE ctr < 0 OR cvr < 0 OR roas < 0;
