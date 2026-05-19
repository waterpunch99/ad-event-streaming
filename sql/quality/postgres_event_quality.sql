-- PostgreSQL Bronze/Silver event quality checks.
-- Each query should return zero rows or zero violations in a healthy pipeline.

SELECT 'silver_ad_events_null_required_fields' AS check_name, COUNT(*) AS violations
FROM silver_ad_events
WHERE event_id IS NULL
   OR event_type IS NULL
   OR event_time IS NULL
   OR ingestion_time IS NULL
   OR user_id IS NULL
   OR campaign_id IS NULL
   OR ad_id IS NULL
   OR advertiser_id IS NULL;

SELECT 'silver_ad_events_invalid_event_type' AS check_name, COUNT(*) AS violations
FROM silver_ad_events
WHERE event_type NOT IN ('impression', 'click', 'conversion');

SELECT 'silver_ad_events_negative_money' AS check_name, COUNT(*) AS violations
FROM silver_ad_events
WHERE cost < 0 OR revenue < 0;

SELECT 'silver_ad_events_conversion_missing_conversion_id' AS check_name, COUNT(*) AS violations
FROM silver_ad_events
WHERE event_type = 'conversion' AND conversion_id IS NULL;

SELECT 'silver_ad_events_duplicate_event_id' AS check_name, COUNT(*) AS violations
FROM (
    SELECT event_id
    FROM silver_ad_events
    GROUP BY event_id
    HAVING COUNT(*) > 1
) duplicated;

SELECT 'silver_ad_events_duplicate_conversion_id' AS check_name, COUNT(*) AS violations
FROM (
    SELECT conversion_id
    FROM silver_ad_events
    WHERE conversion_id IS NOT NULL
    GROUP BY conversion_id
    HAVING COUNT(*) > 1
) duplicated;

SELECT 'silver_invalid_events_routed' AS check_name, COUNT(*) AS invalid_events
FROM silver_invalid_events;

SELECT 'silver_duplicate_events_routed' AS check_name, COUNT(*) AS duplicate_events
FROM silver_duplicate_events;

SELECT 'silver_late_events_routed' AS check_name, COUNT(*) AS late_events
FROM silver_late_events;
