CREATE DATABASE IF NOT EXISTS ad_pipeline;

USE ad_pipeline;

CREATE TABLE IF NOT EXISTS gold_campaign_metrics_minutely (
    metric_date Date DEFAULT toDate(window_start),
    window_start DateTime64(3, 'UTC'),
    window_end DateTime64(3, 'UTC'),
    campaign_id String,
    advertiser_id String,
    impressions UInt64,
    clicks UInt64,
    conversions UInt64,
    ctr Float64,
    cvr Float64,
    cost Decimal(18, 6),
    revenue Decimal(18, 6),
    roas Float64,
    updated_at DateTime DEFAULT now()
)
ENGINE = MergeTree
PARTITION BY metric_date
ORDER BY (campaign_id, advertiser_id, window_start);

CREATE TABLE IF NOT EXISTS gold_campaign_metrics_hourly (
    metric_date Date DEFAULT toDate(window_start),
    window_start DateTime64(3, 'UTC'),
    window_end DateTime64(3, 'UTC'),
    campaign_id String,
    advertiser_id String,
    impressions UInt64,
    clicks UInt64,
    conversions UInt64,
    ctr Float64,
    cvr Float64,
    cost Decimal(18, 6),
    revenue Decimal(18, 6),
    roas Float64,
    updated_at DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY metric_date
ORDER BY (campaign_id, advertiser_id, window_start);

CREATE TABLE IF NOT EXISTS gold_campaign_metrics_daily (
    metric_date Date,
    campaign_id String,
    advertiser_id String,
    impressions UInt64,
    clicks UInt64,
    conversions UInt64,
    ctr Float64,
    cvr Float64,
    cost Decimal(18, 6),
    revenue Decimal(18, 6),
    roas Float64,
    updated_at DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY metric_date
ORDER BY (campaign_id, advertiser_id, metric_date);

CREATE TABLE IF NOT EXISTS gold_advertiser_mart_daily (
    metric_date Date,
    advertiser_id String,
    campaign_count UInt64,
    impressions UInt64,
    clicks UInt64,
    conversions UInt64,
    ctr Float64,
    cvr Float64,
    cost Decimal(18, 6),
    revenue Decimal(18, 6),
    roas Float64,
    updated_at DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY metric_date
ORDER BY (advertiser_id, metric_date);
