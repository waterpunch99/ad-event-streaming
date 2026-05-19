CREATE TABLE IF NOT EXISTS bronze_ad_events (
    id BIGSERIAL PRIMARY KEY,
    event_id TEXT,
    raw_payload JSONB NOT NULL,
    source_file TEXT,
    kafka_topic TEXT DEFAULT 'ad-events-raw',
    kafka_partition INTEGER,
    kafka_offset BIGINT,
    event_time TIMESTAMPTZ,
    ingestion_time TIMESTAMPTZ,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_bronze_ad_events_event_id
    ON bronze_ad_events (event_id);

CREATE INDEX IF NOT EXISTS idx_bronze_ad_events_event_time
    ON bronze_ad_events (event_time);

CREATE INDEX IF NOT EXISTS idx_bronze_ad_events_campaign_id
    ON bronze_ad_events ((raw_payload ->> 'campaign_id'));

CREATE INDEX IF NOT EXISTS idx_bronze_ad_events_user_id
    ON bronze_ad_events ((raw_payload ->> 'user_id'));

CREATE TABLE IF NOT EXISTS silver_ad_events (
    id BIGSERIAL PRIMARY KEY,
    event_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    event_time TIMESTAMPTZ NOT NULL,
    ingestion_time TIMESTAMPTZ NOT NULL,
    user_id TEXT NOT NULL,
    session_id TEXT,
    campaign_id TEXT NOT NULL,
    ad_id TEXT NOT NULL,
    advertiser_id TEXT NOT NULL,
    category TEXT,
    device_type TEXT,
    os TEXT,
    country TEXT,
    placement TEXT,
    cost NUMERIC(18, 6) NOT NULL DEFAULT 0,
    revenue NUMERIC(18, 6) NOT NULL DEFAULT 0,
    conversion_id TEXT,
    conversion_value NUMERIC(18, 6),
    attributed_click_id TEXT,
    raw_payload JSONB NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT chk_silver_ad_events_event_type
        CHECK (event_type IN ('impression', 'click', 'conversion')),
    CONSTRAINT chk_silver_ad_events_cost_non_negative
        CHECK (cost >= 0),
    CONSTRAINT chk_silver_ad_events_revenue_non_negative
        CHECK (revenue >= 0),
    CONSTRAINT chk_silver_ad_events_conversion_id_required
        CHECK (event_type <> 'conversion' OR conversion_id IS NOT NULL)
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_silver_ad_events_event_id
    ON silver_ad_events (event_id);

CREATE UNIQUE INDEX IF NOT EXISTS ux_silver_ad_events_conversion_id
    ON silver_ad_events (conversion_id)
    WHERE conversion_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_silver_ad_events_event_time
    ON silver_ad_events (event_time);

CREATE INDEX IF NOT EXISTS idx_silver_ad_events_campaign_id
    ON silver_ad_events (campaign_id);

CREATE INDEX IF NOT EXISTS idx_silver_ad_events_user_id
    ON silver_ad_events (user_id);

CREATE INDEX IF NOT EXISTS idx_silver_ad_events_advertiser_campaign_time
    ON silver_ad_events (advertiser_id, campaign_id, event_time);

CREATE TABLE IF NOT EXISTS silver_invalid_events (
    id BIGSERIAL PRIMARY KEY,
    event_id TEXT,
    event_type TEXT,
    event_time TIMESTAMPTZ,
    ingestion_time TIMESTAMPTZ,
    user_id TEXT,
    campaign_id TEXT,
    ad_id TEXT,
    advertiser_id TEXT,
    raw_payload JSONB NOT NULL,
    validation_errors TEXT[] NOT NULL,
    kafka_topic TEXT DEFAULT 'ad-events-raw',
    kafka_partition INTEGER,
    kafka_offset BIGINT,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_silver_invalid_events_event_id
    ON silver_invalid_events (event_id);

CREATE INDEX IF NOT EXISTS idx_silver_invalid_events_event_time
    ON silver_invalid_events (event_time);

CREATE INDEX IF NOT EXISTS idx_silver_invalid_events_campaign_id
    ON silver_invalid_events (campaign_id);

CREATE INDEX IF NOT EXISTS idx_silver_invalid_events_user_id
    ON silver_invalid_events (user_id);

CREATE TABLE IF NOT EXISTS silver_duplicate_events (
    id BIGSERIAL PRIMARY KEY,
    event_id TEXT,
    event_type TEXT,
    event_time TIMESTAMPTZ,
    ingestion_time TIMESTAMPTZ,
    user_id TEXT,
    campaign_id TEXT,
    ad_id TEXT,
    advertiser_id TEXT,
    conversion_id TEXT,
    duplicate_key_type TEXT NOT NULL,
    duplicate_key_value TEXT NOT NULL,
    raw_payload JSONB NOT NULL,
    detected_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT chk_silver_duplicate_events_key_type
        CHECK (duplicate_key_type IN ('event_id', 'conversion_id'))
);

CREATE INDEX IF NOT EXISTS idx_silver_duplicate_events_event_id
    ON silver_duplicate_events (event_id);

CREATE INDEX IF NOT EXISTS idx_silver_duplicate_events_event_time
    ON silver_duplicate_events (event_time);

CREATE INDEX IF NOT EXISTS idx_silver_duplicate_events_campaign_id
    ON silver_duplicate_events (campaign_id);

CREATE INDEX IF NOT EXISTS idx_silver_duplicate_events_user_id
    ON silver_duplicate_events (user_id);

CREATE INDEX IF NOT EXISTS idx_silver_duplicate_events_duplicate_key
    ON silver_duplicate_events (duplicate_key_type, duplicate_key_value);

CREATE TABLE IF NOT EXISTS silver_late_events (
    id BIGSERIAL PRIMARY KEY,
    event_id TEXT,
    event_type TEXT,
    event_time TIMESTAMPTZ NOT NULL,
    ingestion_time TIMESTAMPTZ NOT NULL,
    user_id TEXT,
    session_id TEXT,
    campaign_id TEXT,
    ad_id TEXT,
    advertiser_id TEXT,
    category TEXT,
    device_type TEXT,
    os TEXT,
    country TEXT,
    placement TEXT,
    cost NUMERIC(18, 6) NOT NULL DEFAULT 0,
    revenue NUMERIC(18, 6) NOT NULL DEFAULT 0,
    conversion_id TEXT,
    conversion_value NUMERIC(18, 6),
    attributed_click_id TEXT,
    watermark_time TIMESTAMPTZ NOT NULL,
    raw_payload JSONB NOT NULL,
    detected_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT chk_silver_late_events_event_type
        CHECK (event_type IN ('impression', 'click', 'conversion')),
    CONSTRAINT chk_silver_late_events_cost_non_negative
        CHECK (cost >= 0),
    CONSTRAINT chk_silver_late_events_revenue_non_negative
        CHECK (revenue >= 0)
);

CREATE INDEX IF NOT EXISTS idx_silver_late_events_event_id
    ON silver_late_events (event_id);

CREATE INDEX IF NOT EXISTS idx_silver_late_events_event_time
    ON silver_late_events (event_time);

CREATE INDEX IF NOT EXISTS idx_silver_late_events_campaign_id
    ON silver_late_events (campaign_id);

CREATE INDEX IF NOT EXISTS idx_silver_late_events_user_id
    ON silver_late_events (user_id);

CREATE INDEX IF NOT EXISTS idx_silver_late_events_advertiser_campaign_time
    ON silver_late_events (advertiser_id, campaign_id, event_time);
