"""Build hourly and daily advertising report marts in ClickHouse."""

from __future__ import annotations

import os
from datetime import datetime
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context


CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "clickhouse")
CLICKHOUSE_HTTP_PORT = int(os.getenv("CLICKHOUSE_HTTP_PORT", "8123"))
CLICKHOUSE_DATABASE = os.getenv("CLICKHOUSE_DATABASE", "ad_pipeline")
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "")


def execute_clickhouse(sql: str) -> None:
    query = urlencode(
        {
            "database": CLICKHOUSE_DATABASE,
            "user": CLICKHOUSE_USER,
            "password": CLICKHOUSE_PASSWORD,
        }
    )
    request = Request(
        url=f"http://{CLICKHOUSE_HOST}:{CLICKHOUSE_HTTP_PORT}/?{query}",
        data=sql.encode("utf-8"),
        method="POST",
    )
    with urlopen(request, timeout=60) as response:
        response.read()


def query_clickhouse_text(sql: str) -> str:
    query = urlencode(
        {
            "database": CLICKHOUSE_DATABASE,
            "user": CLICKHOUSE_USER,
            "password": CLICKHOUSE_PASSWORD,
        }
    )
    request = Request(
        url=f"http://{CLICKHOUSE_HOST}:{CLICKHOUSE_HTTP_PORT}/?{query}",
        data=sql.encode("utf-8"),
        method="POST",
    )
    with urlopen(request, timeout=60) as response:
        return response.read().decode("utf-8").strip()


def minutely_metrics_table_expr() -> str:
    engine = query_clickhouse_text(
        """
        SELECT engine
        FROM system.tables
        WHERE database = currentDatabase()
          AND name = 'gold_campaign_metrics_minutely'
        FORMAT TabSeparated
        """
    )
    if engine in {"ReplacingMergeTree", "SummingMergeTree", "AggregatingMergeTree", "CollapsingMergeTree"}:
        return "gold_campaign_metrics_minutely FINAL"
    return "gold_campaign_metrics_minutely"


def target_metric_date() -> str:
    context = get_current_context()
    dag_run = context.get("dag_run")
    if dag_run and dag_run.conf and dag_run.conf.get("metric_date"):
        return dag_run.conf["metric_date"]
    data_interval_start = context["data_interval_start"]
    return data_interval_start.strftime("%Y-%m-%d")


@dag(
    dag_id="daily_ad_report_dag",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["ad-pipeline", "clickhouse", "gold-mart"],
)
def daily_ad_report_dag():
    @task
    def build_hourly_campaign_metrics() -> None:
        metric_date = target_metric_date()
        minutely_table = minutely_metrics_table_expr()
        execute_clickhouse(
            f"""
            ALTER TABLE gold_campaign_metrics_hourly
            DELETE WHERE metric_date = toDate('{metric_date}')
            SETTINGS mutations_sync = 1
            """
        )
        execute_clickhouse(
            f"""
            INSERT INTO gold_campaign_metrics_hourly (
                window_start,
                window_end,
                campaign_id,
                advertiser_id,
                impressions,
                clicks,
                conversions,
                ctr,
                cvr,
                cost,
                revenue,
                roas,
                updated_at
            )
            SELECT
                hour_start AS window_start,
                hour_start + INTERVAL 1 HOUR AS window_end,
                campaign_id,
                advertiser_id,
                impressions,
                clicks,
                conversions,
                if(impressions = 0, 0, clicks / impressions) AS ctr,
                if(clicks = 0, 0, conversions / clicks) AS cvr,
                cost,
                revenue,
                if(cost = 0, 0, revenue / cost) AS roas,
                now() AS updated_at
            FROM (
                SELECT
                    toStartOfHour(window_start) AS hour_start,
                    campaign_id,
                    advertiser_id,
                    sum(impressions) AS impressions,
                    sum(clicks) AS clicks,
                    sum(conversions) AS conversions,
                    sum(cost) AS cost,
                    sum(revenue) AS revenue
                FROM {minutely_table}
                WHERE metric_date = toDate('{metric_date}')
                GROUP BY
                    hour_start,
                    campaign_id,
                    advertiser_id
            )
            """
        )

    @task
    def build_daily_campaign_metrics() -> None:
        metric_date = target_metric_date()
        execute_clickhouse(
            f"""
            ALTER TABLE gold_campaign_metrics_daily
            DELETE WHERE metric_date = toDate('{metric_date}')
            SETTINGS mutations_sync = 1
            """
        )
        execute_clickhouse(
            f"""
            INSERT INTO gold_campaign_metrics_daily (
                metric_date,
                campaign_id,
                advertiser_id,
                impressions,
                clicks,
                conversions,
                ctr,
                cvr,
                cost,
                revenue,
                roas,
                updated_at
            )
            SELECT
                toDate('{metric_date}') AS metric_date,
                campaign_id,
                advertiser_id,
                impressions,
                clicks,
                conversions,
                if(impressions = 0, 0, clicks / impressions) AS ctr,
                if(clicks = 0, 0, conversions / clicks) AS cvr,
                cost,
                revenue,
                if(cost = 0, 0, revenue / cost) AS roas,
                now() AS updated_at
            FROM (
                SELECT
                    campaign_id,
                    advertiser_id,
                    sum(impressions) AS impressions,
                    sum(clicks) AS clicks,
                    sum(conversions) AS conversions,
                    sum(cost) AS cost,
                    sum(revenue) AS revenue
                FROM gold_campaign_metrics_hourly FINAL
                WHERE metric_date = toDate('{metric_date}')
                GROUP BY
                    campaign_id,
                    advertiser_id
            )
            """
        )

    @task
    def build_advertiser_mart_daily() -> None:
        metric_date = target_metric_date()
        execute_clickhouse(
            f"""
            ALTER TABLE gold_advertiser_mart_daily
            DELETE WHERE metric_date = toDate('{metric_date}')
            SETTINGS mutations_sync = 1
            """
        )
        execute_clickhouse(
            f"""
            INSERT INTO gold_advertiser_mart_daily (
                metric_date,
                advertiser_id,
                campaign_count,
                impressions,
                clicks,
                conversions,
                ctr,
                cvr,
                cost,
                revenue,
                roas,
                updated_at
            )
            SELECT
                toDate('{metric_date}') AS metric_date,
                advertiser_id,
                campaign_count,
                impressions,
                clicks,
                conversions,
                if(impressions = 0, 0, clicks / impressions) AS ctr,
                if(clicks = 0, 0, conversions / clicks) AS cvr,
                cost,
                revenue,
                if(cost = 0, 0, revenue / cost) AS roas,
                now() AS updated_at
            FROM (
                SELECT
                    advertiser_id,
                    uniqExact(campaign_id) AS campaign_count,
                    sum(impressions) AS impressions,
                    sum(clicks) AS clicks,
                    sum(conversions) AS conversions,
                    sum(cost) AS cost,
                    sum(revenue) AS revenue
                FROM gold_campaign_metrics_daily FINAL
                WHERE metric_date = toDate('{metric_date}')
                GROUP BY advertiser_id
            )
            """
        )

    # Extension point: late events in PostgreSQL silver_late_events can be replayed into
    # minutely metrics before these ClickHouse rollups are rebuilt.
    hourly = build_hourly_campaign_metrics()
    daily = build_daily_campaign_metrics()
    advertiser = build_advertiser_mart_daily()

    hourly >> daily >> advertiser


daily_ad_report_dag()
