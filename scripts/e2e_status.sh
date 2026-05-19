#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_ROOT"

log() {
  printf '\n[%s] %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$*"
}

compose_exec() {
  docker compose exec -T "$@"
}

log "Docker Compose services"
docker compose ps

log "Kafka topics"
compose_exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:29092 \
  --list

log "Flink jobs"
compose_exec flink-jobmanager flink list

log "PostgreSQL Silver row counts"
compose_exec postgres psql -U postgres -d ad_pipeline -c "
SELECT 'silver_ad_events' AS table_name, COUNT(*) FROM silver_ad_events
UNION ALL
SELECT 'silver_invalid_events', COUNT(*) FROM silver_invalid_events
UNION ALL
SELECT 'silver_duplicate_events', COUNT(*) FROM silver_duplicate_events
UNION ALL
SELECT 'silver_late_events', COUNT(*) FROM silver_late_events;
"

log "ClickHouse Gold row counts"
compose_exec clickhouse clickhouse-client --database ad_pipeline --query "
SELECT 'gold_campaign_metrics_minutely' AS table_name, count() FROM gold_campaign_metrics_minutely
UNION ALL
SELECT 'gold_campaign_metrics_hourly', count() FROM gold_campaign_metrics_hourly
UNION ALL
SELECT 'gold_campaign_metrics_daily', count() FROM gold_campaign_metrics_daily
UNION ALL
SELECT 'gold_advertiser_mart_daily', count() FROM gold_advertiser_mart_daily;
"

log "Airflow DAG state"
compose_exec airflow-scheduler airflow dags list | grep daily_ad_report_dag || true
