#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
EVENT_COUNT="${EVENT_COUNT:-1000}"
EVENT_SEED="${EVENT_SEED:-42}"
WAIT_SECONDS="${WAIT_SECONDS:-20}"
METRIC_DATE="${METRIC_DATE:-$(date -u '+%Y-%m-%d')}"
AUTO_CANCEL_FLINK_JOBS="${AUTO_CANCEL_FLINK_JOBS:-1}"
CLEAN_JOB_ARGS="${CLEAN_JOB_ARGS:-}"
METRICS_JOB_ARGS="${METRICS_JOB_ARGS:-}"
POSTGRES_DSN="${POSTGRES_DSN:-postgresql://postgres:postgres@postgres:5432/ad_pipeline}"
FLINK_JAR="$PROJECT_ROOT/flink-jobs/target/ad-realtime-data-pipeline-flink-jobs-0.1.0.jar"
FLINK_CONTAINER_JAR="/opt/flink/usrlib/ad-pipeline/target/ad-realtime-data-pipeline-flink-jobs-0.1.0.jar"

cd "$PROJECT_ROOT"

log() {
  printf '\n[%s] %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$*"
}

require_command() {
  if ! command -v "$1" >/dev/null 2>&1; then
    printf 'Required command not found: %s\n' "$1" >&2
    exit 1
  fi
}

compose_exec() {
  docker compose exec -T "$@"
}

compose_run() {
  docker compose run --rm "$@"
}

clickhouse_final_suffix() {
  local table_name="$1"
  local engine
  engine="$(compose_exec clickhouse clickhouse-client --database ad_pipeline --query "
SELECT engine
FROM system.tables
WHERE database = currentDatabase()
  AND name = '$table_name'
FORMAT TabSeparated
" 2>/dev/null || true)"

  case "$engine" in
    ReplacingMergeTree|SummingMergeTree|AggregatingMergeTree|CollapsingMergeTree)
      printf ' FINAL'
      ;;
    *)
      printf ''
      ;;
  esac
}

wait_for_compose_service() {
  local service="$1"
  local retries="${2:-60}"

  for _ in $(seq 1 "$retries"); do
    case "$service" in
      kafka)
        compose_exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:29092 --list >/dev/null 2>&1 && return 0
        ;;
      postgres)
        compose_exec postgres pg_isready -U postgres -d ad_pipeline >/dev/null 2>&1 && return 0
        ;;
      clickhouse)
        compose_exec clickhouse clickhouse-client --database ad_pipeline --query "SELECT 1" >/dev/null 2>&1 && return 0
        ;;
      *)
        docker compose ps "$service" >/dev/null 2>&1 && return 0
        ;;
    esac
    sleep 2
  done

  printf 'Timed out waiting for service: %s\n' "$service" >&2
  docker compose ps
  exit 1
}

require_command docker

if [[ ! -f .env ]]; then
  log "Creating .env from .env.example"
  cp .env.example .env
fi

log "Starting Docker Compose infrastructure"
docker compose up -d

log "Waiting for infrastructure health checks"
wait_for_compose_service kafka
wait_for_compose_service postgres
wait_for_compose_service clickhouse
sleep 5

log "Checking Kafka topics"
compose_exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:29092 \
  --list

log "Checking PostgreSQL tables"
compose_exec postgres psql -U postgres -d ad_pipeline -c "\\dt"

log "Checking ClickHouse tables"
compose_exec clickhouse clickhouse-client --database ad_pipeline --query "SHOW TABLES"

log "Building Python runner image"
docker compose build python-runner

log "Building Flink jobs with Docker Maven"
compose_run maven-runner mvn package

if [[ ! -f "$FLINK_JAR" ]]; then
  printf 'Flink job jar was not created: %s\n' "$FLINK_JAR" >&2
  exit 1
fi

if [[ "$AUTO_CANCEL_FLINK_JOBS" == "1" ]]; then
  log "Cancelling existing Flink pipeline jobs before submission"
  ./scripts/flink_cancel_jobs.sh
fi

log "Submitting CleanAdEventsJob"
compose_exec flink-jobmanager flink run -d \
  -c com.example.adpipeline.jobs.CleanAdEventsJob \
  "$FLINK_CONTAINER_JAR" \
  $CLEAN_JOB_ARGS

log "Submitting CampaignMetricsJob"
compose_exec flink-jobmanager flink run -d \
  -c com.example.adpipeline.jobs.CampaignMetricsJob \
  "$FLINK_CONTAINER_JAR" \
  $METRICS_JOB_ARGS

log "Generating JSONL ad events"
compose_run python-runner python -m src.generator.event_generator \
  --count "$EVENT_COUNT" \
  --seed "$EVENT_SEED"

log "Collecting JSONL logs into Kafka ad-events-raw"
compose_run python-runner python -m src.collector.log_collector \
  --bootstrap-servers kafka:29092 \
  --enable-bronze \
  --postgres-dsn "$POSTGRES_DSN"

log "Waiting for streaming jobs to process records"
sleep "$WAIT_SECONDS"

log "Checking PostgreSQL Bronze/Silver row counts"
compose_exec postgres psql -U postgres -d ad_pipeline -c "
SELECT 'bronze_ad_events' AS table_name, COUNT(*) FROM bronze_ad_events
UNION ALL
SELECT 'silver_ad_events', COUNT(*) FROM silver_ad_events
UNION ALL
SELECT 'silver_invalid_events', COUNT(*) FROM silver_invalid_events
UNION ALL
SELECT 'silver_duplicate_events', COUNT(*) FROM silver_duplicate_events
UNION ALL
SELECT 'silver_late_events', COUNT(*) FROM silver_late_events;
"

log "Checking ClickHouse minutely campaign metrics"
MINUTELY_FINAL_SUFFIX="$(clickhouse_final_suffix gold_campaign_metrics_minutely)"
compose_exec clickhouse clickhouse-client --database ad_pipeline --query "
SELECT
  campaign_id,
  advertiser_id,
  sum(impressions) AS impressions,
  sum(clicks) AS clicks,
  sum(conversions) AS conversions,
  round(avg(ctr), 4) AS avg_ctr,
  round(avg(cvr), 4) AS avg_cvr,
  round(avg(roas), 4) AS avg_roas
FROM gold_campaign_metrics_minutely${MINUTELY_FINAL_SUFFIX}
GROUP BY campaign_id, advertiser_id
ORDER BY impressions DESC
LIMIT 10;
"

log "Triggering Airflow daily_ad_report_dag for metric_date=${METRIC_DATE}"
compose_exec airflow-scheduler airflow dags unpause daily_ad_report_dag
compose_exec airflow-scheduler airflow dags trigger daily_ad_report_dag \
  -c "{\"metric_date\":\"${METRIC_DATE}\"}" \
  -r "manual_metric_date_${METRIC_DATE//-/_}_$(date -u '+%Y%m%d%H%M%S')"

log "Waiting for Airflow batch DAG"
sleep "$WAIT_SECONDS"

log "Checking final ClickHouse marts"
compose_exec clickhouse clickhouse-client --database ad_pipeline --query "
SELECT 'gold_campaign_metrics_hourly' AS table_name, count() FROM gold_campaign_metrics_hourly
UNION ALL
SELECT 'gold_campaign_metrics_daily', count() FROM gold_campaign_metrics_daily
UNION ALL
SELECT 'gold_advertiser_mart_daily', count() FROM gold_advertiser_mart_daily;
"

log "End-to-end run completed"
printf '\nUseful UIs:\n'
printf '  Kafka UI:   http://localhost:8080\n'
printf '  Flink UI:   http://localhost:8081\n'
printf '  Airflow UI: http://localhost:8082\n'
