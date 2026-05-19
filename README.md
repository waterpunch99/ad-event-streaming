# ad-realtime-data-pipeline

광고 노출, 클릭, 전환 로그를 JSONL 애플리케이션 로그로 생성하고, collector가 Kafka에 적재한 뒤 Java Flink로 실시간 정제/집계하고 PostgreSQL과 ClickHouse에 저장하는 로컬 데이터 파이프라인 포트폴리오입니다.

핵심은 단순한 샘플 ETL이 아니라 실무형 흐름을 작게 재현하는 것입니다. 이벤트 생성기는 Kafka를 직접 알지 못하고 파일 로그만 만들며, collector가 Kafka 적재와 PostgreSQL Bronze 추적을 담당합니다. Flink는 event-time 기준 검증, 중복 제거, late event 분리, 1분 캠페인 지표 집계를 수행합니다. Airflow는 ClickHouse Gold Mart를 시간/일 단위로 재생성합니다.

## 결과 요약

이 프로젝트는 로컬 Docker Compose에서 아래 산출물을 생성합니다.

| Layer | Result |
| --- | --- |
| Landing | `data/landing/ad_events_*.jsonl` 광고 이벤트 로그 |
| Kafka Raw | `ad-events-raw` 토픽, key는 `user_id` |
| PostgreSQL Bronze | `bronze_ad_events` 원본 payload, source file/line, collector run id, Kafka offset |
| PostgreSQL Silver | clean, invalid, duplicate, late 이벤트 분리 저장 |
| Kafka Clean | `ad-events-clean` deduplicated valid event |
| ClickHouse Realtime Gold | `gold_campaign_metrics_minutely` 캠페인 1분 지표 |
| ClickHouse Batch Gold | hourly, daily, advertiser daily mart |
| Airflow | `daily_ad_report_dag` 재실행 가능한 리포트 DAG |

계산되는 실시간 지표:

- impressions, clicks, conversions
- CTR, CVR, ROAS
- cost, revenue
- division by zero 안전 처리

데이터 품질/운영 추적 결과:

- invalid event는 `silver_invalid_events`
- duplicate `event_id` 또는 `conversion_id`는 `silver_duplicate_events`
- watermark보다 늦은 late event는 `silver_late_events`
- collector가 Kafka에 성공적으로 publish한 원본은 `bronze_ad_events`

## 빠른 실행

필수 도구:

- Docker Compose
- `make`

전체 파이프라인 실행:

```bash
make e2e
```

옵션 예시:

```bash
EVENT_COUNT=500 EVENT_SEED=42 WAIT_SECONDS=30 make e2e
```

현재 상태 확인:

```bash
make status
```

테스트:

```bash
make test
```

주요 UI:

| Service | URL |
| --- | --- |
| Kafka UI | `http://localhost:8080` |
| Flink UI | `http://localhost:8081` |
| Airflow UI | `http://localhost:8082` |

Airflow login: `admin / admin`

## 실행 결과 확인

PostgreSQL Bronze/Silver row count:

```bash
docker compose exec postgres psql -U postgres -d ad_pipeline -c "
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
```

ClickHouse 실시간 캠페인 지표:

```bash
docker compose exec clickhouse clickhouse-client --database ad_pipeline --query "
SELECT
  campaign_id,
  advertiser_id,
  impressions,
  clicks,
  conversions,
  ctr,
  cvr,
  cost,
  revenue,
  roas
FROM gold_campaign_metrics_minutely
ORDER BY window_start DESC
LIMIT 10;
"
```

ClickHouse 배치 Mart:

```bash
docker compose exec clickhouse clickhouse-client --database ad_pipeline --query "
SELECT 'gold_campaign_metrics_hourly' AS table_name, count() FROM gold_campaign_metrics_hourly
UNION ALL
SELECT 'gold_campaign_metrics_daily', count() FROM gold_campaign_metrics_daily
UNION ALL
SELECT 'gold_advertiser_mart_daily', count() FROM gold_advertiser_mart_daily;
"
```

최근 로컬 E2E에서 확인한 결과 예시:

```text
Kafka topics:
  ad-events-raw
  ad-events-clean
  ad-events-invalid
  ad-events-deduplicated
  ad-metrics-realtime

Flink jobs:
  Clean Ad Events Job RUNNING
  Campaign Metrics Job RUNNING

PostgreSQL Silver:
  silver_ad_events        1384
  silver_invalid_events      3
  silver_duplicate_events    4
  silver_late_events        11

ClickHouse Gold:
  gold_campaign_metrics_minutely  317
  gold_campaign_metrics_hourly     40
  gold_campaign_metrics_daily      20
  gold_advertiser_mart_daily        5
```

## 아키텍처

```text
src/generator/event_generator.py
-> data/landing/*.jsonl
-> src/collector/log_collector.py
-> Kafka ad-events-raw
-> Java Flink CleanAdEventsJob
-> PostgreSQL Bronze/Silver + Kafka ad-events-clean
-> Java Flink CampaignMetricsJob
-> ClickHouse gold_campaign_metrics_minutely + Kafka ad-metrics-realtime
-> Airflow daily_ad_report_dag
-> ClickHouse hourly/daily/advertiser marts
```

## 컴포넌트 역할

| Component | Responsibility |
| --- | --- |
| `event_generator` | 광고 서비스 로그를 흉내 내는 JSONL 파일 생성 |
| `log_collector` | JSONL을 읽어 Kafka `ad-events-raw` publish, PostgreSQL Bronze 기록, archive/bad 파일 관리 |
| `CleanAdEventsJob` | raw event 검증, invalid/duplicate/late 분리, clean event 발행 |
| `CampaignMetricsJob` | clean event를 1분 window로 집계해 ClickHouse Gold 저장 |
| `daily_ad_report_dag` | ClickHouse minutely metric을 hourly/daily/advertiser mart로 재생성 |

## 데이터 저장소

PostgreSQL은 운영 추적용 Bronze/Silver 저장소입니다.

| Table | Purpose |
| --- | --- |
| `bronze_ad_events` | Kafka publish 성공 원본 payload와 source/Kafka metadata |
| `silver_ad_events` | valid, non-late, deduplicated event |
| `silver_invalid_events` | 필수 필드, event type, 금액, conversion 필드 검증 실패 |
| `silver_duplicate_events` | duplicate `event_id`, duplicate `conversion_id` |
| `silver_late_events` | 10분 watermark보다 늦게 도착한 이벤트 |

ClickHouse는 분석/서빙용 Gold 저장소입니다.

| Table | Purpose |
| --- | --- |
| `gold_campaign_metrics_minutely` | 실시간 1분 캠페인 지표 |
| `gold_campaign_metrics_hourly` | 시간 단위 캠페인 리포트 |
| `gold_campaign_metrics_daily` | 일 단위 캠페인 리포트 |
| `gold_advertiser_mart_daily` | 일 단위 광고주 Mart |

## Kafka Topics

| Topic | Producer | Consumer | Purpose |
| --- | --- | --- | --- |
| `ad-events-raw` | `log_collector` | `CleanAdEventsJob` | Raw application log events |
| `ad-events-clean` | `CleanAdEventsJob` | `CampaignMetricsJob` | Valid, non-late, deduplicated events |
| `ad-metrics-realtime` | `CampaignMetricsJob` | Optional clients | Real-time campaign metrics |
| `ad-events-invalid` | Reserved | Reserved | Optional invalid event topic |
| `ad-events-deduplicated` | Reserved | Reserved | Optional dedup topic |

Kafka event topic partition key는 `user_id`입니다.

## 핵심 정책

중복 이벤트:

- `event_id` 기준으로 중복 제거
- conversion 이벤트는 `conversion_id` 기준으로도 중복 방지
- 중복 이벤트는 버리지 않고 `silver_duplicate_events`에 저장
- clean topic에는 deduplicated 이벤트만 발행

지연 이벤트:

- 처리 시간 기준이 아니라 `event_time` 기준 처리
- Flink watermark는 10분 bounded out-of-orderness
- watermark보다 늦은 이벤트는 `silver_late_events`에 저장
- late event는 실시간 지표에 반영하지 않음
- 배치 리포트에서 재계산할 수 있도록 확장 지점 유지

지표 계산:

- `ctr = clicks / impressions`, impressions가 0이면 0
- `cvr = conversions / clicks`, clicks가 0이면 0
- `roas = revenue / cost`, cost가 0이면 0
- ClickHouse minutely table은 `ReplacingMergeTree(updated_at)` 기반으로 window key replay에 대응

Flink 운영:

- Checkpointing: exactly once
- Checkpoint interval: 60 seconds
- Checkpoint storage: `file:///opt/flink/checkpoints`
- Restart strategy: fixed delay, 3 attempts, 10 seconds delay
- Default parallelism: 1
- Dedup state TTL: `event_id` 24 hours, `conversion_id` 168 hours

## 수동 실행

인프라 시작:

```bash
make up
```

빌드:

```bash
make build-python
make build-flink
```

Flink job 제출:

```bash
make submit-clean
make submit-metrics
```

기존 Flink job 중복 실행 방지:

```bash
make cancel-flink
```

이벤트 생성:

```bash
EVENT_COUNT=1000 EVENT_SEED=42 make generate
```

Kafka 적재 및 PostgreSQL Bronze 기록:

```bash
make collect
```

호스트에서 PostgreSQL DSN을 직접 지정해야 할 때:

```bash
POSTGRES_DSN=postgresql://postgres:postgres@localhost:5433/ad_pipeline make collect
```

Airflow 리포트 DAG 실행:

```bash
METRIC_DATE=2026-05-19 make report
```

Flink runtime override:

```bash
CLEAN_JOB_ARGS="--event-id-dedup-ttl-hours 48 --conversion-id-dedup-ttl-hours 336" make submit-clean
METRICS_JOB_ARGS="--parallelism 2 --checkpoint-interval-ms 30000" make submit-metrics
```

## 테스트와 품질 검증

Python tests:

```bash
make test-python
```

Java tests:

```bash
make test-java
```

SQL quality checks:

```bash
docker compose exec -T postgres psql -U postgres -d ad_pipeline \
  < sql/quality/postgres_event_quality.sql

docker compose exec -T clickhouse clickhouse-client --database ad_pipeline --multiquery \
  < sql/quality/clickhouse_metric_quality.sql
```

## 프로젝트 구조

```text
configs/                 Local service config notes
data/landing/            JSONL files generated by event_generator
data/archive/            JSONL files processed by log_collector
data/bad/                Bad JSONL records from log_collector
dags/                    Airflow DAGs
docker/                  Dockerfiles for local tool runners
flink-jobs/              Java 17 Maven Flink jobs
Makefile                 Standard Docker-backed local commands
scripts/                 Local E2E scripts
sql/postgres/            PostgreSQL init SQL
sql/clickhouse/          ClickHouse init SQL
sql/quality/             Data quality SQL checks
src/common/              Python schema, validation, metric helpers
src/generator/           JSONL event generator
src/collector/           JSONL to Kafka/PostgreSQL Bronze collector
src/storage/             PostgreSQL/ClickHouse helper modules
tests/                   Python tests
```

## 기술적 의사결정

1. 이벤트 생성기와 Kafka producer를 분리했습니다.

   `event_generator`는 JSONL 로그만 생성하고, Kafka 적재는 `log_collector`가 담당합니다. 실무의 application log -> collector -> Kafka 구조를 모방했습니다.

2. PostgreSQL과 ClickHouse의 역할을 분리했습니다.

   PostgreSQL은 원본, 정제, 오류, 중복, 지연 이벤트 추적에 사용합니다. ClickHouse는 대량 분석과 리포트 조회에 사용합니다.

3. Java Flink를 사용했습니다.

   Kafka consume, event-time watermark, validation, deduplication, late event routing, window aggregation, JDBC sink를 타입 안정성 있는 Java 코드로 구현했습니다.

4. `event_time`과 `ingestion_time`을 분리했습니다.

   지연 이벤트 처리를 설명할 수 있고, 실시간 지표와 배치 리포트의 차이를 명확히 보여줍니다.

5. 중복 이벤트와 지연 이벤트를 버리지 않습니다.

   invalid, duplicate, late 데이터를 별도 Silver 테이블에 저장해 운영 추적성과 데이터 품질 분석 가능성을 높였습니다.

6. 실시간 지표와 배치 리포트를 분리했습니다.

   Flink는 빠른 실시간 지표를 만들고, Airflow batch는 재실행 가능한 보정 리포트를 만듭니다.

7. 로컬 실행 경로를 Docker/Make로 표준화했습니다.

   Python, Java, Maven 설치 상태에 덜 의존하고 `make e2e`, `make test`로 결과를 재현할 수 있게 했습니다.

## Troubleshooting

Docker CLI가 없을 때:

```text
The command 'docker' could not be found
```

Docker Desktop WSL integration을 켜거나 Docker Compose가 가능한 환경에서 실행합니다.

테이블 스키마 변경이 반영되지 않을 때:

```bash
docker compose down -v
docker compose up -d
```

Docker init SQL은 volume 최초 생성 시점에만 실행됩니다. 단, `log_collector`의 Bronze metadata 컬럼은 실행 시 `ALTER TABLE ... ADD COLUMN IF NOT EXISTS`로 보강합니다.

Flink job이 중복으로 떠 있을 때:

```bash
make cancel-flink
make submit-clean
make submit-metrics
```

Airflow DAG가 보이지 않을 때:

```bash
docker compose exec airflow-scheduler airflow dags list | grep daily_ad_report_dag
docker compose logs airflow-scheduler
```

Kafka raw event가 없을 때:

```bash
ls data/landing
EVENT_COUNT=100 make generate
make collect
```

## Development Plan

단계별 구현 이력과 진행 규칙은 [DEVELOPMENT_PLAN.md](DEVELOPMENT_PLAN.md)에 정리되어 있습니다.
