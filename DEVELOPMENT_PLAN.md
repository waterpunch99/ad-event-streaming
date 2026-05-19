# ad-realtime-data-pipeline Development Plan

## Project Goal

Build a local Docker Compose based advertising data pipeline portfolio project.

The final architecture will collect ad impression, click, and conversion logs, process them with Java Flink, store Bronze/Silver operational data in PostgreSQL, store Gold analytical marts in ClickHouse, and generate hourly/daily advertiser reports with Airflow.

```text
event_generator
-> JSONL log file
-> log_collector
-> Kafka
-> Java Flink streaming jobs
-> PostgreSQL / ClickHouse
-> Airflow batch jobs
-> ClickHouse Mart
```

The event generator must not send messages directly to Kafka. It only writes JSONL files that mimic application logs.

```text
src/generator/event_generator.py
-> data/landing/*.jsonl
-> src/collector/log_collector.py
-> Kafka ad-events-raw topic
-> Java Flink jobs
-> PostgreSQL / ClickHouse
```

## Development Rules

1. Do not implement the whole project at once.
2. Implement only the requested step.
3. Do not create code for a future step before the user requests it.
4. Preserve previous step outputs as much as possible.
5. Make minimal changes when a later step requires adjustments.
6. Prefer runnable, consistent structure over excessive implementation.
7. All runnable components must target local Docker Compose execution.
8. Java Flink code must be Maven based.
9. Python code is limited to event generation, log collection, Airflow DAGs, quality helpers, and tests.
10. README is finalized in the last step. Earlier steps may provide only minimal execution notes in the response.

Each completed step must summarize:

- Created files
- Modified files
- How to run
- How to verify
- What to do in the next step

## Kafka Topics

- `ad-events-raw`
- `ad-events-invalid`
- `ad-events-deduplicated`
- `ad-events-clean`
- `ad-metrics-realtime`

Kafka message partition key: `user_id`.

## Event Types

- `impression`
- `click`
- `conversion`

The event-time field is `event_time`. Real-time processing uses a 10 minute event-time watermark.

## Storage Roles

PostgreSQL is the Bronze/Silver operational store:

- Raw events
- Cleaned events
- Invalid events
- Duplicate events
- Late events
- Data quality and operational traceability

ClickHouse is the Gold serving store:

- Real-time campaign metrics
- Hourly and daily campaign reports
- Daily advertiser mart
- Fast OLAP analytics

## Step Plan

### STEP 0. Project scaffolding and execution rules

- Create the directory structure.
- Create minimal placeholder files only.
- Document the project goal and step-by-step rules in this file.
- Do not implement Docker, Kafka, Flink, or Airflow logic.
- Create only the minimal Java Flink Maven project directory structure.

### STEP 1. Docker Compose infrastructure

- Add Kafka, Kafka UI or tools, PostgreSQL, ClickHouse, Flink JobManager, Flink TaskManager, Airflow webserver, and Airflow scheduler.
- Add required networks and volumes.
- Document how to check Kafka topics, PostgreSQL, ClickHouse, Flink UI, and Airflow UI.
- Do not implement business logic.

### STEP 2. PostgreSQL and ClickHouse table design

- Add PostgreSQL Bronze/Silver table SQL.
- Add ClickHouse Gold table SQL.
- Add PostgreSQL indexes for `event_id`, `event_time`, `campaign_id`, and `user_id`.
- Use MergeTree family tables in ClickHouse.
- Configure init SQL for Docker Compose.

### STEP 3. Python event schema and validation

- Add Python schema and validation used by the generator and tests.
- Validate event type, required fields, non-negative cost/revenue, and conversion fields.
- Add pytest coverage.
- Do not send data to Kafka.

### STEP 4. Event generator

- Generate JSONL files under `data/landing/`.
- Generate impression, click, and conversion events.
- Include controlled invalid, duplicate, and late events.
- Keep `event_time` and `ingestion_time` separate.

### STEP 5. JSONL log collector

- Read JSONL files from `data/landing/`.
- Send records to Kafka topic `ad-events-raw`.
- Use `user_id` as the Kafka message key.
- Move completed files to `data/archive/`.
- Route bad records or files to `data/bad/` or `ad-events-invalid`.

### STEP 6. Java Flink base project

- Add `flink-jobs/pom.xml`.
- Configure Java 17, Flink connectors, Jackson, and JUnit.
- Add `AdEvent`, `CampaignMetric`, JSON deserialization, and base `EventValidator`.
- Keep real Flink jobs incomplete.
- `mvn test` should pass.

### STEP 7. Java Flink cleaning pipeline

- Implement `CleanAdEventsJob`.
- Consume `ad-events-raw`.
- Validate schema.
- Store valid, invalid, duplicate, and late events in PostgreSQL Silver tables.
- Publish deduplicated clean events to `ad-events-clean`.
- Do not aggregate campaign metrics yet.

### STEP 8. Java Flink real-time campaign metrics

- Implement `CampaignMetricsJob`.
- Consume `ad-events-clean`.
- Use 1 minute tumbling event-time windows.
- Aggregate by `campaign_id` and `advertiser_id`.
- Calculate impressions, clicks, conversions, CTR, CVR, cost, revenue, and ROAS.
- Store minutely metrics in ClickHouse.
- Safely handle division by zero.

### STEP 9. Airflow batch report DAG

- Add `daily_ad_report_dag`.
- Build hourly, daily, and advertiser daily marts in ClickHouse.
- Use rerunnable delete-insert or ReplacingMergeTree strategy.
- Leave an extension point for PostgreSQL late events.

### STEP 10. Data quality validation and tests

- Add pytest and JUnit coverage for validation, collector behavior, deduplication, late events, and metric calculation.
- Add SQL quality queries.
- Verify division by zero handling for CTR, CVR, and ROAS.

### STEP 11. End-to-end execution scripts

- Add local scripts to run the full pipeline in order.
- Include infrastructure startup, topic/table checks, event generation, collection, Flink jobs, ClickHouse checks, Airflow DAG run, and final mart checks.

### STEP 12. README and architecture documentation

- Finalize README.
- Document architecture, execution order, component roles, Kafka topics, PostgreSQL tables, ClickHouse tables, Flink jobs, late/duplicate event policies, tests, troubleshooting, and portfolio decisions.

## Portfolio Decisions To Highlight

1. Event generation and Kafka producing are separated.
2. PostgreSQL and ClickHouse have distinct operational and analytical roles.
3. Java Flink is used for real-time stream processing.
4. `event_time` and `ingestion_time` are separated.
5. Duplicate and late events are stored for traceability instead of being discarded.
6. Real-time metrics and batch reports are separated.
