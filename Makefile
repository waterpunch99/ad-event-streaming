SHELL := /usr/bin/env bash

EVENT_COUNT ?= 1000
EVENT_SEED ?= 42
WAIT_SECONDS ?= 30
METRIC_DATE ?= $(shell date -u +%F)
CLEAN_JOB_ARGS ?=
METRICS_JOB_ARGS ?=
POSTGRES_DSN ?= postgresql://postgres:postgres@postgres:5432/ad_pipeline

COMPOSE := docker compose
PY_RUN := $(COMPOSE) run --rm python-runner
MVN_RUN := $(COMPOSE) run --rm maven-runner
FLINK_JAR := /opt/flink/usrlib/ad-pipeline/target/ad-realtime-data-pipeline-flink-jobs-0.1.0.jar

.PHONY: help init up down ps logs build-python build-flink test-python test-java test \
	generate collect cancel-clean cancel-metrics cancel-flink submit-clean submit-metrics \
	flink-list report status e2e clean-volumes

help:
	@printf 'Usage: make <target>\n\n'
	@printf 'Core targets:\n'
	@printf '  init             Create .env from .env.example when missing\n'
	@printf '  up               Start Docker Compose infrastructure\n'
	@printf '  down             Stop Docker Compose infrastructure\n'
	@printf '  ps               Show service status\n'
	@printf '  build-python     Build Python runner image\n'
	@printf '  build-flink      Build Flink Maven jar in Docker\n'
	@printf '  generate         Generate JSONL events with Docker Python\n'
	@printf '  collect          Publish landing JSONL files to Kafka with Docker Python\n'
	@printf '  cancel-flink     Cancel running Flink pipeline jobs by name\n'
	@printf '  submit-clean     Submit CleanAdEventsJob\n'
	@printf '  submit-metrics   Submit CampaignMetricsJob\n'
	@printf '  report           Trigger Airflow report DAG for METRIC_DATE\n'
	@printf '  status           Show pipeline status and row counts\n'
	@printf '  e2e              Run the existing end-to-end script\n'
	@printf '  test             Run Python and Java tests in Docker\n\n'
	@printf 'Variables:\n'
	@printf '  EVENT_COUNT=%s EVENT_SEED=%s WAIT_SECONDS=%s METRIC_DATE=%s\n' \
		"$(EVENT_COUNT)" "$(EVENT_SEED)" "$(WAIT_SECONDS)" "$(METRIC_DATE)"

init:
	@test -f .env || cp .env.example .env

up: init
	$(COMPOSE) up -d

down:
	$(COMPOSE) down

clean-volumes:
	$(COMPOSE) down -v

ps:
	$(COMPOSE) ps

logs:
	$(COMPOSE) logs --tail=200

build-python:
	$(COMPOSE) build python-runner

build-flink:
	$(MVN_RUN) mvn -q package

test-python: build-python
	$(PY_RUN) python -m pytest tests

test-java:
	$(MVN_RUN) mvn -q test

test: test-python test-java

generate: build-python
	$(PY_RUN) python -m src.generator.event_generator --count "$(EVENT_COUNT)" --seed "$(EVENT_SEED)"

collect: build-python
	$(PY_RUN) python -m src.collector.log_collector --bootstrap-servers kafka:29092 --enable-bronze --postgres-dsn "$(POSTGRES_DSN)"

cancel-clean:
	./scripts/flink_cancel_jobs.sh "Clean Ad Events Job"

cancel-metrics:
	./scripts/flink_cancel_jobs.sh "Campaign Metrics Job"

cancel-flink:
	./scripts/flink_cancel_jobs.sh

submit-clean: build-flink
	./scripts/flink_cancel_jobs.sh "Clean Ad Events Job"
	$(COMPOSE) exec -T flink-jobmanager flink run -d -c com.example.adpipeline.jobs.CleanAdEventsJob "$(FLINK_JAR)" $(CLEAN_JOB_ARGS)

submit-metrics: build-flink
	./scripts/flink_cancel_jobs.sh "Campaign Metrics Job"
	$(COMPOSE) exec -T flink-jobmanager flink run -d -c com.example.adpipeline.jobs.CampaignMetricsJob "$(FLINK_JAR)" $(METRICS_JOB_ARGS)

flink-list:
	$(COMPOSE) exec -T flink-jobmanager flink list

report:
	$(COMPOSE) exec -T airflow-scheduler airflow dags unpause daily_ad_report_dag
	$(COMPOSE) exec -T airflow-scheduler airflow dags trigger daily_ad_report_dag \
		-c '{"metric_date":"$(METRIC_DATE)"}' \
		-r "manual_metric_date_$(subst -,_,$(METRIC_DATE))_$$(date -u +%Y%m%d%H%M%S)"

status:
	./scripts/e2e_status.sh

e2e:
	EVENT_COUNT="$(EVENT_COUNT)" EVENT_SEED="$(EVENT_SEED)" WAIT_SECONDS="$(WAIT_SECONDS)" ./scripts/e2e_run.sh
