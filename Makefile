.PHONY: help probe up down ps logs test lint replay-dq replay-bench export-kibana import-kibana

help:
	@echo "Smart Port targets:"
	@echo "  make probe          - Run Module 0 source validation probe"
	@echo "  make up             - Start full Docker Compose stack"
	@echo "  make down           - Stop Docker Compose stack (keeps volumes)"
	@echo "  make ps             - Show service status"
	@echo "  make logs           - Tail service logs"
	@echo "  make test           - Run unit test suite via uv"
	@echo "  make lint           - Run ruff linter via uv"
	@echo "  make replay-dq      - Inject DQ edge-case fixtures into Kafka"
	@echo "  make replay-bench   - Run isolated benchmark replay"
	@echo "  make export-kibana  - Export Kibana Data Views and dashboards"
	@echo "  make import-kibana  - Import Kibana Data Views and dashboards"

probe:
	uv run scripts/probe_sources.py

up:
	docker compose up -d

down:
	docker compose down

ps:
	docker compose ps

logs:
	docker compose logs -f --tail=100

test:
	uv run pytest

lint:
	uv run ruff check .

replay-dq:
	uv run services/replay/replay_producer.py --mode fixture

replay-bench:
	uv run services/replay/replay_producer.py --mode benchmark

export-kibana:
	./kibana/export_dashboards.sh

import-kibana:
	./kibana/import_dashboards.sh
