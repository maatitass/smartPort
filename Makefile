.PHONY: help probe up down ps logs test lint reset-es replay-dq replay-bench export-kibana import-kibana

help:
	@echo "Smart Port targets:"
	@echo "  make probe          - Run Phase 0 source validation probe (3 sources)"
	@echo "  make up             - Start local Docker Compose stack"
	@echo "  make down           - Stop Docker Compose stack (keeps volumes)"
	@echo "  make ps             - Show service status"
	@echo "  make logs           - Tail service logs"
	@echo "  make test           - Run pytest suite via uv"
	@echo "  make lint           - Run ruff linter via uv"
	@echo "  make reset-es       - Manually delete ais-* ES indices and re-init mappings"
	@echo "  make replay-dq      - Inject DQ edge-case fixtures into Kafka"
	@echo "  make replay-bench   - Run 1 vs 2 Worker benchmark replay on a clean stack"
	@echo "  make export-kibana  - Export Kibana Data Views and dashboard"
	@echo "  make import-kibana  - Import Kibana Data Views and dashboard"

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

reset-es:
	./infra/init-es.sh --reset

replay-dq:
	uv run scripts/replay.py --mode fixture

replay-bench:
	uv run scripts/replay.py --mode benchmark

export-kibana:
	./kibana/export.sh

import-kibana:
	./kibana/import.sh
