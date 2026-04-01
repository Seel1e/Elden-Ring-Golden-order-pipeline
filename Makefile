# Golden Order Data Pipeline — Makefile
# ════════════════════════════════════════════════════════════════════════════
# Usage:
#   make install      Install Python dependencies
#   make test         Run all tests
#   make diagram      Generate architecture diagram PNG
#   make pipeline     Run the full batch pipeline (Phases 1 + 2)
#   make produce      Start the Kafka telemetry producer
#   make consume      Start the Spark streaming consumer
#   make infra-up     Start all Docker services (Kafka, Postgres, Airflow, Spark)
#   make infra-down   Stop all Docker services
#   make demo         Run a quick demo (no Docker required)
# ════════════════════════════════════════════════════════════════════════════

PYTHON     := python
PIP        := pip
PYTEST     := pytest
DOCKER     := docker compose

.PHONY: install test diagram pipeline produce consume infra-up infra-down demo clean \
        calculator advisor streamlit api meta-report

# ── Setup ─────────────────────────────────────────────────────────────────────

install:
	$(PIP) install -r requirements.txt
	cp -n .env.example .env || true

# ── Tests ─────────────────────────────────────────────────────────────────────

test:
	$(PYTEST) tests/ -v --tb=short --cov=src --cov-report=term-missing

test-scaling:
	$(PYTEST) tests/test_scaling_engine.py -v --tb=short

test-telemetry:
	$(PYTEST) tests/test_telemetry_generator.py -v --tb=short

# ── Architecture diagram ──────────────────────────────────────────────────────

diagram:
	$(PYTHON) -m architecture.diagram

# ── Batch pipeline (Phases 1 + 2, no Docker required) ────────────────────────

# Phase 1: Extract from API + load Kaggle data
ingest:
	$(PYTHON) -m src.ingestion.api_extractor
	$(PYTHON) -m src.ingestion.kaggle_loader

# Phase 2: Flatten JSON, apply Scaling Engine, load warehouse
transform:
	$(PYTHON) -m src.transformation.dim_builder

pipeline: ingest transform
	@echo "Batch pipeline complete."

# ── Streaming (requires Docker infra) ────────────────────────────────────────

produce:
	$(PYTHON) -m src.streaming.kafka_producer

consume:
	$(PYTHON) -m src.streaming.spark_consumer

# ── Analytics ─────────────────────────────────────────────────────────────────

analytics:
	$(PYTHON) -m src.analytics.queries

meta-report:
	$(PYTHON) -m src.analytics.meta_report

# ── Interactive tools ─────────────────────────────────────────────────────────

calculator:
	$(PYTHON) -m src.calculator

advisor:
	$(PYTHON) -m src.advisor

streamlit:
	streamlit run src/app.py

api:
	uvicorn src.api:app --reload --port 8000

# ── Docker infrastructure ─────────────────────────────────────────────────────

infra-up:
	$(DOCKER) up -d
	@echo "Waiting 30s for services to initialise…"
	sleep 30
	@echo "Services ready."

infra-down:
	$(DOCKER) down

infra-logs:
	$(DOCKER) logs -f

# ── Demo (no Docker — uses local SQLite-like Parquet + console output) ────────

demo:
	@echo "=== Running Scaling Engine demo ==="
	$(PYTHON) -m src.transformation.scaling_engine
	@echo ""
	@echo "=== Generating 10 sample telemetry events ==="
	$(PYTHON) -m src.streaming.telemetry_generator
	@echo ""
	@echo "=== Generating architecture diagram ==="
	$(PYTHON) -m architecture.diagram

# ── Clean ─────────────────────────────────────────────────────────────────────

clean:
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -name "*.pyc" -delete 2>/dev/null || true
	rm -rf data/raw/* data/staged/* data/checkpoints/*
	@echo "Cleaned cache and data directories."
