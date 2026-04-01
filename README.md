# Golden Order — Elden Ring Combat Analytics

An end-to-end data pipeline built around one question: **which weapon builds actually win fights, and which bosses kill the most players?**

Answering that correctly means solving the non-linear soft-cap scaling system that determines weapon damage in Elden Ring. That math is the core of this project. Everything else — Kafka, Spark, Airflow, the REST API, the Streamlit dashboard — exists to run it at scale and surface the results.

---

## What it does

Synthetic player encounter events are generated and published to Kafka at ~500/s. A Spark Structured Streaming consumer picks them up, joins against weapon and boss dimension tables, runs the scaling engine UDF to compute each player's true Attack Rating at their exact stats, then writes the enriched rows to PostgreSQL. Airflow runs the nightly batch to refresh the dimensions from the API and Kaggle dataset. The results are queryable through a FastAPI service and a Streamlit dashboard.

```
[Fan API + Kaggle CSV] ──► dim_weapons / dim_bosses
[Telemetry Generator]  ──► Kafka ──► Spark Streaming ──► fact_encounters
                                                               │
                          FastAPI :8000  ◄───────────────────┤
                          Streamlit :8501 ◄──────────────────┘
```

---

## The Scaling Engine

Weapon damage in Elden Ring is not a flat number. It scales non-linearly with player stats through two soft caps — returns diminish sharply at stat 60, then again at 80.

```
Total AR = base + Σ (base × grade_multiplier × correction(stat))
```

The correction curve maps a stat value (1–99) to a 0–1 multiplier. At stat 60 the slope drops by ~40%, at 80 by another ~50%. Grade multipliers (S = 3.0×, A = 2.5×, B = 1.75×, C = 1.15×, D = 0.75×, E = 0.45×) control the ceiling.

Two-handing adds 50% effective STR — STR 40 two-handed equals STR 60 one-handed in damage output.

This logic runs as a Spark UDF against every streaming micro-batch so `fact_encounters.calculated_ar` is always the player's real damage number, not a pre-computed approximation.

---

## Stack

| Layer | Tech |
|---|---|
| Warehouse | PostgreSQL 15 (star schema) |
| Stream broker | Apache Kafka 7.6 |
| Stream processing | PySpark 3.5 Structured Streaming |
| Batch orchestration | Apache Airflow 2.8 |
| Web dashboard | Streamlit |
| REST API | FastAPI + Pydantic v2 |
| Infrastructure | Docker Compose |
| Tests | pytest (50 tests) |

---

## Quick start

```bash
# 1. Clone and set up
python -m venv .venv && .venv\Scripts\activate   # Windows
pip install -r requirements.txt
cp .env.example .env

# 2. Run the scaling engine demo (no Docker needed)
python -m src.transformation.scaling_engine

# 3. Full stack
docker compose up -d
# Airflow UI  → http://localhost:8081  (admin / admin)
# Spark UI   → http://localhost:8080

# 4. Load the warehouse
python -m src.ingestion.kaggle_loader
python -m src.transformation.dim_builder

# 5. Start streaming
python -m src.streaming.kafka_producer   # terminal 1
python -m src.streaming.spark_consumer  # terminal 2

# 6. Run the tools
streamlit run src/app.py                          # web dashboard :8501
uvicorn src.api:app --reload --port 8000          # REST API :8000/docs
python -m src.calculator                          # CLI calculator
python -m src.advisor                             # CLI build advisor
python -m src.analytics.meta_report              # daily meta snapshot
```

---

## Project structure

```
src/
├── transformation/
│   ├── scaling_engine.py     ← the AR math (soft caps, grades, two-handing)
│   ├── json_flattener.py     ← parse nested dict columns from Kaggle CSVs
│   └── dim_builder.py        ← load dim_weapons + dim_bosses
├── ingestion/
│   ├── api_extractor.py      ← paginated REST extraction with retry
│   └── kaggle_loader.py      ← CSV → Parquet staging
├── streaming/
│   ├── telemetry_generator.py
│   ├── kafka_producer.py
│   └── spark_consumer.py     ← foreachBatch + scaling UDF + upsert
├── analytics/
│   ├── queries.py
│   └── meta_report.py        ← daily snapshot → fact_meta_reports
├── calculator.py             ← interactive CLI: pick boss/weapon/stats
├── advisor.py                ← build advisor: class picker, gauntlet, saves
├── app.py                    ← Streamlit dashboard (4 pages)
└── api.py                    ← FastAPI (13 endpoints)

sql/ddl/                      ← schema, dims, fact, 5 analytics views
dags/elden_ring_pipeline_dag.py
tests/                        ← 50 tests (scaling engine + telemetry)
docker-compose.yml
```

---

## Interactive tools

**CLI Calculator** — pick a boss, pick a weapon + upgrade level, enter your stats. Prints your AR breakdown, hits to kill, where your AR ranks against other players who fought that boss, and the top 5 weapons for that matchup.

**Build Advisor** — full menu-driven tool. Choose a starting class, level up stats, see soft cap advice (which stat gives the most AR per point), run the full boss gauntlet ranked by difficulty score, save builds to the warehouse, compare saved builds side by side.

**Streamlit dashboard** — four pages: Build Advisor (sliders + AR pie chart + soft cap bar chart), Boss Gauntlet (horizontal bar chart), Analytics Dashboard (boss lethality, weapon win rates, archetype tier list), Saved Builds (table + comparison chart).

**FastAPI** — 13 endpoints covering everything above plus `/api/meta` for the live meta report. Swagger UI at `/docs`.

---

## Data sources

- **Elden Ring Fan API** — weapons, bosses, armors. Paginated with exponential back-off.
- **Kaggle Elden Ring dataset** — `weapons_upgrades.csv` (60,201 rows), `bosses.csv` (153 rows). Attack power and scaling stored as Python dict strings, parsed with `ast.literal_eval()`.
- **Synthetic telemetry** — generated using the scaling engine itself to produce realistic win probability based on weapon AR vs boss HP. High-AR builds vs low-HP bosses win more; DLC bosses have a penalty.

---

## Schema

```
dim_weapons  (60,201 rows)     dim_bosses  (153 rows)
weapon_name                    boss_name
upgrade_level                  hp / is_dlc
scale_str/dex/int/fai/arc      drops_runes
base_physical/magic/...
                    │
                    └──► fact_encounters
                         event_id  (UNIQUE — dedup)
                         player_id / player_level / archetype
                         strength_stat … arcane_stat
                         boss_name / boss_hp
                         weapon_used / upgrade_level
                         calculated_ar   ← scaling engine output
                         is_victory
```

Kafka messages are keyed by `boss_id` so all events for a boss land on the same partition. `event_id UNIQUE` on `fact_encounters` handles Kafka redelivery deduplication silently at the DB level. Spark checkpoints at `data/checkpoints/` mean a consumer restart picks up from the last committed offset.

---

## Tests

```bash
python -m pytest tests/ -v          # 50 tests
python -m pytest tests/test_scaling_engine.py -v    # 31 — soft caps, grades, UDF, two-handing
python -m pytest tests/test_telemetry_generator.py -v  # 19 — schema, distribution, pools
```

---

## Airflow DAG

`golden_order_pipeline` runs `@daily`. Task order:

```
start
 ├── extract_api_weapons ─┐
 ├── extract_api_bosses   ├──► api_and_kaggle_done ──► refresh_stg_weapons ─┐
 └── load_kaggle_data ────┘                           refresh_stg_bosses   ─┴──► build_dim_tables ──► validate_row_counts ──► end
```

Every task is idempotent — safe to re-run without producing duplicates.

---

## Config

Copy `.env.example` to `.env`. Key variables:

```ini
WAREHOUSE_HOST=localhost
WAREHOUSE_PORT=5432
WAREHOUSE_USER=eruser
WAREHOUSE_PASSWORD=erpass
WAREHOUSE_DB=elden_ring_dw
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
TELEMETRY_EVENTS_PER_SECOND=500
```

---

## Troubleshooting

**`No module named 'src'` in Airflow** — containers need to be restarted after the `PYTHONPATH: /opt/airflow` env var is set in docker-compose.yml: `docker compose down && docker compose up -d`

**Streamlit `No module named 'src'`** — run from the project root: `streamlit run src/app.py` (the path fix is already in the file).

**Kafka connection refused on startup** — broker takes ~30s to be ready. The producer has built-in retry but if you start it immediately after `docker compose up` you may see a brief refusal.

**numpy DLL error on Python 3.13 (Windows)** — use Python 3.11 or 3.12. The scaling engine and tests don't need numpy; the Kaggle loader and dim builder do.
