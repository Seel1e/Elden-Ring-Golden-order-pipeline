# The Golden Order Data Pipeline
### Elden Ring Combat & Build Analytics

A production-grade end-to-end data engineering pipeline that ingests complex, nested game asset data and high-velocity synthetic player telemetry to uncover statistically optimal weapon builds and the most lethal bosses in the Lands Between.

---

## Table of Contents

1. [Project Overview](#1-project-overview)
2. [Architecture](#2-architecture)
3. [Technology Stack](#3-technology-stack)
4. [Repository Structure](#4-repository-structure)
5. [Data Sources](#5-data-sources)
6. [The Scaling Engine — Soft Cap Math](#6-the-scaling-engine--soft-cap-math)
7. [Prerequisites](#7-prerequisites)
8. [Quick Start — No Docker Demo](#8-quick-start--no-docker-demo)
9. [Full Stack Setup — Docker](#9-full-stack-setup--docker)
10. [Phase 1: Data Ingestion](#10-phase-1-data-ingestion)
11. [Phase 2: Dimensional Modeling](#11-phase-2-dimensional-modeling)
12. [Phase 3: Streaming Pipeline](#12-phase-3-streaming-pipeline)
13. [Phase 4: Analytics & Dashboards](#13-phase-4-analytics--dashboards)
14. [Interactive Tools](#14-interactive-tools)
15. [REST API](#15-rest-api)
16. [Orchestration with Airflow](#16-orchestration-with-airflow)
17. [Running the Tests](#17-running-the-tests)
18. [Configuration Reference](#18-configuration-reference)
19. [SQL Schema Reference](#19-sql-schema-reference)
20. [Makefile Commands](#20-makefile-commands)
21. [Design Decisions & Idempotency](#21-design-decisions--idempotency)
22. [Troubleshooting](#22-troubleshooting)

---

## 1. Project Overview

This pipeline answers two core analytical questions:

- **Which bosses kill the most players?** — Ranked by death rate across all encounter events
- **Which weapon builds win the most fights?** — Ranked by win rate per weapon × stat archetype

To answer these questions correctly, the pipeline must solve a non-trivial mathematical challenge: weapon damage in Elden Ring is not a flat number. It scales **non-linearly** with player stats through a **soft-cap system** where returns diminish sharply at stats 60 and 80. The central engineering effort of this project is the **Scaling Engine** — a Python/Spark UDF that implements this exact math at streaming scale.

### Pipeline Flow (high level)

```
[Elden Ring Fan API]  ──► Data Lake (JSON) ──►
[Kaggle CSV Dataset]  ──► Staging (Parquet) ──► dim_weapons ──►
                                               dim_bosses   ──► fact_encounters ──► Dashboards
[Synthetic Telemetry] ──► Kafka ──► Spark Streaming ──► (enriched with AR via UDF)
```

---

## 2. Architecture

Open `architecture/pipeline_architecture.html` in any browser to view the full visual architecture diagram (no dependencies required — pure SVG).

```
╔══════════════════════════════════════════════════════════════════════╗
║                    DATA SOURCES                                      ║
║  [Elden Ring Fan API]  [Kaggle CSV Dataset]  [Telemetry Generator]   ║
╠══════════════════════════════════════════════════════════════════════╣
║                    INGESTION  (Phase 1)                              ║
║  api_extractor.py      kaggle_loader.py       kafka_producer.py      ║
║  retry/pagination      CSV → Parquet           confluent-kafka        ║
╠══════════════════════════════════════════════════════════════════════╣
║                    STORAGE                                           ║
║  Data Lake             Staging Area            Kafka Topic            ║
║  data/raw/*.json       data/staged/*.parquet   player_encounters      ║
╠══════════════════════════════════════════════════════════════════════╣
║                    TRANSFORMATION  (Phase 2 + 3)                     ║
║  json_flattener.py  ──► ★ scaling_engine.py ──► spark_consumer.py   ║
║  Parse nested dicts     Soft-cap AR UDF         Structured Streaming  ║
╠══════════════════════════════════════════════════════════════════════╣
║            DATA WAREHOUSE  (golden_order schema)                     ║
║  ┌──────────────┐  ┌─────────────┐  ┌──────────────────────────┐    ║
║  │ dim_weapons  │  │ dim_bosses  │  │     fact_encounters       │    ║
║  │ 60K rows     │  │ 154 rows    │  │  streaming sink           │    ║
║  │ AR pre-calc  │  │ HP + drops  │  │  event_id UNIQUE          │    ║
║  └──────────────┘  └─────────────┘  └──────────────────────────┘    ║
╠══════════════════════════════════════════════════════════════════════╣
║                    ANALYTICS  (Phase 4)                              ║
║  SQL Views ──► Metabase Dashboard                                    ║
║  v_boss_lethality  /  v_weapon_win_rates  /  v_best_weapons_per_boss ║
╠══════════════════════════════════════════════════════════════════════╣
║                    INTERACTIVE LAYER  (Phase 5)                      ║
║  calculator.py  advisor.py  ──► CLI tools (terminal)                 ║
║  app.py (Streamlit) ──────────► Web UI  :8501                        ║
║  api.py (FastAPI)   ──────────► REST API :8000  /docs (Swagger)      ║
║  analytics/meta_report.py ────► Daily snapshot → fact_meta_reports   ║
╠══════════════════════════════════════════════════════════════════════╣
║  ORCHESTRATION: Apache Airflow DAG  @daily  (golden_order_pipeline)  ║
╚══════════════════════════════════════════════════════════════════════╝
```

---

## 3. Technology Stack

| Layer | Technology | Purpose |
|---|---|---|
| **Language** | Python 3.11+ | All pipeline scripts, UDFs, generators |
| **API Ingestion** | `requests` + `tenacity` | Paginated REST calls with exponential back-off |
| **Data Processing** | `pandas` + `pyarrow` | CSV loading, JSON flattening, Parquet staging |
| **Data Warehouse** | PostgreSQL 15 | Star schema (simulates BigQuery / Snowflake) |
| **ORM / DDL** | `SQLAlchemy` | Warehouse connections, upserts |
| **Message Broker** | Apache Kafka 7.6 | High-velocity encounter event streaming |
| **Zookeeper** | Confluent Zookeeper 7.6 | Kafka cluster coordination |
| **Stream Processing** | Apache Spark 3.5 (PySpark) | Structured Streaming consumer, UDF compute |
| **Orchestration** | Apache Airflow 2.8 | DAG scheduling, task dependency, retries |
| **Containerisation** | Docker + Docker Compose | Full local stack (all services) |
| **Testing** | `pytest` | 50 tests across 2 suites |
| **Logging** | `loguru` | Structured logs across all modules |
| **Web App** | `Streamlit` | 4-page interactive analytics dashboard |
| **REST API** | `FastAPI` + `Pydantic v2` | 13 JSON endpoints, auto Swagger docs |
| **Diagram** | Pure Python SVG | Architecture diagram, zero dependencies |

---

## 4. Repository Structure

```
project 2/
│
├── .env.example                  ← Copy to .env and configure
├── docker-compose.yml            ← Full infrastructure stack
├── requirements.txt              ← Python dependencies
├── Makefile                      ← All common commands
│
├── eldenringScrap/               ← Kaggle dataset (pre-loaded)
│   ├── weapons.csv               ─ 402 weapons
│   ├── weapons_upgrades.csv      ─ 60,201 upgrade rows (+0 to +25)
│   ├── bosses.csv                ─ 153 bosses
│   ├── shields.csv / shields_upgrades.csv
│   ├── armors.csv, talismans.csv, skills.csv …
│   └── items/                    ─ ammos, consumables, keys …
│
├── src/
│   ├── config.py                 ← Central config (reads .env)
│   │
│   ├── ingestion/
│   │   ├── api_extractor.py      ← Phase 1: REST API → data lake
│   │   └── kaggle_loader.py      ← Phase 1: CSV → Parquet staging
│   │
│   ├── transformation/
│   │   ├── scaling_engine.py     ← ★ THE SCALING ENGINE ★
│   │   ├── json_flattener.py     ← Nested dict → typed columns
│   │   └── dim_builder.py        ← Build + load dim_weapons, dim_bosses
│   │
│   ├── streaming/
│   │   ├── telemetry_generator.py ← Synthetic encounter event generator
│   │   ├── kafka_producer.py      ← Phase 3: Kafka producer
│   │   └── spark_consumer.py      ← Phase 3: Spark Structured Streaming
│   │
│   ├── analytics/
│   │   ├── queries.py             ← Phase 4: BI query wrappers
│   │   └── meta_report.py         ← Daily meta snapshot → fact_meta_reports
│   │
│   ├── calculator.py              ← Interactive CLI: AR report vs a chosen boss
│   ├── advisor.py                 ← Full build advisor: class picker, gauntlet, saves
│   └── app.py                     ← Streamlit 4-page web dashboard
│
├── src/
│   └── api.py                     ← FastAPI REST service (13 endpoints)
│
├── sql/
│   ├── ddl/
│   │   ├── 01_create_schema.sql   ← golden_order schema bootstrap
│   │   ├── 02_dim_weapons.sql     ← Weapon dimension table + indexes
│   │   ├── 03_dim_bosses.sql      ← Boss dimension table
│   │   ├── 04_fact_encounters.sql ← Encounter fact table + BRIN index
│   │   └── 05_analytics_views.sql ← 5 analytics views for Metabase
│   └── transforms/
│       ├── 01_staging_weapons.sql ← Raw API JSON → stg_weapons procedure
│       └── 02_staging_bosses.sql  ← Raw API JSON → stg_bosses procedure
│
├── dags/
│   └── elden_ring_pipeline_dag.py ← Airflow DAG (all 4 phases)
│
├── architecture/
│   ├── diagram.py                 ← Generates the SVG diagram
│   ├── pipeline_architecture.svg  ← Generated diagram
│   └── pipeline_architecture.html ← Open in browser to view
│
├── tests/
│   ├── test_scaling_engine.py     ← 31 tests for soft-cap math
│   └── test_telemetry_generator.py ← 19 tests for event generation
│
└── data/
    ├── raw/          ← API JSON pages land here
    ├── staged/       ← Parquet files land here
    └── checkpoints/  ← Spark streaming checkpoints
```

---

## 5. Data Sources

### 5.1 Elden Ring Fan API

- **URL**: `https://eldenring.fanapis.com/api`
- **Type**: REST / JSON / paginated
- **Endpoints used**: `/weapons`, `/bosses`, `/armors`, `/shields`, `/spells`
- **Pagination**: `?limit=100&page=N` cursor. The extractor loops until the API returns fewer than `limit` records.
- **Rate limiting**: Configurable delay between pages (default 250 ms). Exponential back-off on HTTP 429 / 5xx with up to 5 retries.
- **Nested structure example**:
  ```json
  {
    "name": "Moonveil",
    "attack": { "physical": 73, "magic": 87 },
    "scalesWith": [
      { "name": "Strength", "scaling": "D" },
      { "name": "Dexterity", "scaling": "C" },
      { "name": "Intelligence", "scaling": "B" }
    ],
    "requiredAttributes": [ { "name": "Strength", "amount": 12 } ]
  }
  ```

### 5.2 Kaggle Elden Ring Ultimate Dataset (`eldenringScrap/`)

Pre-loaded local CSVs scraped from the Elden Ring Fextralife wiki. Used for bulk backfill without hitting API rate limits.

| File | Rows | Description |
|---|---|---|
| `weapons.csv` | 402 | Base weapon stats, category, skill, requirements |
| `weapons_upgrades.csv` | 60,201 | All weapons at every upgrade level (+0 to +25) with attack power dicts and scaling grade dicts |
| `bosses.csv` | 153 | Boss names, HP, rune drops, DLC flag |
| `shields.csv` | 100 | Shield base stats |
| `shields_upgrades.csv` | 28,286 | Shield upgrade table |
| `armors.csv`, `talismans.csv`, `skills.csv` … | varies | Supporting reference data |

**Key parsing challenge**: Attack power and scaling are stored as Python dict strings:
```
attack power  →  "{'Phy': '125 ', 'Mag': '- ', 'Fir': '- ', 'Lit': '- ', 'Hol': '- ', 'Sta': '70 '}"
stat scaling  →  "{'Str': 'D ', 'Dex': 'D ', 'Int': '- ', 'Fai': '- ', 'Arc': '- '}"
```
`json_flattener.py` uses `ast.literal_eval()` to safely parse these and expands them into 12 typed columns.

### 5.3 Synthetic Player Telemetry

Generated by `telemetry_generator.py`. Each event is a JSON object representing one player–boss encounter:

```json
{
  "event_id":          "16e3b8c9-241f-475e-8dc4-2ce7fcb558b2",
  "event_timestamp":   "2026-03-30T05:40:37.621194+00:00",
  "player_id":         "c901099a-87f2-4a9a-b0ff-2aa9aacf7b19",
  "player_level":      29,
  "archetype":         "str",
  "strength_stat":     47,
  "dexterity_stat":    22,
  "intelligence_stat": 9,
  "faith_stat":        9,
  "arcane_stat":       9,
  "two_handing":       false,
  "boss_id":           "57",
  "boss_name":         "Death Rite Bird",
  "weapon_used":       "Flowing Curved Sword",
  "upgrade_level":     25,
  "outcome":           "victory",
  "win_probability":   0.6103
}
```

**Outcome model**: Win probability is calculated using the Scaling Engine itself:
1. Compute the weapon's true AR at the player's stats.
2. Estimate normalised TTK = `boss_hp / AR / 20` (20 hits is the reference).
3. Apply sigmoid: `win_prob = 1 / (1 + normalised_ttk)`.
4. Add a level bonus (+0.3% per level above 60) and a DLC penalty (-10%).
5. Clip to `[0.05, 0.95]`.

This ensures the generated data has analytically interesting correlations — high-AR weapons against low-HP bosses yield high win rates, DLC bosses are harder, higher-level players win more often.

---

## 6. The Scaling Engine — Soft Cap Math

**File**: `src/transformation/scaling_engine.py`

This is the core intellectual contribution of the project. FromSoftware's weapon damage formula is non-linear. Here is how it works and how we implemented it.

### 6.1 The Formula

```
Total AR = base_damage + Σ scaling_bonus(stat, grade, base_damage)

scaling_bonus = base_damage × GRADE_MAX_MULTIPLIER[grade] × correction(stat_value)
```

Where `correction(stat_value)` is a piecewise-linear function over the soft-cap breakpoints.

### 6.2 Soft Cap Correction Curve

The correction function maps a player's stat value (1–99) to a percentage of the weapon's maximum grade bonus (0.0–1.0):

| Stat Value | Correction % | Note |
|---|---|---|
| 0–18 | 0% | Below minimum — no scaling |
| 20 | 5% | Scaling begins |
| 30 | 21% | |
| 40 | 37% | |
| 50 | 55% | |
| **60** | **70%** | **First soft cap — slope drops sharply** |
| 70 | 80% | |
| **80** | **90%** | **Second soft cap — slope drops again** |
| 90 | 95% | |
| 99 | 100% | Maximum scaling achieved |

**What this means in practice** (Giant-Crusher, S-STR, 155 base physical):

| STR | Total AR | Gain per next point |
|---|---|---|
| 20 | 178.2 | +7.4 |
| 40 | 327.1 | +8.4 |
| 60 | 480.5 | +4.7 — **first cap hit** |
| 80 | 573.5 | +2.3 — **second cap hit** |
| 99 | 620.0 | +0.0 |

Two-handing at STR 40 gives effective STR 60, producing the same AR as 60 STR one-handed — this is a first-class feature of the Scaling Engine.

### 6.3 Grade Multipliers

| Grade | Max AR bonus (as × of base) |
|---|---|
| S | 3.00× |
| A | 2.50× |
| B | 1.75× |
| C | 1.15× |
| D | 0.75× |
| E | 0.45× |
| − | 0.00× |

### 6.4 Stat-to-Damage-Type Mapping

| Stat | Damage type it scales |
|---|---|
| STR, DEX | Physical |
| INT | Magic |
| FAI | Holy, Fire |
| ARC | Fire, Lightning |

### 6.5 Performance at Scale

The Scaling Engine is used in three modes:

1. **Pandas batch (dim_builder.py)** — Scalar Python UDF applied per row to build the `dim_weapons` table with pre-computed AR across 10 stat profiles. Runs in seconds on 60K rows.

2. **PySpark UDF (spark_consumer.py)** — `_spark_udf_calculate_ar()` is registered with `spark.udf.register()`. It accepts only primitive types (no dataclasses) so Spark can serialise it across the cluster. Called once per streaming micro-batch row after the dim join resolves base damage.

3. **Vectorised NumPy (optional)** — `vectorised_ar()` uses `np.vectorize()` for ~40× faster batch computation when numpy is available. Falls back to list comprehension when numpy is unavailable (e.g., restricted environments).

---

## 7. Prerequisites

### Required (for demo without Docker)

- Python 3.11 or 3.12 (Python 3.13 has a numpy DLL issue on Windows — use 3.11/3.12 for numpy support)
- Git

### Required (for full stack)

- Docker Desktop 4.x+
- Docker Compose v2+
- 8 GB RAM available for Docker (Kafka + Spark + Airflow + PostgreSQL)
- Ports free: `5432` (PostgreSQL), `9092` (Kafka), `8080` (Spark UI), `8081` (Airflow)

### Optional

- Metabase (for the analytics dashboard) — connect to PostgreSQL at `localhost:5432`

---

## 8. Quick Start — No Docker Demo

This demonstrates the core pipeline components (Scaling Engine + Telemetry Generator) without any external services.

```bash
# 1. Clone / navigate to the project directory
cd "project 2"

# 2. Create a virtual environment
python -m venv .venv

# Windows
.venv\Scripts\activate

# macOS / Linux
source .venv/bin/activate

# 3. Install dependencies
pip install -r requirements.txt

# 4. Copy and review config (defaults work for demo)
cp .env.example .env

# 5. Run the Scaling Engine demo
python -m src.transformation.scaling_engine
```

**Expected output**:
```
=== Giant-Crusher (S/D) at different STR values ===
  STR  20: total AR =    178.2  (+7.44 per next point)
  STR  40: total AR =    327.1  (+8.37 per next point)
  STR  60: total AR =    480.5  (+4.65 per next point)   ← first soft cap
  STR  80: total AR =    573.5  (+2.33 per next point)   ← second soft cap
  STR  99: total AR =    620.0  (+0.00 per next point)

STR 40 one-hand: 327.1  |  two-hand (eff. STR 60): 480.5
```

```bash
# 6. Run the telemetry generator (generates 10 sample events)
python -m src.streaming.telemetry_generator

# 7. Generate the architecture diagram
python -m architecture.diagram
# Then open: architecture/pipeline_architecture.html
```

---

## 9. Full Stack Setup — Docker

```bash
# 1. Start all services
docker compose up -d

# 2. Wait for services to be healthy (about 60 seconds)
docker compose ps   # check STATUS = healthy / running

# 3. Initialise the warehouse schema
# The DDL files in sql/ddl/ are auto-executed by PostgreSQL on first start.
# If you need to run them manually:
docker exec -it er_warehouse psql -U eruser -d elden_ring_dw \
  -f /docker-entrypoint-initdb.d/01_create_schema.sql

# 4. Create the Airflow admin user (first time only)
docker compose run --rm airflow-init

# 5. Open the Airflow UI
# http://localhost:8081
# Username: admin  |  Password: admin

# 6. Open the Spark UI
# http://localhost:8080

# 7. To stop everything
docker compose down
```

### Docker Services

| Service | Container | Port | Purpose |
|---|---|---|---|
| `postgres-warehouse` | `er_warehouse` | 5432 | Data warehouse (star schema) |
| `postgres-meta` | `er_airflow_meta` | — | Airflow metadata store |
| `zookeeper` | `er_zookeeper` | 2181 | Kafka coordination |
| `kafka` | `er_kafka` | 9092 | Message broker |
| `kafka-init` | `er_kafka_init` | — | Creates topics on startup |
| `spark-master` | `er_spark_master` | 7077, 8080 | Spark master + UI |
| `spark-worker` | `er_spark_worker` | — | 2 cores, 2 GB RAM |
| `airflow-webserver` | `er_airflow_web` | 8081 | Airflow UI |
| `airflow-scheduler` | `er_airflow_scheduler` | — | DAG scheduler |

---

## 10. Phase 1: Data Ingestion

### 10.1 Extract from Elden Ring Fan API

```bash
# Extract all configured endpoints (weapons, bosses, armors, shields, spells)
python -m src.ingestion.api_extractor
```

**What it does**:
- Loops pages with `?limit=100&page=N` until the API returns fewer than 100 records
- Writes each page as `data/raw/{endpoint}/{YYYY-MM-DD}/{page:04d}.json`
- Re-running on the same day overwrites the same files — **idempotent by design**
- Failed pages are logged and skipped — partial data is better than aborting the run

**Output example**:
```
INFO | /weapons: extracted 308 records across 4 pages
INFO | /bosses:  extracted 178 records across 2 pages
INFO | Extraction complete: {'weapons': 308, 'bosses': 178, ...}
```

**To extract a single endpoint**:
```python
from src.ingestion.api_extractor import EldenRingAPIExtractor
extractor = EldenRingAPIExtractor()
extractor.extract_endpoint("weapons")
```

### 10.2 Load Kaggle Dataset

```bash
python -m src.ingestion.kaggle_loader
```

**What it does**:
- Reads `eldenringScrap/weapons.csv` + `weapons_upgrades.csv`, joins them on weapon name
- Parses all `dict-as-string` columns (`attack power`, `stat scaling`, `requirements`, `damage reduction`) using `ast.literal_eval()`
- Keeps the highest upgrade level per weapon (max +25)
- Writes output to `data/staged/stg_weapons_upgrades.parquet` (Snappy compressed)
- Also processes `bosses.csv`, `shields.csv` + `shields_upgrades.csv`

**Output example**:
```
INFO | Weapons loaded: 60,201 rows (402 unique weapons)
INFO | Staged 60,201 rows → data/staged/stg_weapons_upgrades.parquet
INFO | Bosses loaded: 153 rows
INFO | Kaggle load complete: {'weapons_upgrades': 60201, 'bosses': 153}
```

### 10.3 Airflow Connection (for automated ingestion)

In the Airflow UI, add a connection:
- **Conn ID**: `elden_ring_warehouse`
- **Conn Type**: Postgres
- **Host**: `postgres-warehouse`
- **Schema**: `elden_ring_dw`
- **Login**: `eruser`
- **Password**: `erpass`
- **Port**: `5432`

---

## 11. Phase 2: Dimensional Modeling

### 11.1 JSON Flattening

The `json_flattener.py` module expands nested dict columns from the Kaggle staging data:

```
Input:  attack_power_parsed = '{"Phy": "125 ", "Mag": "- ", "Fir": "- ", "Lit": "- ", "Hol": "- ", "Sta": "70 "}'
Output: atk_physical=125.0, atk_magic=0.0, atk_fire=0.0, atk_lightning=0.0, atk_holy=0.0, atk_stamina=70.0

Input:  stat_scaling_parsed = '{"Str": "D ", "Dex": "D ", "Int": "- ", "Fai": "- ", "Arc": "- "}'
Output: scale_str="D", scale_dex="D", scale_int="-", scale_fai="-", scale_arc="-"
```

Columns produced: `atk_physical`, `atk_magic`, `atk_fire`, `atk_lightning`, `atk_holy`, `atk_stamina`, `scale_str`, `scale_dex`, `scale_int`, `scale_fai`, `scale_arc`, `dr_physical`, `dr_magic`, `dr_fire`, `dr_lightning`, `dr_holy`, `req_str`, `req_dex`, `req_int`, `req_fai`, `req_arc`

### 11.2 Build Dimensional Tables

```bash
python -m src.transformation.dim_builder
```

**What it does**:
1. Loads the staged Parquet files
2. Flattens JSON columns
3. For every (weapon × upgrade level) row, calls the Scaling Engine across 10 representative stat profiles:
   - `str_build_60` — STR 60 / DEX 15
   - `dex_build_60` — STR 13 / DEX 60
   - `quality_build_60` — STR 60 / DEX 60
   - `int_build_60` — INT 60
   - `faith_build_60` — FAI 60
   - `arcane_build_60` — ARC 60
   - `str_build_80`, `dex_build_80` — past second soft cap
   - `two_hand_str_40` — STR 40 two-handed (effective STR 60)
   - `beginner_30_all` — STR/DEX 30
4. Stores pre-computed AR as individual columns (`ar_str_build_60` etc.) + full JSON map
5. **Upserts** into `dim_weapons` via `INSERT … ON CONFLICT (weapon_name, upgrade_level, upgrade_type) DO UPDATE`
6. Same for `dim_bosses`

### 11.3 Star Schema

```
dim_weapons (60,201 rows)          dim_bosses (153 rows)
─────────────────────────          ─────────────────────
weapon_key (PK)                    boss_key (PK)
weapon_name                        boss_name
upgrade_level                      hp
upgrade_type                       is_dlc
category                           drops_runes
scale_str / dex / int / fai / arc  drops_items
ar_str_build_60                    loaded_at
ar_dex_build_60
ar_quality_build_60                    │
ar_int_build_60                        │
ar_faith_build_60                      │
ar_profiles_json                       │
                                       ▼
                         fact_encounters
                         ───────────────────────────
                         encounter_id (PK BIGSERIAL)
                         event_id (UNIQUE — dedup)
                         event_ts (watermarked)
                         player_id
                         player_level / archetype
                         strength_stat … arcane_stat
                         two_handing
                         boss_id / boss_name / boss_hp
                         weapon_used / upgrade_level
                         calculated_ar  ← from Scaling Engine UDF
                         outcome (victory | death)
                         is_victory (generated column)
                         loaded_at
```

---

## 12. Phase 3: Streaming Pipeline

### 12.1 Start the Kafka Producer

```bash
# Start generating and publishing encounter events
python -m src.streaming.kafka_producer
```

**Configuration** (in `.env`):
```
TELEMETRY_EVENTS_PER_SECOND=500    # target throughput
TELEMETRY_NUM_PLAYERS=10000        # player pool size
TELEMETRY_RUN_DURATION_S=0         # 0 = run forever
```

**Key design features**:
- **Partitioning key**: `boss_id` — all events for the same boss land on the same partition, enabling ordered per-boss aggregations in the consumer
- **Compression**: LZ4 — fast, CPU-efficient compression for JSON payloads
- **Micro-batching**: 5ms linger + 64KB batch size for throughput
- **Async delivery**: `on_delivery` callback logs failures; doesn't block the produce loop
- **Dead Letter Queue**: Failed events are redirected to `player_encounters_dlq`
- **Graceful shutdown**: SIGINT/SIGTERM flushes in-flight messages before exit

```bash
# Check the topic has data
docker exec -it er_kafka kafka-console-consumer \
  --bootstrap-server localhost:29092 \
  --topic player_encounters \
  --max-messages 5
```

### 12.2 Start the Spark Streaming Consumer

```bash
# Submit to the Spark cluster
python -m src.streaming.spark_consumer
```

**What it does per micro-batch (every 10 seconds)**:
1. Reads up to 50,000 events from Kafka (`maxOffsetsPerTrigger`)
2. Parses JSON with the defined schema (malformed events are `null` — not lost)
3. Applies 30-minute event-time watermark (bounds state store size)
4. **Broadcast joins** against `dim_weapons` and `dim_bosses` (loaded once, cached)
5. Calls `calculate_ar()` Spark UDF with the player's **actual** stats and the weapon's base damage from the dim join — this is the exact true AR, not a pre-computed approximation
6. Writes enriched rows to `fact_encounters` via JDBC (`mode=append`)

**Fault tolerance**:
- Checkpoints at `data/checkpoints/spark_encounters/` — pipeline resumes from last committed offset after restart
- `event_id UNIQUE` constraint on `fact_encounters` — Kafka re-deliveries on consumer restart are silently deduplicated by PostgreSQL

### 12.3 Kafka Topics

| Topic | Partitions | Retention | Purpose |
|---|---|---|---|
| `player_encounters` | 6 | 24 hours | Live encounter event stream |
| `player_encounters_dlq` | 2 | 24 hours | Failed / malformed events |

---

## 13. Phase 4: Analytics & Dashboards

### 13.1 Run Analytics Queries

```bash
python -m src.analytics.queries
```

This prints a full terminal report:
- Top 15 most lethal bosses by death rate
- Top 20 weapon builds by win rate
- Stat distribution by archetype (with soft-cap breach detection)

### 13.2 SQL Analytics Views

All views are in `sql/ddl/05_analytics_views.sql` and are created automatically when the warehouse initialises.

| View | Description |
|---|---|
| `v_boss_lethality` | Death rate, avg AR at death, avg player level per boss. Ordered by death rate descending. |
| `v_weapon_win_rates` | Win rate per (weapon × upgrade × category × archetype). Min 20 samples. |
| `v_stat_distribution` | Average stats and soft-cap breach % per build archetype. |
| `v_rolling_win_rate_1h` | Per-minute win rate for the last 60 minutes (near real-time). |
| `v_best_weapons_per_boss` | Top 5 weapons by win rate for each boss. Min 15 samples per pair. |

**Example queries**:

```sql
-- Most lethal bosses
SELECT boss_name, death_rate_pct, avg_ar_on_death, avg_player_level
FROM golden_order.v_boss_lethality
LIMIT 10;

-- Best greatsword for quality builds
SELECT weapon_used, upgrade_level, win_rate_pct, avg_ar
FROM golden_order.v_weapon_win_rates
WHERE category = 'Greatswords' AND archetype = 'quality'
ORDER BY win_rate_pct DESC
LIMIT 5;

-- Is it worth going past STR 60?
SELECT pct_str_past_second_softcap, avg_ar, avg_str
FROM golden_order.v_stat_distribution
WHERE archetype = 'str';
```

### 13.3 Connecting Metabase

1. Install Metabase: `docker run -p 3000:3000 metabase/metabase`
2. Open `http://localhost:3000` and complete setup
3. Add database:
   - **Type**: PostgreSQL
   - **Host**: `localhost` (or `host.docker.internal` from inside Docker)
   - **Port**: `5432`
   - **Database**: `elden_ring_dw`
   - **Username**: `eruser`
   - **Password**: `erpass`
4. Create dashboards using the `golden_order.*` views as data sources

---

## 14. Interactive Tools

Five tools sit on top of the warehouse and give you a hands-on way to explore every result the pipeline produces.

### 14.1 Build Calculator (CLI)

```bash
python -m src.calculator
```

Pick a boss, pick a weapon and upgrade level, enter your 5 stats — get an instant report:

```
============================================================
  REPORT: Rivers of Blood +25  vs  Malenia, Blade of Miquella
============================================================

  YOUR ATTACK RATING
  |- Total AR    : 812.4
  |- Physical    : 312.4
  |- Fire        : 250.0
  |- Bleed       : 250.0
  \- Two-handing : No

  BOSS INFO
  |- Boss HP     : 33,251
  |- DLC boss    : No
  \- Hits to kill: ~41  (ignoring resistances / phase changes)

  COMMUNITY STATS  (18,432 encounters in pipeline)
  |- Avg player AR   : 543.2
  |- Your AR ranks   : top 8% of players who fought this boss
  |- Overall win rate: 34.6%
  \- Avg player level: 87.3

  TOP 5 WEAPONS vs Malenia, Blade of Miquella
  Weapon                           Upgrade    Win%   Avg AR   Uses
  ----------------------------------------------------------
  Rivers of Blood                      +25   52.3%    798.1   3241  << YOU
  Moonveil                             +10   48.1%    612.4   2817
  Giant-Crusher                        +25   45.7%    891.2   1932
  Blasphemous Blade                    +25   44.9%    703.8   1654
  Eleonora's Poleblade                 +25   43.2%    621.9   1421
============================================================
```

### 14.2 Build Advisor (CLI)

The full interactive advisor — choose a starting class, optimise stats, run the gauntlet, and save builds to the warehouse.

```bash
python -m src.advisor
```

**Main menu**:
```
============================================================
  GOLDEN ORDER BUILD ADVISOR
============================================================

  1. Set starting class + level + stats
  2. Set weapon + upgrade level
  3. View AR breakdown + soft cap advice
  4. Boss Gauntlet — rank all bosses for your build
  5. Save this build to the warehouse
  6. Compare saved builds
  7. Full run (1 + 2 + 3 + 4 + save)
  0. Exit
```

**Soft cap advice output** (step 3):

```
  SOFT CAP ADVISOR — where to invest next 5 levels
  Stat          Current  AR gain (+5)
  ------------------------------------------
  Arcane             60     +34.2   *** INVEST HERE
  Dexterity          40     +18.7
  Strength           18     +11.1
  Faith              10      +0.0
  Intelligence       10      +0.0
```

**Boss Gauntlet output** (step 4):

```
  BOSS GAUNTLET — Rivers of Blood +25 | ARC 60 / DEX 40
  Your AR: 812.4
  Rank  Boss                           HP       Difficulty  DR%    Hits
  -------------------------------------------------------------------
     1  Training Dummy                  0          0.0     0.0%      0
     2  Soldier of Godrick          1,104          8.4    11.0%      2
     3  Flying Dragon Agheel       12,647         27.1    22.0%     16
    ...
   152  Malenia, Blade of Miquella  33,251         73.4    65.4%     41
   153  Elden Beast                 18,486         81.2    76.1%     23
```

### 14.3 Streamlit Web Dashboard

```bash
streamlit run src/app.py
# Opens at http://localhost:8501
```

Four pages:

| Page | What you can do |
|---|---|
| **Build Advisor** | Slider-based stat builder, AR pie chart (physical/magic/fire/lightning/holy), soft cap bar chart, save build button |
| **Boss Gauntlet** | Horizontal bar chart of all bosses ranked easiest → hardest for your current build |
| **Analytics Dashboard** | Live pipeline metrics: total encounters, overall win rate, boss lethality bar chart, weapon win rate scatter, archetype tier bar + radar chart |
| **Saved Builds** | Sortable table of all saved builds, side-by-side AR comparison chart, delete build |

### 14.4 Meta Report

Generates and persists a full daily meta snapshot to `golden_order.fact_meta_reports`.

```bash
python -m src.analytics.meta_report
```

**Sample output**:
```
==============================================================
  GOLDEN ORDER META REPORT
  Generated: 2026-03-31T00:00:12 UTC
==============================================================

  Total encounters : 2,847,391
  Overall win rate : 41.3%

  Hardest boss  : Malenia, Blade of Miquella (65.4% death rate)
  Easiest boss  : Soldier of Godrick (11.0% death rate)

  Top weapon    : Rivers of Blood (52.1% win rate)
  Most used     : Uchigatana (148,231 uses)

  Best archetype: arc (49.3% win rate)

  ARCHETYPE TIER LIST
  Rank  Archetype     Win Rate    Avg AR    Encounters
  ----------------------------------------------------
     1  arc              49.3%    721.4       412,847
     2  quality          47.1%    683.2       389,124
     3  dex              45.8%    612.7       441,203
     4  str              44.2%    834.9       398,711
     5  int              43.1%    598.4       311,442
     6  faith            41.7%    623.1       287,903
     7  hex              40.2%    589.7       206,161

  TOP 10 WEAPON BUILDS
  Weapon                           Upg   Win%    Avg AR    Uses
  ------------------------------------------------------------
  Rivers of Blood                  +25   52.1%    812.4    3,241
  Moonveil                         +10   48.3%    612.4    2,817
  Giant-Crusher                    +25   46.9%    891.2    1,932
  ...
==============================================================
```

---

## 15. REST API

```bash
uvicorn src.api:app --reload --port 8000
# Swagger UI: http://localhost:8000/docs
# ReDoc:      http://localhost:8000/redoc
```

### Endpoints

| Method | Path | Description |
|---|---|---|
| `GET` | `/health` | Service health check |
| `GET` | `/api/classes` | All 10 starting classes with base stats |
| `GET` | `/api/weapons` | All weapons (optional `?category=` filter) |
| `GET` | `/api/weapons/{name}/upgrades` | All upgrade levels for a weapon |
| `GET` | `/api/bosses` | All bosses (optional `?dlc_only=true`) |
| `GET` | `/api/bosses/{name}/stats` | Community stats for one boss from pipeline |
| `POST` | `/api/ar` | Calculate AR for a weapon + build (+ soft cap advice) |
| `GET` | `/api/bosses/gauntlet` | Rank all bosses hardest→easiest for a build |
| `GET` | `/api/meta` | Current meta: top builds, hardest boss, archetype tier list |
| `GET` | `/api/builds` | List all saved builds ordered by AR |
| `GET` | `/api/builds/{name}` | Get one saved build |
| `POST` | `/api/builds` | Save or update a build |
| `DELETE` | `/api/builds/{name}` | Delete a saved build |

### Example: Calculate AR

```bash
curl -X POST http://localhost:8000/api/ar \
  -H "Content-Type: application/json" \
  -d '{"weapon_name": "Rivers of Blood", "upgrade_level": 25,
       "dexterity": 40, "arcane": 60}'
```

```json
{
  "ar": {
    "physical": 312.4,
    "fire": 250.0,
    "total": 812.4
  },
  "soft_cap_advice": [
    {"stat": "Arcane",  "current": 60, "ar_gain_plus5": 34.2},
    {"stat": "Dexterity","current": 40, "ar_gain_plus5": 18.7},
    {"stat": "Strength", "current": 10, "ar_gain_plus5": 11.1},
    {"stat": "Intelligence","current":10,"ar_gain_plus5":  0.0},
    {"stat": "Faith",    "current": 10, "ar_gain_plus5":  0.0}
  ]
}
```

### Example: Boss Gauntlet

```bash
curl "http://localhost:8000/api/bosses/gauntlet?weapon=Rivers+of+Blood&upgrade=25&dexterity=40&arcane=60"
```

---

## 16. Orchestration with Airflow

### 16.1 DAG Overview

**DAG ID**: `golden_order_pipeline`
**Schedule**: `@daily` (runs at midnight UTC)
**Max active runs**: 1 (prevents overlapping runs)

```
[start]
   ├── extract_api_weapons   ─┐
   ├── extract_api_bosses     ├──► [api_and_kaggle_done]
   └── load_kaggle_data      ─┘           │
                                   ┌──────┴──────┐
                             refresh_stg_weapons  refresh_stg_bosses
                                   └──────┬──────┘
                                   [staging_done]
                                          │
                                   build_dim_tables    ← Scaling Engine runs here
                                          │
                                   [dims_loaded]
                                          │
                                   validate_row_counts  ← Quality gate
                                          │
                                       [end]
```

### 16.2 Task Descriptions

| Task | Operator | Idempotency |
|---|---|---|
| `extract_api_weapons` | PythonOperator | Overwrites same date-partitioned JSON files |
| `extract_api_bosses` | PythonOperator | Overwrites same date-partitioned JSON files |
| `load_kaggle_data` | PythonOperator | Overwrites same Parquet files |
| `refresh_stg_weapons` | PostgresOperator | TRUNCATE + INSERT stored procedure |
| `refresh_stg_bosses` | PostgresOperator | TRUNCATE + INSERT stored procedure |
| `build_dim_tables` | PythonOperator | INSERT … ON CONFLICT DO UPDATE |
| `validate_row_counts` | PythonOperator | Asserts non-empty dims; fails DAG if empty |

### 16.3 Triggering the DAG Manually

```bash
# Via Airflow CLI (from inside the container)
docker exec -it er_airflow_web airflow dags trigger golden_order_pipeline

# Or via the Airflow UI at http://localhost:8081
# Navigate to DAGs → golden_order_pipeline → Trigger DAG
```

### 16.4 Retries and Back-off

All tasks are configured with:
- 3 retries
- 5-minute delay between retries
- Exponential back-off enabled

---

## 17. Running the Tests

```bash
# Activate venv
.venv\Scripts\activate   # Windows
source .venv/bin/activate # macOS/Linux

# Run all 50 tests
python -m pytest tests/ -v

# Run only scaling engine tests (31 tests)
python -m pytest tests/test_scaling_engine.py -v

# Run only telemetry generator tests (19 tests)
python -m pytest tests/test_telemetry_generator.py -v
```

### Test Coverage

**`tests/test_scaling_engine.py`** — 31 tests

| Class | Tests | What is verified |
|---|---|---|
| `TestSoftCapCurve` | 7 | Curve is monotone, clamped, and has correct slopes at soft caps 60 and 80 |
| `TestGradeMultipliers` | 3 | S > A > B > C > D > E ordering; `-` = 0 |
| `TestCalculateAR` | 7 | Base-only weapons, stat monotonicity, split-damage, dict keys, component sum |
| `TestTwoHanding` | 4 | Effective STR multiplier, 99 cap, AR boost for STR weapon, no effect on DEX weapon |
| `TestMarginalGain` | 3 | Marginal gain decreases past each soft cap |
| `TestSoftCapReport` | 4 | 99 rows, monotone AR, correct flag values |
| `TestSparkUDF` | 3 | UDF matches native, handles None, two-handing |

**`tests/test_telemetry_generator.py`** — 19 tests

| Class | Tests | What is verified |
|---|---|---|
| `TestEventSchema` | 8 | All required keys present, JSON serialisable, valid outcome, stats in range, UUID uniqueness |
| `TestOutcomeDistribution` | 5 | Both outcomes present, all 7 archetypes appear, two-handing occurs |
| `TestRateLimiting` | 2 | `max_events` stops generator, 100 events complete in under 5 seconds |
| `TestPools` | 4 | 400 weapons loaded, 153 bosses loaded, variety in generated events |

---

## 18. Configuration Reference

Copy `.env.example` to `.env` before running. All settings have sensible defaults.

```ini
# ── Warehouse ──────────────────────────────────────────────────────────────
WAREHOUSE_HOST=localhost
WAREHOUSE_PORT=5432
WAREHOUSE_USER=eruser
WAREHOUSE_PASSWORD=erpass
WAREHOUSE_DB=elden_ring_dw

# ── Kafka ──────────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_TOPIC_ENCOUNTERS=player_encounters
KAFKA_TOPIC_DLQ=player_encounters_dlq
KAFKA_CONSUMER_GROUP=er_spark_consumer

# ── Elden Ring Fan API ─────────────────────────────────────────────────────
ELDENRING_API_BASE_URL=https://eldenring.fanapis.com/api
ELDENRING_API_PAGE_SIZE=100
ELDENRING_API_REQUEST_DELAY_S=0.25    # polite rate-limiting between pages

# ── Data Lake ──────────────────────────────────────────────────────────────
DATA_LAKE_ROOT=./data/raw             # swap for gs://your-bucket/raw in cloud
STAGING_ROOT=./data/staged

# ── Telemetry Generator ────────────────────────────────────────────────────
TELEMETRY_EVENTS_PER_SECOND=500
TELEMETRY_NUM_PLAYERS=10000
TELEMETRY_RUN_DURATION_S=0            # 0 = run forever
```

---

## 19. SQL Schema Reference

### Schema: `golden_order`

**`dim_weapons`** — Unique constraint: `(weapon_name, upgrade_level, upgrade_type)`

Key columns: `weapon_name`, `upgrade_level`, `upgrade_type`, `category`, `scale_str/dex/int/fai/arc`, `base_physical/magic/fire/lightning/holy`, `ar_str_build_60`, `ar_dex_build_60`, `ar_quality_build_60`, `ar_int_build_60`, `ar_faith_build_60`, `ar_profiles_json`

**`dim_bosses`** — Unique constraint: `(boss_name)`

Key columns: `boss_name`, `hp`, `is_dlc`, `drops_runes`, `drops_items`

**`fact_encounters`** — Unique constraint: `(event_id)` for deduplication

Key columns: `event_id`, `event_ts`, `player_id`, `player_level`, `archetype`, `strength_stat` through `arcane_stat`, `two_handing`, `boss_name`, `boss_hp`, `weapon_used`, `upgrade_level`, `calculated_ar`, `outcome`, `is_victory` (generated column)

**Indexes on `fact_encounters`**:
- `BRIN` on `event_ts` — time-range scans for dashboards
- B-tree on `(boss_name, is_victory)` — most-lethal-bosses query
- B-tree on `(weapon_used, upgrade_level, is_victory)` — win-rate queries
- B-tree on `calculated_ar` — AR distribution analysis

---

## 20. Makefile Commands

```bash
make install        # pip install -r requirements.txt + copy .env

make test           # Run all 50 tests
make test-scaling   # Run only scaling engine tests
make test-telemetry # Run only telemetry generator tests

make diagram        # Generate architecture SVG + HTML

make ingest         # Phase 1: API extraction + Kaggle load
make transform      # Phase 2: Flatten + Scaling Engine + load warehouse
make pipeline       # Phase 1 + Phase 2 (full batch)

make produce        # Phase 3: Start Kafka producer
make consume        # Phase 3: Start Spark consumer

make analytics      # Phase 4: Print terminal analytics report
make meta-report    # Generate + persist daily meta snapshot

make calculator     # Run interactive CLI build calculator
make advisor        # Run interactive CLI build advisor
make streamlit      # Launch Streamlit web dashboard (:8501)
make api            # Launch FastAPI REST service (:8000)

make infra-up       # docker compose up -d
make infra-down     # docker compose down
make infra-logs     # docker compose logs -f

make demo           # Run Scaling Engine + telemetry demo + diagram (no Docker)
make clean          # Remove __pycache__, data/raw/*, data/staged/*
```

---

## 21. Design Decisions & Idempotency

### Why idempotency matters

The Airflow DAG runs daily. Failures will happen — network timeouts, API rate limits, pod restarts. Every task must be safe to re-run from the beginning without producing duplicates or corrupted state.

| Layer | Idempotency mechanism |
|---|---|
| API extractor | Date-partitioned file paths. Re-running on the same date overwrites the same files. |
| Kaggle loader | Parquet files written with overwrite semantics. Same input → same output always. |
| Staging SQL | `TRUNCATE + INSERT` stored procedure — always produces a fresh, deterministic staging table. |
| Dim loader | `INSERT … ON CONFLICT (natural key) DO UPDATE` — upsert semantics guarantee no duplicates even on repeated runs. |
| Fact table | `event_id` has a `UNIQUE` constraint. Kafka may redeliver messages on consumer restart; PostgreSQL silently drops duplicates. |
| Spark streaming | Checkpoints at `data/checkpoints/` record the last committed Kafka offset. Restarts resume from that offset — no reprocessing. |

### Why pre-compute AR in `dim_weapons`?

Running the Scaling Engine at BI query time (in a SQL view) would require a Python UDF call for every row the dashboard fetches. By pre-computing AR for 10 representative stat profiles during the nightly batch run and storing them as plain `NUMERIC` columns, dashboard queries become pure SQL arithmetic — no Python, no UDF latency.

### Why compute AR again in Spark?

The pre-computed columns in `dim_weapons` cover specific profiles. When a real player encounter arrives with `strength=47`, `dexterity=22`, the pre-computed `ar_str_build_60` (STR=60) is not their actual AR. The Spark consumer calls `calculate_ar()` with the player's **exact stats** so `fact_encounters.calculated_ar` is always precise. This is the number used in all win-rate analytics.

### Why Kafka partitioned by `boss_id`?

Partitioning by `boss_id` ensures all events for a given boss go to the same partition in the same order. This enables future stateful streaming aggregations (e.g., rolling death count per boss over a 5-minute window) without a cross-partition shuffle.

### Why PostgreSQL instead of BigQuery/Snowflake?

PostgreSQL is used locally to keep the project runnable without a cloud account or billing. The SQL is written to be cloud-portable:
- All `SERIAL` / `BIGSERIAL` types map to `AUTO_INCREMENT` (Snowflake) or `INT64` (BigQuery)
- `JSONB` maps to `JSON` in BigQuery or `VARIANT` in Snowflake
- The `golden_order` schema maps directly to a BigQuery dataset or Snowflake schema
- JDBC URLs in `spark_consumer.py` and `dim_builder.py` are configured through `WarehouseConfig` — swap the DSN to point at any JDBC-compatible warehouse

---

## 22. Troubleshooting

### `ImportError: DLL load failed` (numpy on Windows Python 3.13)

Python 3.13 has known DLL compatibility issues with numpy on Windows. The core pipeline (Scaling Engine, Telemetry Generator, Airflow DAG) does **not** require numpy. Use Python 3.11 or 3.12 if you need full pandas/numpy functionality for the `kaggle_loader.py` and `dim_builder.py` batch transforms.

```bash
# Check your Python version
python --version

# The demo + tests work without numpy:
python -m src.transformation.scaling_engine   # no numpy needed
python -m src.streaming.telemetry_generator   # no numpy needed
python -m pytest tests/ -v                    # no numpy needed
```

### Kafka connection refused

The Kafka broker takes ~30 seconds to start after `docker compose up`. The producer has built-in retry logic, but if you start it too early you may see `connection refused`. Wait until `docker compose ps` shows `er_kafka` as healthy.

### Airflow DAG not appearing

Ensure the `dags/` directory is mounted into the Airflow containers (it is by default in `docker-compose.yml`). Trigger a DAG scan: `airflow dags reserialize` inside the scheduler container.

### Spark JDBC write fails

The Spark consumer requires the PostgreSQL JDBC driver JAR on the Spark classpath. Add this to your Spark submit command:
```bash
--packages org.postgresql:postgresql:42.7.3
```
Or set `SPARK_CLASSPATH` to include the JAR before starting the worker.

### `stg_weapons` procedure does not exist

Run the staging transform SQL manually:
```bash
docker exec -it er_warehouse psql -U eruser -d elden_ring_dw \
  -f /opt/sql/transforms/01_staging_weapons.sql
```

---

## Project Summary

| Deliverable | Status | Location |
|---|---|---|
| API extraction with retry/pagination | Complete | `src/ingestion/api_extractor.py` |
| Kaggle CSV loader + Parquet staging | Complete | `src/ingestion/kaggle_loader.py` |
| JSON flattener for nested dict columns | Complete | `src/transformation/json_flattener.py` |
| **Soft-cap Scaling Engine (the UDF)** | **Complete** | **`src/transformation/scaling_engine.py`** |
| Star schema DDL (dim + fact) | Complete | `sql/ddl/` |
| Dimensional model builder with upserts | Complete | `src/transformation/dim_builder.py` |
| Synthetic telemetry generator | Complete | `src/streaming/telemetry_generator.py` |
| Kafka producer (LZ4, keyed, async) | Complete | `src/streaming/kafka_producer.py` |
| Spark Structured Streaming consumer | Complete | `src/streaming/spark_consumer.py` |
| Analytics views (5 views) | Complete | `sql/ddl/05_analytics_views.sql` |
| Analytics query wrappers | Complete | `src/analytics/queries.py` |
| **Daily meta report + JSONB persistence** | **Complete** | **`src/analytics/meta_report.py`** |
| Interactive CLI build calculator | Complete | `src/calculator.py` |
| Interactive CLI build advisor | Complete | `src/advisor.py` |
| **Streamlit 4-page web dashboard** | **Complete** | **`src/app.py`** |
| **FastAPI REST service (13 endpoints)** | **Complete** | **`src/api.py`** |
| Airflow DAG (8 tasks, idempotent) | Complete | `dags/elden_ring_pipeline_dag.py` |
| Docker Compose (full stack) | Complete | `docker-compose.yml` |
| Architecture diagram (SVG/HTML) | Complete | `architecture/pipeline_architecture.html` |
| Test suite (50 tests, all passing) | Complete | `tests/` |
