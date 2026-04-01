"""
Golden Order Data Pipeline — Apache Airflow DAG
================================================
Orchestrates all batch phases of the Elden Ring analytics pipeline.

DAG: golden_order_pipeline
Schedule: @daily (runs once per day, data for the previous UTC day)

Task dependency graph:

  [start]
     │
     ├── extract_api_weapons ──────┐
     ├── extract_api_bosses        │
     ├── load_kaggle_data ─────────┤
     │                             ▼
     │                      [api_and_kaggle_done]
     │                             │
     │                      refresh_stg_weapons ─┐
     │                      refresh_stg_bosses   │
     │                                           ▼
     │                                   [staging_done]
     │                                           │
     │                                    build_dim_tables   ← Phase 2 (Scaling Engine)
     │                                           │
     │                                   [dims_loaded]
     │                                           │
     │                                   validate_row_counts
     │                                           │
     │                                        [end]

Idempotency guarantees
----------------------
* API extractor writes pages to date-partitioned paths; re-running on the
  same day overwrites the same files (no duplicate raw JSON).
* Kaggle loader writes Parquet with mode="overwrite" per partition.
* dim_builder uses INSERT … ON CONFLICT DO UPDATE — re-running never
  duplicates rows in the warehouse.
* The Spark streaming job uses event_id as a unique key with
  ON CONFLICT DO NOTHING to deduplicate re-delivered Kafka messages.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.trigger_rule import TriggerRule

# ── Default args ──────────────────────────────────────────────────────────────

DEFAULT_ARGS = {
    "owner":            "golden_order",
    "depends_on_past":  False,
    "email_on_failure": False,
    "email_on_retry":   False,
    "retries":          3,
    "retry_delay":      timedelta(minutes=5),
    "retry_exponential_backoff": True,
}

# ── Python callables ──────────────────────────────────────────────────────────

def _extract_api_weapons(**kwargs):
    """Pull weapon data from Elden Ring Fan API into the data lake."""
    from src.ingestion.api_extractor import EldenRingAPIExtractor
    extractor = EldenRingAPIExtractor()
    total = extractor.extract_endpoint("weapons")
    kwargs["ti"].xcom_push(key="weapons_extracted", value=total)


def _extract_api_bosses(**kwargs):
    """Pull boss data from Elden Ring Fan API into the data lake."""
    from src.ingestion.api_extractor import EldenRingAPIExtractor
    extractor = EldenRingAPIExtractor()
    total = extractor.extract_endpoint("bosses")
    kwargs["ti"].xcom_push(key="bosses_extracted", value=total)


def _load_kaggle_data(**kwargs):
    """
    Load Kaggle CSVs and stage as Parquet.
    This is idempotent: always overwrites the same Parquet files.
    """
    from src.ingestion.kaggle_loader import run_kaggle_load
    summary = run_kaggle_load()
    kwargs["ti"].xcom_push(key="kaggle_summary", value=summary)


def _build_dim_tables(**kwargs):
    """
    Phase 2: Flatten JSON, apply Scaling Engine UDF, load dim_weapons and
    dim_bosses into the warehouse with ON CONFLICT DO UPDATE idempotency.
    """
    from src.transformation.dim_builder import run_dim_build
    run_dim_build()


def _validate_row_counts(**kwargs):
    """
    Quality gate: assert that dim_weapons and dim_bosses have non-zero rows.
    Raises ValueError if either table is empty — halts the DAG.
    """
    from sqlalchemy import create_engine, text
    from src.config import WarehouseConfig

    engine = create_engine(WarehouseConfig.dsn())
    with engine.connect() as conn:
        w_count = conn.execute(
            text("SELECT COUNT(*) FROM golden_order.dim_weapons")
        ).scalar()
        b_count = conn.execute(
            text("SELECT COUNT(*) FROM golden_order.dim_bosses")
        ).scalar()

    if w_count == 0:
        raise ValueError("dim_weapons is empty — pipeline validation FAILED")
    if b_count == 0:
        raise ValueError("dim_bosses is empty — pipeline validation FAILED")

    from loguru import logger
    logger.info(f"Validation passed: dim_weapons={w_count:,} rows, dim_bosses={b_count} rows")
    kwargs["ti"].xcom_push(key="validation", value={"weapons": w_count, "bosses": b_count})


# ── DAG definition ────────────────────────────────────────────────────────────

with DAG(
    dag_id="golden_order_pipeline",
    description="Elden Ring Combat & Build Analytics — daily batch pipeline",
    default_args=DEFAULT_ARGS,
    schedule_interval="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,       # don't backfill historical runs
    max_active_runs=1,   # prevent concurrent runs stomping each other
    tags=["elden_ring", "batch", "dimensional_modeling"],
) as dag:

    dag.doc_md = __doc__

    start = EmptyOperator(task_id="start")
    end   = EmptyOperator(task_id="end", trigger_rule=TriggerRule.ALL_SUCCESS)

    # ── Phase 1: Extraction ───────────────────────────────────────────────────

    extract_weapons = PythonOperator(
        task_id="extract_api_weapons",
        python_callable=_extract_api_weapons,
        doc_md="Pull all weapon pages from Elden Ring Fan API → data lake.",
    )

    extract_bosses = PythonOperator(
        task_id="extract_api_bosses",
        python_callable=_extract_api_bosses,
        doc_md="Pull all boss pages from Elden Ring Fan API → data lake.",
    )

    load_kaggle = PythonOperator(
        task_id="load_kaggle_data",
        python_callable=_load_kaggle_data,
        doc_md="Stage Kaggle CSVs as Parquet in data/staged/.",
    )

    api_and_kaggle_done = EmptyOperator(
        task_id="api_and_kaggle_done",
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    # ── Phase 2a: Staging transforms (SQL) ───────────────────────────────────

    refresh_stg_weapons = PostgresOperator(
        task_id="refresh_stg_weapons",
        postgres_conn_id="elden_ring_warehouse",
        sql="CALL golden_order.refresh_stg_weapons();",
        doc_md="Flatten raw API weapon JSON into stg_weapons (idempotent TRUNCATE+INSERT).",
    )

    refresh_stg_bosses = PostgresOperator(
        task_id="refresh_stg_bosses",
        postgres_conn_id="elden_ring_warehouse",
        sql="CALL golden_order.refresh_stg_bosses();",
        doc_md="Flatten raw API boss JSON into stg_bosses.",
    )

    staging_done = EmptyOperator(
        task_id="staging_done",
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    # ── Phase 2b: Dim build (Scaling Engine) ─────────────────────────────────

    build_dims = PythonOperator(
        task_id="build_dim_tables",
        python_callable=_build_dim_tables,
        execution_timeout=timedelta(minutes=30),
        doc_md=(
            "Apply Scaling Engine to compute Attack Rating across 10 stat profiles "
            "for every weapon × upgrade combination. Upsert into dim_weapons and "
            "dim_bosses with ON CONFLICT DO UPDATE idempotency."
        ),
    )

    dims_loaded = EmptyOperator(
        task_id="dims_loaded",
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    # ── Quality gate ──────────────────────────────────────────────────────────

    validate = PythonOperator(
        task_id="validate_row_counts",
        python_callable=_validate_row_counts,
        doc_md="Assert dim_weapons and dim_bosses are non-empty. Fail DAG if not.",
    )

    # ── Task dependencies ─────────────────────────────────────────────────────

    start >> [extract_weapons, extract_bosses, load_kaggle] >> api_and_kaggle_done
    api_and_kaggle_done >> [refresh_stg_weapons, refresh_stg_bosses] >> staging_done
    staging_done >> build_dims >> dims_loaded >> validate >> end
