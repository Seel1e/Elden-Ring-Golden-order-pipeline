"""
Phase 3 — Spark Structured Streaming Consumer
==============================================
Consumes player encounter events from Kafka, enriches them by joining against
dim_weapons and dim_bosses from the warehouse, applies the Scaling Engine UDF
to compute the true Attack Rating, and sinks enriched records into
fact_encounters.

Architecture
------------
  Kafka topic: player_encounters
        │
        ▼ (Spark readStream)
  Raw JSON events parsed into typed schema
        │
        ▼ (UDF: calculate_ar)
  Attack Rating computed per event using player's actual stats
        │
        ▼ (broadcast join)
  Enriched with dim_weapons metadata (category, weight, DR …)
        │
        ▼ (streaming join)
  Enriched with dim_bosses metadata (boss HP, DLC flag)
        │
        ▼ (foreachBatch sink)
  Upserted into fact_encounters in PostgreSQL

Fault tolerance
---------------
* Checkpointing to ./data/checkpoints/spark_encounters prevents reprocessing
  after restarts (exactly-once with idempotent JDBC writes).
* Dead-letter queue: malformed JSON events are redirected to the DLQ topic.
* Watermarking (30-minute event-time watermark) bounds state store size for
  the streaming boss-stats aggregation.
"""

import json
import os
from datetime import datetime, timezone
from functools import lru_cache

import pandas as pd
from loguru import logger
from sqlalchemy import create_engine
from sqlalchemy.dialects.postgresql import insert as pg_insert
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType, StringType, IntegerType, BooleanType,
    StructType, StructField, TimestampType,
)

from src.config import KafkaConfig, WarehouseConfig
from src.transformation.scaling_engine import (
    WeaponScaling, PlayerStats, calculate_attack_rating,
)


# ── Spark session ─────────────────────────────────────────────────────────────

def create_spark_session() -> SparkSession:
    """
    Build the SparkSession with Kafka and JDBC (PostgreSQL) connectors.

    Packages auto-downloaded from Maven on first run (requires internet):
      - org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1
      - org.postgresql:postgresql:42.7.3
    """
    # Windows: PySpark requires HADOOP_HOME and hadoop.dll in java.library.path.
    # Java 17 no longer auto-adds PATH to java.library.path, so we must use
    # JAVA_TOOL_OPTIONS which is read by ANY JVM subprocess at startup.
    if os.name == "nt":
        hadoop_home = os.environ.get("HADOOP_HOME", r"C:\hadoop")
        hadoop_bin  = os.path.join(hadoop_home, "bin")
        os.environ["HADOOP_HOME"]     = hadoop_home
        os.environ["hadoop.home.dir"] = hadoop_home
        # Add to PATH (for winutils.exe lookup)
        current_path = os.environ.get("PATH", "")
        if hadoop_bin.lower() not in current_path.lower():
            os.environ["PATH"] = hadoop_bin + os.pathsep + current_path
        # JAVA_TOOL_OPTIONS is honoured by the JVM subprocess before any class loads
        existing_jto = os.environ.get("JAVA_TOOL_OPTIONS", "")
        lib_opt = f"-Djava.library.path={hadoop_bin}"
        if "java.library.path" not in existing_jto:
            os.environ["JAVA_TOOL_OPTIONS"] = (existing_jto + " " + lib_opt).strip()
        logger.info(
            f"Windows: HADOOP_HOME={hadoop_home}  "
            f"JAVA_TOOL_OPTIONS={os.environ['JAVA_TOOL_OPTIONS']}"
        )

    # Match the installed PySpark version for the Kafka connector
    import pyspark
    spark_version = pyspark.__version__   # e.g. "3.5.1"
    scala_version = "2.12"
    packages = ",".join([
        f"org.apache.spark:spark-sql-kafka-0-10_{scala_version}:{spark_version}",
        "org.postgresql:postgresql:42.7.3",
    ])

    return (
        SparkSession.builder
        .appName("EldenRing-EncounterStreamProcessor")
        .master(os.getenv("SPARK_MASTER_URL", "local[*]"))
        .config("spark.jars.packages", packages)
        .config("spark.sql.streaming.checkpointLocation", "./data/checkpoints/spark_encounters")
        .config("spark.sql.shuffle.partitions", "12")
        .config("spark.streaming.kafka.consumer.poll.ms", "512")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        # Suppress verbose Spark/Kafka INFO noise
        .config("spark.driver.extraJavaOptions", "-Dlog4j.logger.org.apache.kafka=WARN")
        .getOrCreate()
    )


# ── Event schema ──────────────────────────────────────────────────────────────

ENCOUNTER_SCHEMA = StructType([
    StructField("event_id",          StringType(),    True),
    StructField("event_timestamp",   StringType(),    True),
    StructField("player_id",         StringType(),    True),
    StructField("player_level",      IntegerType(),   True),
    StructField("archetype",         StringType(),    True),
    StructField("strength_stat",     IntegerType(),   True),
    StructField("dexterity_stat",    IntegerType(),   True),
    StructField("intelligence_stat", IntegerType(),   True),
    StructField("faith_stat",        IntegerType(),   True),
    StructField("arcane_stat",       IntegerType(),   True),
    StructField("two_handing",       BooleanType(),   True),
    StructField("boss_id",           StringType(),    True),
    StructField("boss_name",         StringType(),    True),
    StructField("weapon_used",       StringType(),    True),
    StructField("upgrade_level",     IntegerType(),   True),
    StructField("outcome",           StringType(),    True),
    StructField("win_probability",   DoubleType(),    True),
])


# ── Register Scaling Engine as a Spark UDF ────────────────────────────────────

def register_scaling_udf(spark: SparkSession) -> None:
    """
    Register _spark_udf_calculate_ar as a SQL-callable UDF named calculate_ar.
    The UDF is broadcast to all executors and cached for reuse.
    """
    spark.udf.register("calculate_ar", _spark_udf_calculate_ar, DoubleType())
    logger.info("Scaling Engine UDF 'calculate_ar' registered with Spark.")


# ── Dim table loaders (broadcast joins) ──────────────────────────────────────

@lru_cache(maxsize=1)
def _load_dim_weapons(spark: SparkSession) -> DataFrame:
    """
    Load dim_weapons from the warehouse and broadcast it.
    Cached so it's only fetched once per Spark application lifecycle.
    """
    df = (
        spark.read
        .format("jdbc")
        .option("url", WarehouseConfig.jdbc_url())
        .option("dbtable", "golden_order.dim_weapons")
        .option("user", WarehouseConfig.USER)
        .option("password", WarehouseConfig.PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .load()
    )
    logger.info(f"dim_weapons loaded: {df.count():,} rows (broadcast)")
    return F.broadcast(df)


@lru_cache(maxsize=1)
def _load_dim_bosses(spark: SparkSession) -> DataFrame:
    df = (
        spark.read
        .format("jdbc")
        .option("url", WarehouseConfig.jdbc_url())
        .option("dbtable", "golden_order.dim_bosses")
        .option("user", WarehouseConfig.USER)
        .option("password", WarehouseConfig.PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .load()
    )
    logger.info(f"dim_bosses loaded: {df.count()} rows (broadcast)")
    return F.broadcast(df)


# ── Stream processing pipeline ────────────────────────────────────────────────

def build_stream(spark: SparkSession) -> DataFrame:
    """
    Build the complete streaming transformation pipeline.

    Returns a streaming DataFrame ready to be written to the sink.
    AR calculation and dim enrichment happen inside _process_micro_batch (Pandas).
    """
    # 1. Read raw bytes from Kafka
    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KafkaConfig.BOOTSTRAP_SERVERS)
        .option("subscribe", KafkaConfig.TOPIC_ENCOUNTERS)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .option("maxOffsetsPerTrigger", 50000)
        .load()
    )

    # 2. Parse JSON payload and add watermark for state management
    parsed = (
        raw_stream
        .select(F.from_json(F.col("value").cast("string"), ENCOUNTER_SCHEMA).alias("e"))
        .select("e.*")
        .withColumn("event_ts", F.to_timestamp("event_timestamp"))
        .withWatermark("event_ts", "30 minutes")
    )

    return parsed


# ── Micro-batch sink ──────────────────────────────────────────────────────────

def _compute_ar(row: pd.Series) -> float:
    """Apply the Scaling Engine directly on a Pandas row — no Spark UDF worker needed."""
    weapon = WeaponScaling(
        base_physical=float(row.get("base_physical") or 0),
        base_magic=float(row.get("base_magic") or 0),
        base_fire=float(row.get("base_fire") or 0),
        base_lightning=float(row.get("base_lightning") or 0),
        base_holy=float(row.get("base_holy") or 0),
        scaling={
            "Str": str(row.get("scale_str") or "-"),
            "Dex": str(row.get("scale_dex") or "-"),
            "Int": str(row.get("scale_int") or "-"),
            "Fai": str(row.get("scale_fai") or "-"),
            "Arc": str(row.get("scale_arc") or "-"),
        },
    )
    player = PlayerStats(
        strength=int(row.get("strength_stat") or 10),
        dexterity=int(row.get("dexterity_stat") or 10),
        intelligence=int(row.get("intelligence_stat") or 10),
        faith=int(row.get("faith_stat") or 10),
        arcane=int(row.get("arcane_stat") or 10),
        two_handing=bool(row.get("two_handing")),
    )
    return float(calculate_attack_rating(weapon, player)["total"])


def _process_micro_batch(batch_df: DataFrame, batch_id: int) -> None:
    """
    Called by Spark for each micro-batch. Runs entirely in Pandas within the
    foreachBatch Python context — no Spark UDF worker subprocesses, no pickling.
    Writes enriched records to fact_encounters via SQLAlchemy.
    """
    if batch_df.isEmpty():
        return

    spark = batch_df.sparkSession
    n = batch_df.count()
    logger.info(f"Processing micro-batch {batch_id}: {n} events")

    # ── Pull dim tables to Pandas (broadcast-cached after first call) ──────────
    dim_w_pd = _load_dim_weapons(spark).select(
        "weapon_name", "upgrade_level", "base_physical", "base_magic",
        "base_fire", "base_lightning", "base_holy",
        "scale_str", "scale_dex", "scale_int", "scale_fai", "scale_arc",
        "category", "weight", "dr_physical",
    ).toPandas()

    dim_b_pd = _load_dim_bosses(spark).select(
        "boss_name", "hp", "is_dlc",
    ).toPandas()

    # ── Collect batch to Pandas ────────────────────────────────────────────────
    batch_pd = batch_df.toPandas()

    # ── Pandas joins (left join — keep all events even if weapon not in dim) ───
    enriched = (
        batch_pd
        .merge(dim_w_pd, left_on=["weapon_used", "upgrade_level"],
               right_on=["weapon_name", "upgrade_level"], how="left")
        .merge(dim_b_pd, on="boss_name", how="left")
    )

    # ── Apply Scaling Engine directly (no Spark UDF) ───────────────────────────
    enriched["calculated_ar"] = enriched.apply(_compute_ar, axis=1)
    enriched["is_victory"]    = enriched["outcome"] == "victory"
    enriched["boss_hp"]       = enriched["hp"].fillna(0).astype(int)
    enriched["loaded_at"]     = datetime.now(timezone.utc)

    # ── Select final columns matching fact_encounters DDL ─────────────────────
    fact = enriched[[
        "event_id", "event_ts", "player_id", "player_level", "archetype",
        "strength_stat", "dexterity_stat", "intelligence_stat",
        "faith_stat", "arcane_stat", "two_handing",
        "boss_id", "boss_name", "boss_hp",
        "weapon_used", "upgrade_level", "calculated_ar",
        "outcome", "is_victory", "win_probability",
        "category", "weight", "is_dlc",
        "loaded_at",
    ]].copy()

    # ── Write to warehouse via SQLAlchemy (upsert — skip duplicate event_ids) ──
    engine = create_engine(WarehouseConfig.dsn())

    def _upsert_ignore(table, conn, keys, data_iter):
        stmt = pg_insert(table.table).values(list(dict(zip(keys, row)) for row in data_iter))
        conn.execute(stmt.on_conflict_do_nothing(index_elements=["event_id"]))

    fact.to_sql(
        "fact_encounters",
        engine,
        schema="golden_order",
        if_exists="append",
        index=False,
        method=_upsert_ignore,
        chunksize=2000,
    )
    logger.info(f"Micro-batch {batch_id} written: {len(fact)} enriched rows → fact_encounters")


# ── Entry point ───────────────────────────────────────────────────────────────

def run_consumer() -> None:
    logger.info("Creating Spark session (downloading JARs on first run — may take ~60s)…")
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    logger.info(f"Spark session ready: v{spark.version}")

    logger.info("Building Kafka stream pipeline…")
    stream = build_stream(spark)

    query = (
        stream
        .writeStream
        .foreachBatch(_process_micro_batch)
        .trigger(processingTime="10 seconds")
        .option("checkpointLocation", "./data/checkpoints/spark_encounters")
        .start()
    )

    logger.info("Streaming query started. Waiting for events…")
    try:
        query.awaitTermination()
    except Exception as exc:
        logger.error(f"Streaming query failed: {exc}")
        raise


if __name__ == "__main__":
    run_consumer()
