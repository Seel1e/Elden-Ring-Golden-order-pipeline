"""
Phase 2 — Dimensional Model Builder
====================================
Takes the flattened staging DataFrames and:
  1. Applies the Scaling Engine to compute Attack Rating at every upgrade level
     for a representative set of player stat profiles.
  2. Builds dim_weapons and dim_bosses DataFrames.
  3. Ensures the golden_order schema and tables exist (self-initialising).
  4. Loads them into PostgreSQL using a single-connection UPSERT for idempotency.
"""

import json
import re
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from loguru import logger
from sqlalchemy import create_engine, text

from src.config import WarehouseConfig
from src.transformation.json_flattener import load_and_flatten_weapons, load_and_flatten_bosses
from src.transformation.scaling_engine import (
    WeaponScaling, PlayerStats, calculate_attack_rating
)


# ── Safe conversion helpers ───────────────────────────────────────────────────

def _safe_float(val) -> float:
    """Convert to float, handling NaN, None, ranges like '10 - 25', dashes."""
    if val is None:
        return 0.0
    s = str(val).strip()
    if not s or s in ("-", "nan", "None", ""):
        return 0.0
    try:
        return float(s)
    except ValueError:
        m = re.search(r"[\d]+(?:\.\d+)?", s)
        return float(m.group()) if m else 0.0


def _safe_bool(val) -> bool:
    """Convert to bool, handling NaN and None."""
    try:
        return bool(int(_safe_float(val)))
    except (ValueError, TypeError):
        return False


def _safe_int(val) -> int:
    return int(_safe_float(val))


# ── Representative stat profiles ──────────────────────────────────────────────

REPRESENTATIVE_PROFILES = [
    # (name, STR, DEX, INT, FAI, ARC, two_handing)
    ("str_build_60",      60, 15, 9,  9,  9,  False),
    ("str_build_80",      80, 15, 9,  9,  9,  False),
    ("dex_build_60",      13, 60, 9,  9,  9,  False),
    ("dex_build_80",      13, 80, 9,  9,  9,  False),
    ("quality_build_60",  60, 60, 9,  9,  9,  False),
    ("int_build_60",       9, 12, 60, 9,  9,  False),
    ("faith_build_60",     9, 12, 9,  60, 9,  False),
    ("arcane_build_60",    9, 12, 9,  9,  60, False),
    ("two_hand_str_40",   40, 12, 9,  9,  9,  True),
    ("beginner_30_all",   30, 30, 9,  9,  9,  False),
]


# ── Dim builders ──────────────────────────────────────────────────────────────

def build_dim_weapons(weapons_df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Building dim_weapons…")
    rows = []
    for _, row in weapons_df.iterrows():
        weapon = WeaponScaling(
            base_physical=_safe_float(row.get("atk_physical")),
            base_magic=_safe_float(row.get("atk_magic")),
            base_fire=_safe_float(row.get("atk_fire")),
            base_lightning=_safe_float(row.get("atk_lightning")),
            base_holy=_safe_float(row.get("atk_holy")),
            scaling={
                "Str": str(row.get("scale_str", "-")),
                "Dex": str(row.get("scale_dex", "-")),
                "Int": str(row.get("scale_int", "-")),
                "Fai": str(row.get("scale_fai", "-")),
                "Arc": str(row.get("scale_arc", "-")),
            },
        )
        ar_by_profile: dict[str, float] = {}
        for (name, s, d, i, f, a, th) in REPRESENTATIVE_PROFILES:
            player = PlayerStats(strength=s, dexterity=d, intelligence=i,
                                 faith=f, arcane=a, two_handing=th)
            ar_by_profile[name] = calculate_attack_rating(weapon, player)["total"]

        rows.append({
            "weapon_name":         str(row.get("weapon name", "")),
            "upgrade_level":       _safe_int(row.get("upgrade_level", 0)),
            "upgrade_type":        str(row.get("upgrade_type", "Standard")),
            "category":            str(row.get("category", "")),
            "damage_type":         str(row.get("damage type", "")),
            "weight":              _safe_float(row.get("weight")),
            "skill":               str(row.get("skill", "")),
            "fp_cost":             _safe_float(row.get("FP cost")),
            "is_dlc":              _safe_bool(row.get("dlc", 0)),
            "req_str":             _safe_int(row.get("req_str", 0)),
            "req_dex":             _safe_int(row.get("req_dex", 0)),
            "req_int":             _safe_int(row.get("req_int", 0)),
            "req_fai":             _safe_int(row.get("req_fai", 0)),
            "req_arc":             _safe_int(row.get("req_arc", 0)),
            "base_physical":       _safe_float(row.get("atk_physical")),
            "base_magic":          _safe_float(row.get("atk_magic")),
            "base_fire":           _safe_float(row.get("atk_fire")),
            "base_lightning":      _safe_float(row.get("atk_lightning")),
            "base_holy":           _safe_float(row.get("atk_holy")),
            "scale_str":           str(row.get("scale_str", "-")),
            "scale_dex":           str(row.get("scale_dex", "-")),
            "scale_int":           str(row.get("scale_int", "-")),
            "scale_fai":           str(row.get("scale_fai", "-")),
            "scale_arc":           str(row.get("scale_arc", "-")),
            "dr_physical":         _safe_float(row.get("dr_physical")),
            "ar_str_build_60":     ar_by_profile.get("str_build_60", 0.0),
            "ar_dex_build_60":     ar_by_profile.get("dex_build_60", 0.0),
            "ar_quality_build_60": ar_by_profile.get("quality_build_60", 0.0),
            "ar_int_build_60":     ar_by_profile.get("int_build_60", 0.0),
            "ar_faith_build_60":   ar_by_profile.get("faith_build_60", 0.0),
            "ar_profiles_json":    json.dumps(ar_by_profile),
            "loaded_at":           datetime.now(timezone.utc),
        })

    dim = pd.DataFrame(rows)
    logger.info(f"dim_weapons: {len(dim):,} rows across {dim['weapon_name'].nunique()} weapons")
    return dim


def build_dim_bosses(bosses_df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Building dim_bosses…")
    dim = pd.DataFrame({
        "boss_name":   bosses_df["name"],
        "hp":          bosses_df["hp_parsed"],
        "is_dlc":      bosses_df["dlc"].apply(_safe_bool),
        "drops_runes": bosses_df.get("drops_runes", pd.Series(0, index=bosses_df.index)),
        "drops_items": bosses_df.get("drops_items", pd.Series("", index=bosses_df.index)),
        "loaded_at":   datetime.now(timezone.utc),
    })
    logger.info(f"dim_bosses: {len(dim)} rows")
    return dim


# ── Schema bootstrap ──────────────────────────────────────────────────────────

_SCHEMA_DDL = """
CREATE SCHEMA IF NOT EXISTS golden_order;
"""

_DIM_WEAPONS_DDL = """
CREATE TABLE IF NOT EXISTS golden_order.dim_weapons (
    weapon_key          BIGSERIAL PRIMARY KEY,
    weapon_name         TEXT        NOT NULL,
    upgrade_level       SMALLINT    NOT NULL DEFAULT 0,
    upgrade_type        TEXT        NOT NULL DEFAULT 'Standard',
    category            TEXT,
    damage_type         TEXT,
    weight              NUMERIC(6,2),
    skill               TEXT,
    fp_cost             NUMERIC(8,2),
    is_dlc              BOOLEAN     NOT NULL DEFAULT FALSE,
    req_str             SMALLINT    NOT NULL DEFAULT 0,
    req_dex             SMALLINT    NOT NULL DEFAULT 0,
    req_int             SMALLINT    NOT NULL DEFAULT 0,
    req_fai             SMALLINT    NOT NULL DEFAULT 0,
    req_arc             SMALLINT    NOT NULL DEFAULT 0,
    base_physical       NUMERIC(8,2) NOT NULL DEFAULT 0,
    base_magic          NUMERIC(8,2) NOT NULL DEFAULT 0,
    base_fire           NUMERIC(8,2) NOT NULL DEFAULT 0,
    base_lightning      NUMERIC(8,2) NOT NULL DEFAULT 0,
    base_holy           NUMERIC(8,2) NOT NULL DEFAULT 0,
    scale_str           TEXT        NOT NULL DEFAULT '-',
    scale_dex           TEXT        NOT NULL DEFAULT '-',
    scale_int           TEXT        NOT NULL DEFAULT '-',
    scale_fai           TEXT        NOT NULL DEFAULT '-',
    scale_arc           TEXT        NOT NULL DEFAULT '-',
    dr_physical         NUMERIC(6,2),
    ar_str_build_60     NUMERIC(8,2),
    ar_dex_build_60     NUMERIC(8,2),
    ar_quality_build_60 NUMERIC(8,2),
    ar_int_build_60     NUMERIC(8,2),
    ar_faith_build_60   NUMERIC(8,2),
    ar_profiles_json    TEXT,
    loaded_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_dim_weapons_natural UNIQUE (weapon_name, upgrade_level, upgrade_type)
)
"""

_DIM_BOSSES_DDL = """
CREATE TABLE IF NOT EXISTS golden_order.dim_bosses (
    boss_key    BIGSERIAL PRIMARY KEY,
    boss_name   TEXT        NOT NULL,
    hp          INTEGER     NOT NULL DEFAULT 0,
    is_dlc      BOOLEAN     NOT NULL DEFAULT FALSE,
    drops_runes BIGINT      NOT NULL DEFAULT 0,
    drops_items TEXT,
    loaded_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_dim_bosses_name UNIQUE (boss_name)
)
"""

_FACT_ENCOUNTERS_DDL = """
CREATE TABLE IF NOT EXISTS golden_order.fact_encounters (
    encounter_id      BIGSERIAL   PRIMARY KEY,
    event_id          TEXT        NOT NULL,
    event_ts          TIMESTAMPTZ,
    loaded_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    player_id         TEXT,
    player_level      SMALLINT,
    archetype         TEXT,
    strength_stat     SMALLINT,
    dexterity_stat    SMALLINT,
    intelligence_stat SMALLINT,
    faith_stat        SMALLINT,
    arcane_stat       SMALLINT,
    two_handing       BOOLEAN     NOT NULL DEFAULT FALSE,
    boss_id           TEXT,
    boss_name         TEXT,
    boss_hp           INTEGER,
    weapon_used       TEXT,
    upgrade_level     SMALLINT,
    category          TEXT,
    weight            NUMERIC(6,2),
    is_dlc            BOOLEAN,
    calculated_ar     NUMERIC(8,2),
    outcome           TEXT,
    is_victory        BOOLEAN,
    win_probability   NUMERIC(6,4),
    CONSTRAINT uq_fact_event_id UNIQUE (event_id)
)
"""


def _ensure_schema(engine) -> None:
    """Create the golden_order schema and all required tables. Idempotent."""
    with engine.begin() as conn:
        conn.execute(text(_SCHEMA_DDL))
        conn.execute(text(_DIM_WEAPONS_DDL))
        conn.execute(text(_DIM_BOSSES_DDL))
        conn.execute(text(_FACT_ENCOUNTERS_DDL))
    logger.info("Warehouse schema verified / initialised.")


# ── Warehouse loader (idempotent UPSERT) ─────────────────────────────────────

def _upsert(df: pd.DataFrame, schema: str, table: str,
            engine, conflict_cols: list[str]) -> None:
    """
    Load a DataFrame into PostgreSQL using a temp table + INSERT ON CONFLICT.

    Everything runs inside a single connection so the temp table is always
    visible when the INSERT executes. Idempotent: safe to re-run.

    Parameters
    ----------
    df            : data to load
    schema        : e.g. "golden_order"
    table         : unqualified table name, e.g. "dim_weapons"
    conflict_cols : columns that form the natural key
    """
    if df.empty:
        logger.warning(f"Empty DataFrame — skipping {schema}.{table}")
        return

    # Deduplicate on the natural key before loading — keeps last occurrence
    before = len(df)
    df = df.drop_duplicates(subset=conflict_cols, keep="last")
    if len(df) < before:
        logger.warning(f"Dropped {before - len(df):,} duplicate rows on {conflict_cols}")

    temp = f"_tmp_{table}"
    cols = list(df.columns)
    col_list    = ", ".join(f'"{c}"' for c in cols)
    conflict_str = ", ".join(f'"{c}"' for c in conflict_cols)
    update_str   = ", ".join(
        f'"{c}" = EXCLUDED."{c}"' for c in cols if c not in conflict_cols
    )

    # Build row tuples for bulk insert
    records = df.to_dict("records")

    with engine.begin() as conn:
        # 1. Create a temp table that mirrors the target (dropped at end of session)
        conn.execute(text(
            f'CREATE TEMP TABLE "{temp}" AS '
            f'SELECT * FROM {schema}."{table}" WHERE FALSE'
        ))

        # 2. Bulk-insert all rows into the temp table using executemany
        if records:
            placeholders = ", ".join(f":{c}" for c in cols)
            conn.execute(
                text(f'INSERT INTO "{temp}" ({col_list}) VALUES ({placeholders})'),
                records,
            )

        # 3. Upsert from temp → target
        conn.execute(text(f"""
            INSERT INTO {schema}."{table}" ({col_list})
            SELECT {col_list} FROM "{temp}"
            ON CONFLICT ({conflict_str}) DO UPDATE SET {update_str}
        """))

        # 4. Clean up temp table
        conn.execute(text(f'DROP TABLE IF EXISTS "{temp}"'))

    logger.info(f"Upserted {len(df):,} rows into {schema}.{table}")


# ── Main pipeline function ────────────────────────────────────────────────────

def run_dim_build() -> None:
    engine = create_engine(WarehouseConfig.dsn(), echo=False)

    # Step 0: Ensure schema + tables exist (self-initialising)
    _ensure_schema(engine)

    # Step 1-2: Load & flatten staging data
    weapons_flat = load_and_flatten_weapons()
    bosses_flat  = load_and_flatten_bosses()

    # Step 3: Build dims (Scaling Engine runs here)
    dim_weapons = build_dim_weapons(weapons_flat)
    dim_bosses  = build_dim_bosses(bosses_flat)

    # Step 4: Upsert into warehouse
    _upsert(dim_weapons, "golden_order", "dim_weapons", engine,
            conflict_cols=["weapon_name", "upgrade_level", "upgrade_type"])
    _upsert(dim_bosses, "golden_order", "dim_bosses", engine,
            conflict_cols=["boss_name"])

    logger.info("Phase 2 complete — dimensional model loaded into warehouse.")


if __name__ == "__main__":
    run_dim_build()
