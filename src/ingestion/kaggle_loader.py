"""
Phase 1 — Kaggle Dataset Loader
================================
Reads the local Kaggle CSV files from eldenringScrap/ and normalises them
into a consistent JSON structure that matches the API extractor output.

This allows Phase 2 transformation logic to operate on a single unified
schema regardless of whether data arrived via API or Kaggle backfill.

Idempotency: output Parquet files are written to data/staged/ with a
date-partitioned path. Re-running overwrites the same partition.
"""

import ast
import json
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from loguru import logger

from src.config import DataLakeConfig


# ── Column parsers ───────────────────────────────────────────────────────────

def _safe_parse_dict(value: str | float) -> dict:
    """Convert a Python-dict-as-string to a real dict. Returns {} on failure."""
    if pd.isna(value) or value in ("", "nan"):
        return {}
    try:
        result = ast.literal_eval(str(value))
        return result if isinstance(result, dict) else {}
    except (ValueError, SyntaxError):
        return {}


def _parse_hp(value: str | float) -> int:
    """Boss HP can be '22,571' or '7,560 (phase 1) ...' — extract first number."""
    if pd.isna(value):
        return 0
    cleaned = str(value).split("(")[0].replace(",", "").strip()
    try:
        return int(cleaned)
    except ValueError:
        return 0


# ── Weapons ──────────────────────────────────────────────────────────────────

def load_weapons() -> pd.DataFrame:
    """
    Load weapons.csv + weapons_upgrades.csv and produce a denormalised
    DataFrame with parsed scaling and attack dicts.

    Returns one row per (weapon_name, upgrade_level).
    """
    weapons_path = DataLakeConfig.KAGGLE_DIR / "weapons.csv"
    upgrades_path = DataLakeConfig.KAGGLE_DIR / "weapons_upgrades.csv"

    logger.info(f"Loading weapons from {weapons_path}")
    weapons_df = pd.read_csv(weapons_path)
    upgrades_df = pd.read_csv(upgrades_path)

    # ── Parse dict-as-string columns ──────────────────────────────────────
    weapons_df["requirements_parsed"] = weapons_df["requirements"].apply(_safe_parse_dict)

    upgrades_df["attack_power_parsed"] = upgrades_df["attack power"].apply(_safe_parse_dict)
    upgrades_df["stat_scaling_parsed"] = upgrades_df["stat scaling"].apply(_safe_parse_dict)
    upgrades_df["passive_effects_parsed"] = upgrades_df["passive effects"].apply(_safe_parse_dict)
    upgrades_df["damage_reduction_parsed"] = upgrades_df["damage reduction (%)"].apply(_safe_parse_dict)

    # ── Extract upgrade level from strings like "Standard +3" ────────────
    upgrades_df["upgrade_level"] = (
        upgrades_df["upgrade"]
        .str.extract(r"\+(\d+)")
        .fillna(0)
        .astype(int)
    )
    upgrades_df["upgrade_type"] = (
        upgrades_df["upgrade"]
        .str.extract(r"^([A-Za-z\s]+)")
        .iloc[:, 0]
        .str.strip()
    )

    # ── Join on weapon name ───────────────────────────────────────────────
    merged = pd.merge(
        upgrades_df,
        weapons_df[["name", "weight", "category", "requirements_parsed",
                    "damage type", "skill", "FP cost", "dlc"]],
        left_on="weapon name",
        right_on="name",
        how="left",
    )

    logger.info(f"Weapons loaded: {len(merged):,} rows ({len(merged['weapon name'].unique())} unique weapons)")
    return merged


def load_bosses() -> pd.DataFrame:
    """Load bosses.csv and parse nested HP / drops columns."""
    path = DataLakeConfig.KAGGLE_DIR / "bosses.csv"
    logger.info(f"Loading bosses from {path}")
    df = pd.read_csv(path)

    df["hp_parsed"] = df["HP"].apply(_parse_hp)
    df["drops_parsed"] = df["Locations & Drops"].apply(_safe_parse_dict)

    logger.info(f"Bosses loaded: {len(df)} rows")
    return df


def load_shields() -> pd.DataFrame:
    """Load shields.csv + shields_upgrades.csv — same structure as weapons."""
    shields_path = DataLakeConfig.KAGGLE_DIR / "shields.csv"
    upgrades_path = DataLakeConfig.KAGGLE_DIR / "shields_upgrades.csv"

    shields_df = pd.read_csv(shields_path)
    upgrades_df = pd.read_csv(upgrades_path)

    shields_df["requirements_parsed"] = shields_df["requirements"].apply(_safe_parse_dict)
    upgrades_df["attack_power_parsed"] = upgrades_df["attack power"].apply(_safe_parse_dict)
    upgrades_df["stat_scaling_parsed"] = upgrades_df["stat scaling"].apply(_safe_parse_dict)
    upgrades_df["damage_reduction_parsed"] = upgrades_df["damage reduction (%)"].apply(_safe_parse_dict)

    upgrades_df["upgrade_level"] = (
        upgrades_df["upgrade"].str.extract(r"\+(\d+)").fillna(0).astype(int)
    )
    upgrades_df["upgrade_type"] = (
        upgrades_df["upgrade"].str.extract(r"^([A-Za-z\s]+)").iloc[:, 0].str.strip()
    )

    merged = pd.merge(
        upgrades_df,
        shields_df[["name", "weight", "category", "requirements_parsed",
                    "damage type", "skill", "FP cost", "dlc"]],
        left_on="shield name",
        right_on="name",
        how="left",
    )
    merged = merged.rename(columns={"shield name": "weapon name"})
    return merged


# ── Stage to Parquet ─────────────────────────────────────────────────────────

def stage_to_parquet(df: pd.DataFrame, name: str) -> Path:
    """
    Serialise a DataFrame to Parquet in the staging area.
    Path: data/staged/{name}.parquet

    Idempotent: overwrites on every run (same path = same date partition).
    """
    out_path = DataLakeConfig.STAGING / f"{name}.parquet"
    out_path.parent.mkdir(parents=True, exist_ok=True)

    # Convert dict columns to JSON strings for Parquet compatibility
    dict_cols = [c for c in df.columns if df[c].dtype == object
                 and df[c].dropna().apply(lambda x: isinstance(x, dict)).any()]
    for col in dict_cols:
        df[col] = df[col].apply(lambda x: json.dumps(x) if isinstance(x, dict) else x)

    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_table(table, str(out_path), compression="snappy")
    logger.info(f"Staged {len(df):,} rows → {out_path}")
    return out_path


# ── Entry point ──────────────────────────────────────────────────────────────

def run_kaggle_load() -> dict[str, int]:
    """
    Load all Kaggle CSVs and stage them as Parquet.
    Returns row counts per dataset.
    """
    summary: dict[str, int] = {}

    weapons = load_weapons()
    stage_to_parquet(weapons, "stg_weapons_upgrades")
    summary["weapons_upgrades"] = len(weapons)

    bosses = load_bosses()
    stage_to_parquet(bosses, "stg_bosses")
    summary["bosses"] = len(bosses)

    shields = load_shields()
    stage_to_parquet(shields, "stg_shields_upgrades")
    summary["shields_upgrades"] = len(shields)

    logger.info(f"Kaggle load complete: {summary}")
    return summary


if __name__ == "__main__":
    run_kaggle_load()
