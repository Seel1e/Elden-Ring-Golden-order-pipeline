"""
Phase 2 — JSON Flattener
========================
Parses the deeply nested dict-as-string columns from the Kaggle CSVs
(and the raw API JSON) into flat, typed columns ready for dim table loading.

The weapons_upgrades.csv has columns like:
    attack power  → "{'Phy': '125 ', 'Mag': '- ', 'Fir': '- ', ...}"
    stat scaling  → "{'Str': 'D ', 'Dex': 'D ', 'Int': '- ', ...}"

This module converts those into tidy numeric / grade columns.
"""

import ast
import re
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq
from loguru import logger

from src.config import DataLakeConfig


# ── Parsing helpers ───────────────────────────────────────────────────────────

def _parse_dict_col(series: pd.Series) -> pd.Series:
    """Turn a Series of dict-as-string into a Series of actual dicts."""
    def _parse(val):
        if pd.isna(val) or str(val).strip() in ("", "nan"):
            return {}
        try:
            return ast.literal_eval(str(val))
        except Exception:
            return {}
    return series.apply(_parse)


def _clean_numeric(val: str | float) -> float:
    """Strip whitespace and convert '- ' → 0.0, '125 ' → 125.0."""
    if pd.isna(val):
        return 0.0
    s = str(val).strip()
    if s in ("-", "", "nan"):
        return 0.0
    try:
        return float(s)
    except ValueError:
        return 0.0


def _clean_grade(val: str | float) -> str:
    """Normalise grade letters: 'D ' → 'D', '- ' → '-'."""
    if pd.isna(val):
        return "-"
    s = str(val).strip()
    if s in ("", "nan"):
        return "-"
    return s


# ── Weapons upgrade flattener ─────────────────────────────────────────────────

def flatten_weapons_upgrades(df: pd.DataFrame) -> pd.DataFrame:
    """
    Input:  staged weapons_upgrades DataFrame with dict-string columns.
    Output: flat DataFrame with one numeric/grade column per sub-field.

    New columns added
    -----------------
    atk_physical, atk_magic, atk_fire, atk_lightning, atk_holy, atk_stamina
    scale_str, scale_dex, scale_int, scale_fai, scale_arc
    dr_physical, dr_magic, dr_fire, dr_lightning, dr_holy, dr_boost, dr_resist
    """
    logger.info(f"Flattening {len(df):,} weapon upgrade rows…")

    # Parse JSON columns
    atk_dicts   = _parse_dict_col(df["attack_power_parsed"])
    scale_dicts = _parse_dict_col(df["stat_scaling_parsed"])
    dr_dicts    = _parse_dict_col(df["damage_reduction_parsed"])

    # ── Attack power ────────────────────────────────────────────────────────
    df["atk_physical"]  = atk_dicts.apply(lambda d: _clean_numeric(d.get("Phy", 0)))
    df["atk_magic"]     = atk_dicts.apply(lambda d: _clean_numeric(d.get("Mag", 0)))
    df["atk_fire"]      = atk_dicts.apply(lambda d: _clean_numeric(d.get("Fir", 0)))
    df["atk_lightning"] = atk_dicts.apply(lambda d: _clean_numeric(d.get("Lit", 0)))
    df["atk_holy"]      = atk_dicts.apply(lambda d: _clean_numeric(d.get("Hol", 0)))
    df["atk_stamina"]   = atk_dicts.apply(lambda d: _clean_numeric(d.get("Sta", 0)))

    # ── Stat scaling grades ──────────────────────────────────────────────────
    df["scale_str"] = scale_dicts.apply(lambda d: _clean_grade(d.get("Str", "-")))
    df["scale_dex"] = scale_dicts.apply(lambda d: _clean_grade(d.get("Dex", "-")))
    df["scale_int"] = scale_dicts.apply(lambda d: _clean_grade(d.get("Int", "-")))
    df["scale_fai"] = scale_dicts.apply(lambda d: _clean_grade(d.get("Fai", "-")))
    df["scale_arc"] = scale_dicts.apply(lambda d: _clean_grade(d.get("Arc", "-")))

    # ── Damage reduction % ───────────────────────────────────────────────────
    df["dr_physical"]  = dr_dicts.apply(lambda d: _clean_numeric(d.get("Phy", 0)))
    df["dr_magic"]     = dr_dicts.apply(lambda d: _clean_numeric(d.get("Mag", 0)))
    df["dr_fire"]      = dr_dicts.apply(lambda d: _clean_numeric(d.get("Fir", 0)))
    df["dr_lightning"] = dr_dicts.apply(lambda d: _clean_numeric(d.get("Lit", 0)))
    df["dr_holy"]      = dr_dicts.apply(lambda d: _clean_numeric(d.get("Hol", 0)))
    df["dr_boost"]     = dr_dicts.apply(lambda d: _clean_numeric(d.get("Bst", 0)))
    df["dr_resist"]    = dr_dicts.apply(lambda d: _clean_numeric(d.get("Rst", 0)))

    # ── Requirements ────────────────────────────────────────────────────────
    req_dicts = _parse_dict_col(df["requirements_parsed"])
    df["req_str"] = req_dicts.apply(lambda d: int(_clean_numeric(d.get("Str", 0))))
    df["req_dex"] = req_dicts.apply(lambda d: int(_clean_numeric(d.get("Dex", 0))))
    df["req_int"] = req_dicts.apply(lambda d: int(_clean_numeric(d.get("Int", 0))))
    df["req_fai"] = req_dicts.apply(lambda d: int(_clean_numeric(d.get("Fai", 0))))
    df["req_arc"] = req_dicts.apply(lambda d: int(_clean_numeric(d.get("Arc", 0))))

    logger.info("Weapons upgrade flattening complete.")
    return df


def flatten_bosses(df: pd.DataFrame) -> pd.DataFrame:
    """
    Flatten boss drops JSON and normalise HP.
    Adds: drops_runes (int), drops_items (list→comma-sep string)
    """
    logger.info(f"Flattening {len(df)} boss rows…")

    import json

    def _parse_drops(drops_str):
        if pd.isna(drops_str):
            return 0, ""
        try:
            d = ast.literal_eval(str(drops_str)) if isinstance(drops_str, str) else drops_str
        except Exception:
            try:
                d = json.loads(str(drops_str).replace("'", '"'))
            except Exception:
                return 0, ""

        all_runes = 0
        all_items: list[str] = []
        for location, drops in d.items():
            if isinstance(drops, list):
                for item in drops:
                    s = str(item).replace(",", "")
                    try:
                        all_runes += int(s)
                    except ValueError:
                        all_items.append(str(item))
        return all_runes, ", ".join(all_items)

    parsed = df["drops_parsed"].apply(_parse_drops)
    df["drops_runes"] = parsed.apply(lambda x: x[0])
    df["drops_items"] = parsed.apply(lambda x: x[1])

    logger.info("Boss flattening complete.")
    return df


# ── Load + flatten pipeline function ─────────────────────────────────────────

def load_and_flatten_weapons() -> pd.DataFrame:
    path = DataLakeConfig.STAGING / "stg_weapons_upgrades.parquet"
    df = pd.read_parquet(str(path))
    return flatten_weapons_upgrades(df)


def load_and_flatten_bosses() -> pd.DataFrame:
    path = DataLakeConfig.STAGING / "stg_bosses.parquet"
    df = pd.read_parquet(str(path))
    return flatten_bosses(df)


if __name__ == "__main__":
    weapons = load_and_flatten_weapons()
    print(weapons[["weapon name", "upgrade_level", "atk_physical",
                   "scale_str", "scale_dex"]].head(10))
