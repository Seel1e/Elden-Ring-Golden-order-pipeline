"""
Phase 4 — Analytics Queries
=============================
Programmatic wrappers around the SQL analytics views.
These power the Metabase dashboard and can also be run standalone.

Run this file to print a live summary of the current pipeline data:
    python -m src.analytics.queries
"""

import pandas as pd
from loguru import logger
from sqlalchemy import create_engine, text

from src.config import WarehouseConfig


def get_engine():
    return create_engine(WarehouseConfig.dsn())


# ── Dashboard 1: Most Lethal Bosses ──────────────────────────────────────────

def most_lethal_bosses(top_n: int = 15) -> pd.DataFrame:
    """
    Return the top N bosses by player death rate.

    Columns: boss_name, boss_hp, is_dlc, total_encounters,
             death_rate_pct, avg_player_level, avg_ar_on_death
    """
    sql = f"""
        SELECT
            boss_name,
            boss_hp,
            is_dlc,
            total_encounters,
            total_deaths,
            death_rate_pct,
            avg_player_level,
            avg_ar_on_death
        FROM golden_order.v_boss_lethality
        ORDER BY death_rate_pct DESC
        LIMIT {top_n}
    """
    return pd.read_sql(text(sql), get_engine())


# ── Dashboard 2: Best Weapon Builds ──────────────────────────────────────────

def highest_win_rate_builds(category: str | None = None,
                            archetype: str | None = None,
                            top_n: int = 20) -> pd.DataFrame:
    """
    Return the top N weapon builds by win rate.

    Filters
    -------
    category  : e.g. "Greatswords", "Katanas", "Colossal Swords" (optional)
    archetype : e.g. "str", "dex", "quality", "int", "faith" (optional)
    """
    conditions = ["1=1"]
    if category:
        conditions.append(f"category = '{category}'")
    if archetype:
        conditions.append(f"archetype = '{archetype}'")
    where_clause = " AND ".join(conditions)

    sql = f"""
        SELECT
            weapon_used,
            upgrade_level,
            category,
            archetype,
            total_uses,
            win_rate_pct,
            avg_ar,
            max_ar_seen
        FROM golden_order.v_weapon_win_rates
        WHERE {where_clause}
        ORDER BY win_rate_pct DESC
        LIMIT {top_n}
    """
    return pd.read_sql(text(sql), get_engine())


# ── Best weapon per boss ──────────────────────────────────────────────────────

def best_weapons_for_boss(boss_name: str) -> pd.DataFrame:
    """Return the top 5 weapons by win rate for a specific boss."""
    sql = """
        SELECT weapon_used, upgrade_level, category, uses, win_rate_pct, avg_ar
        FROM golden_order.v_best_weapons_per_boss
        WHERE boss_name = :boss_name
        ORDER BY rank
    """
    return pd.read_sql(text(sql), get_engine(), params={"boss_name": boss_name})


# ── Soft cap analysis ─────────────────────────────────────────────────────────

def softcap_stat_distribution() -> pd.DataFrame:
    """Show average stats per archetype and % of players past the soft caps."""
    sql = "SELECT * FROM golden_order.v_stat_distribution ORDER BY avg_ar DESC"
    return pd.read_sql(text(sql), get_engine())


# ── Rolling win rate ──────────────────────────────────────────────────────────

def rolling_win_rate() -> pd.DataFrame:
    """Return the per-minute rolling win rate for the last hour."""
    sql = "SELECT * FROM golden_order.v_rolling_win_rate_1h"
    return pd.read_sql(text(sql), get_engine())


# ── Pretty print reports ──────────────────────────────────────────────────────

def print_dashboard() -> None:
    """Print a full text summary of all dashboards to stdout."""

    print("\n" + "═" * 70)
    print("  GOLDEN ORDER ANALYTICS — ELDEN RING COMBAT DATA PIPELINE")
    print("═" * 70)

    print("\n▶ DASHBOARD 1: Most Lethal Bosses (Top 15 by Death Rate)")
    print("─" * 70)
    try:
        df = most_lethal_bosses(15)
        if df.empty:
            print("  (no data yet — run the streaming pipeline first)")
        else:
            print(df.to_string(index=False))
    except Exception as e:
        print(f"  Error: {e}")

    print("\n▶ DASHBOARD 2: Highest Win-Rate Weapon Builds (All archetypes)")
    print("─" * 70)
    try:
        df = highest_win_rate_builds(top_n=20)
        if df.empty:
            print("  (no data yet)")
        else:
            print(df.to_string(index=False))
    except Exception as e:
        print(f"  Error: {e}")

    print("\n▶ SOFT CAP EFFICIENCY — Stat Distribution by Archetype")
    print("─" * 70)
    try:
        df = softcap_stat_distribution()
        if df.empty:
            print("  (no data yet)")
        else:
            print(df.to_string(index=False))
    except Exception as e:
        print(f"  Error: {e}")

    print("\n" + "═" * 70 + "\n")


if __name__ == "__main__":
    print_dashboard()
