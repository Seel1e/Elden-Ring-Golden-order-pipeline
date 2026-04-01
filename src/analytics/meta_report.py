"""
Golden Order — Meta Report
===========================
Generates a daily meta snapshot from the fact_encounters table and saves it
to golden_order.fact_meta_reports. Designed to be called:

  - Standalone:  python -m src.analytics.meta_report
  - From Airflow: from src.analytics.meta_report import run_meta_report
"""

from __future__ import annotations

import json
from datetime import datetime, timezone

from loguru import logger
from sqlalchemy import create_engine, text
import pandas as pd

from src.config import WarehouseConfig


# ── Ensure report table exists ────────────────────────────────────────────────

_DDL = """
CREATE TABLE IF NOT EXISTS golden_order.fact_meta_reports (
    report_id       SERIAL PRIMARY KEY,
    report_date     DATE        NOT NULL DEFAULT CURRENT_DATE,
    generated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    total_encounters BIGINT,
    overall_win_rate NUMERIC(5,2),
    hardest_boss    TEXT,
    hardest_boss_death_rate NUMERIC(5,2),
    easiest_boss    TEXT,
    easiest_boss_death_rate NUMERIC(5,2),
    top_weapon      TEXT,
    top_weapon_win_rate NUMERIC(5,2),
    most_used_weapon TEXT,
    most_used_count  BIGINT,
    top_archetype   TEXT,
    top_archetype_win_rate NUMERIC(5,2),
    top_10_builds   JSONB,
    archetype_tier  JSONB,
    top_weapons_by_boss JSONB
);
"""


def _engine():
    return create_engine(WarehouseConfig.dsn())


def _qry(sql: str, params: dict | None = None) -> pd.DataFrame:
    return pd.read_sql(text(sql), _engine(), params=params or {})


# ── Individual report sections ────────────────────────────────────────────────

def _overall_stats() -> dict:
    df = _qry("""
        SELECT
            COUNT(*) AS total_encounters,
            ROUND(100.0 * SUM(CASE WHEN is_victory THEN 1 ELSE 0 END)::numeric / COUNT(*), 2)
                AS overall_win_rate
        FROM golden_order.fact_encounters
    """)
    row = df.iloc[0]
    return {
        "total_encounters": int(row["total_encounters"]),
        "overall_win_rate": float(row["overall_win_rate"]),
    }


def _boss_lethality() -> dict:
    df = _qry("""
        SELECT boss_name,
               ROUND(100.0 * SUM(CASE WHEN NOT is_victory THEN 1 ELSE 0 END)::numeric / COUNT(*), 2)
                   AS death_rate,
               COUNT(*) AS encounters
        FROM golden_order.fact_encounters
        GROUP BY boss_name
        HAVING COUNT(*) >= 50
        ORDER BY death_rate DESC
    """)
    if df.empty:
        return {"hardest_boss": None, "hardest_death_rate": None,
                "easiest_boss": None, "easiest_death_rate": None}
    return {
        "hardest_boss":       df.iloc[0]["boss_name"],
        "hardest_death_rate": float(df.iloc[0]["death_rate"]),
        "easiest_boss":       df.iloc[-1]["boss_name"],
        "easiest_death_rate": float(df.iloc[-1]["death_rate"]),
        "full_leaderboard":   df.to_dict(orient="records"),
    }


def _weapon_meta() -> dict:
    win_rate_df = _qry("""
        SELECT weapon_used, upgrade_level, category, archetype,
               COUNT(*) AS uses,
               ROUND(100.0 * SUM(CASE WHEN is_victory THEN 1 ELSE 0 END)::numeric / COUNT(*), 2)
                   AS win_rate,
               ROUND(AVG(calculated_ar), 1) AS avg_ar
        FROM golden_order.fact_encounters
        WHERE weapon_used IS NOT NULL
        GROUP BY weapon_used, upgrade_level, category, archetype
        HAVING COUNT(*) >= 30
        ORDER BY win_rate DESC
        LIMIT 10
    """)

    most_used_df = _qry("""
        SELECT weapon_used, COUNT(*) AS uses
        FROM golden_order.fact_encounters
        WHERE weapon_used IS NOT NULL
        GROUP BY weapon_used
        ORDER BY uses DESC
        LIMIT 1
    """)

    result = {
        "top_10_builds": win_rate_df.to_dict(orient="records"),
        "most_used_weapon": None,
        "most_used_count": None,
        "top_weapon": None,
        "top_weapon_win_rate": None,
    }
    if not win_rate_df.empty:
        result["top_weapon"] = win_rate_df.iloc[0]["weapon_used"]
        result["top_weapon_win_rate"] = float(win_rate_df.iloc[0]["win_rate"])
    if not most_used_df.empty:
        result["most_used_weapon"] = most_used_df.iloc[0]["weapon_used"]
        result["most_used_count"] = int(most_used_df.iloc[0]["uses"])
    return result


def _archetype_tier() -> dict:
    df = _qry("""
        SELECT archetype,
               ROUND(100.0 * SUM(CASE WHEN is_victory THEN 1 ELSE 0 END)::numeric / COUNT(*), 2)
                   AS win_rate,
               ROUND(AVG(calculated_ar), 1) AS avg_ar,
               COUNT(*) AS encounters
        FROM golden_order.fact_encounters
        GROUP BY archetype
        ORDER BY win_rate DESC
    """)
    result = {"archetype_tier": df.to_dict(orient="records"),
              "top_archetype": None, "top_archetype_win_rate": None}
    if not df.empty:
        result["top_archetype"] = df.iloc[0]["archetype"]
        result["top_archetype_win_rate"] = float(df.iloc[0]["win_rate"])
    return result


def _top_weapons_per_boss() -> list[dict]:
    df = _qry("""
        WITH ranked AS (
            SELECT boss_name, weapon_used, upgrade_level, category,
                   COUNT(*) AS uses,
                   ROUND(100.0 * SUM(CASE WHEN is_victory THEN 1 ELSE 0 END)::numeric / COUNT(*), 2)
                       AS win_rate,
                   ROW_NUMBER() OVER (
                       PARTITION BY boss_name
                       ORDER BY SUM(CASE WHEN is_victory THEN 1 ELSE 0 END)::float / COUNT(*) DESC
                   ) AS rnk
            FROM golden_order.fact_encounters
            WHERE weapon_used IS NOT NULL
            GROUP BY boss_name, weapon_used, upgrade_level, category
            HAVING COUNT(*) >= 15
        )
        SELECT boss_name, weapon_used, upgrade_level, category, uses, win_rate
        FROM ranked WHERE rnk = 1
        ORDER BY win_rate DESC
        LIMIT 20
    """)
    return df.to_dict(orient="records")


# ── Main runner ───────────────────────────────────────────────────────────────

def run_meta_report() -> dict:
    """Generate and persist the full meta report. Returns the report dict."""
    logger.info("Generating meta report...")
    engine = _engine()

    # Ensure table exists
    with engine.begin() as conn:
        conn.execute(text(_DDL))

    overall  = _overall_stats()
    bosses   = _boss_lethality()
    weapons  = _weapon_meta()
    archetypes = _archetype_tier()
    top_per_boss = _top_weapons_per_boss()

    report = {
        "generated_at":     datetime.now(timezone.utc).isoformat(),
        "report_date":      datetime.now(timezone.utc).date().isoformat(),
        **overall,
        **bosses,
        **weapons,
        **archetypes,
        "top_weapons_per_boss": top_per_boss,
    }

    # Persist to DB
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO golden_order.fact_meta_reports (
                report_date, total_encounters, overall_win_rate,
                hardest_boss, hardest_boss_death_rate,
                easiest_boss, easiest_boss_death_rate,
                top_weapon, top_weapon_win_rate,
                most_used_weapon, most_used_count,
                top_archetype, top_archetype_win_rate,
                top_10_builds, archetype_tier, top_weapons_by_boss
            ) VALUES (
                CURRENT_DATE, :total, :win_rate,
                :hardest, :hardest_dr,
                :easiest, :easiest_dr,
                :top_w, :top_wr,
                :most_used, :most_count,
                :top_arch, :top_arch_wr,
                :builds::jsonb, :arch_tier::jsonb, :top_boss::jsonb
            )
            ON CONFLICT DO NOTHING
        """), dict(
            total      = overall["total_encounters"],
            win_rate   = overall["overall_win_rate"],
            hardest    = bosses.get("hardest_boss"),
            hardest_dr = bosses.get("hardest_death_rate"),
            easiest    = bosses.get("easiest_boss"),
            easiest_dr = bosses.get("easiest_death_rate"),
            top_w      = weapons.get("top_weapon"),
            top_wr     = weapons.get("top_weapon_win_rate"),
            most_used  = weapons.get("most_used_weapon"),
            most_count = weapons.get("most_used_count"),
            top_arch   = archetypes.get("top_archetype"),
            top_arch_wr= archetypes.get("top_archetype_win_rate"),
            builds     = json.dumps(weapons["top_10_builds"]),
            arch_tier  = json.dumps(archetypes["archetype_tier"]),
            top_boss   = json.dumps(top_per_boss),
        ))

    logger.info(
        f"Meta report saved: {overall['total_encounters']:,} encounters | "
        f"Hardest boss: {bosses.get('hardest_boss')} ({bosses.get('hardest_death_rate')}% death rate) | "
        f"Top weapon: {weapons.get('top_weapon')} ({weapons.get('top_weapon_win_rate')}% win rate)"
    )

    _print_report(report)
    return report


def _print_report(r: dict) -> None:
    print("\n" + "=" * 62)
    print("  GOLDEN ORDER META REPORT")
    print(f"  Generated: {r['generated_at'][:19]} UTC")
    print("=" * 62)
    print(f"\n  Total encounters : {r['total_encounters']:,}")
    print(f"  Overall win rate : {r['overall_win_rate']}%")
    print(f"\n  Hardest boss  : {r.get('hardest_boss')} ({r.get('hardest_death_rate')}% death rate)")
    print(f"  Easiest boss  : {r.get('easiest_boss')} ({r.get('easiest_death_rate')}% death rate)")
    print(f"\n  Top weapon    : {r.get('top_weapon')} ({r.get('top_weapon_win_rate')}% win rate)")
    print(f"  Most used     : {r.get('most_used_weapon')} ({r.get('most_used_count'):,} uses)")
    print(f"\n  Best archetype: {r.get('top_archetype')} ({r.get('top_archetype_win_rate')}% win rate)")

    print("\n  ARCHETYPE TIER LIST")
    print(f"  {'Rank':<5} {'Archetype':<12} {'Win Rate':>10}  {'Avg AR':>8}  {'Encounters':>12}")
    print("  " + "-" * 52)
    for i, row in enumerate(r.get("archetype_tier", []), 1):
        print(f"  {i:<5} {row['archetype']:<12} {row['win_rate']:>9.1f}%  "
              f"{row['avg_ar']:>8.1f}  {row['encounters']:>12,}")

    print("\n  TOP 10 WEAPON BUILDS")
    print(f"  {'Weapon':<30} {'Upg':>4}  {'Win%':>6}  {'Avg AR':>8}  {'Uses':>6}")
    print("  " + "-" * 60)
    for row in r.get("top_10_builds", []):
        print(f"  {row['weapon_used']:<30} +{row['upgrade_level']:>3}  "
              f"{row['win_rate']:>5.1f}%  {row['avg_ar']:>8.1f}  {row['uses']:>6,}")

    print("\n" + "=" * 62 + "\n")


if __name__ == "__main__":
    run_meta_report()
