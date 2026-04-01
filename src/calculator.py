"""
Golden Order Build Calculator
==============================
Interactive tool — enter your build stats and a boss name,
get your Attack Rating + how you'd likely do based on pipeline data.

Usage:
    python -m src.calculator
"""

from __future__ import annotations

import sys
from sqlalchemy import create_engine, text
import pandas as pd

from src.config import WarehouseConfig
from src.transformation.scaling_engine import WeaponScaling, PlayerStats, calculate_attack_rating

ENGINE = None


def get_engine():
    global ENGINE
    if ENGINE is None:
        ENGINE = create_engine(WarehouseConfig.dsn())
    return ENGINE


def pick_from_list(prompt: str, options: list[str], allow_partial: bool = True) -> str:
    """Show a numbered list and let the user pick by number or partial name."""
    print(f"\n{prompt}")
    for i, opt in enumerate(options, 1):
        print(f"  {i:>3}. {opt}")
    while True:
        raw = input("\nEnter number or partial name: ").strip()
        if raw.isdigit():
            idx = int(raw) - 1
            if 0 <= idx < len(options):
                return options[idx]
        elif allow_partial:
            matches = [o for o in options if raw.lower() in o.lower()]
            if len(matches) == 1:
                return matches[0]
            elif len(matches) > 1:
                print(f"  Multiple matches: {matches[:5]} — be more specific.")
        print("  Invalid choice, try again.")


def get_stat(name: str, default: int = 10) -> int:
    while True:
        raw = input(f"  {name} [{default}]: ").strip()
        if raw == "":
            return default
        if raw.isdigit() and 1 <= int(raw) <= 99:
            return int(raw)
        print("    Enter a number between 1 and 99.")


def run_calculator():
    engine = get_engine()

    print("\n" + "=" * 60)
    print("  GOLDEN ORDER BUILD CALCULATOR")
    print("=" * 60)

    # -- 1. Pick boss ----------------------------------------------
    bosses = pd.read_sql(
        text("SELECT boss_name FROM golden_order.dim_bosses ORDER BY boss_name"),
        engine
    )["boss_name"].tolist()

    boss_name = pick_from_list("Choose a boss:", bosses)

    # -- 2. Pick weapon --------------------------------------------
    weapons = pd.read_sql(
        text("SELECT DISTINCT weapon_name FROM golden_order.dim_weapons ORDER BY weapon_name"),
        engine
    )["weapon_name"].tolist()

    weapon_name = pick_from_list("Choose your weapon:", weapons)

    # -- 3. Pick upgrade level -------------------------------------
    upgrades = pd.read_sql(
        text("""
            SELECT DISTINCT upgrade_level
            FROM golden_order.dim_weapons
            WHERE weapon_name = :w
            ORDER BY upgrade_level
        """),
        engine, params={"w": weapon_name}
    )["upgrade_level"].tolist()

    print(f"\nAvailable upgrade levels for {weapon_name}: {upgrades}")
    while True:
        raw = input(f"  Upgrade level [{upgrades[-1]}]: ").strip()
        if raw == "":
            upgrade_level = upgrades[-1]
            break
        if raw.isdigit() and int(raw) in upgrades:
            upgrade_level = int(raw)
            break
        print(f"    Choose from: {upgrades}")

    # -- 4. Enter player stats -------------------------------------
    print("\nEnter your character stats (press Enter to use default 10):")
    strength     = get_stat("Strength", 10)
    dexterity    = get_stat("Dexterity", 10)
    intelligence = get_stat("Intelligence", 10)
    faith        = get_stat("Faith", 10)
    arcane       = get_stat("Arcane", 10)

    two_handing_raw = input("  Two-handing? (y/N): ").strip().lower()
    two_handing = two_handing_raw == "y"

    # -- 5. Fetch weapon data --------------------------------------
    weapon_row = pd.read_sql(
        text("""
            SELECT base_physical, base_magic, base_fire, base_lightning, base_holy,
                   scale_str, scale_dex, scale_int, scale_fai, scale_arc,
                   category, weight
            FROM golden_order.dim_weapons
            WHERE weapon_name = :w AND upgrade_level = :u
            LIMIT 1
        """),
        engine, params={"w": weapon_name, "u": upgrade_level}
    )

    if weapon_row.empty:
        print("  Weapon data not found in warehouse.")
        return

    r = weapon_row.iloc[0]
    weapon = WeaponScaling(
        base_physical  = float(r["base_physical"] or 0),
        base_magic     = float(r["base_magic"]    or 0),
        base_fire      = float(r["base_fire"]     or 0),
        base_lightning = float(r["base_lightning"]or 0),
        base_holy      = float(r["base_holy"]     or 0),
        scaling={
            "Str": str(r["scale_str"] or "-"),
            "Dex": str(r["scale_dex"] or "-"),
            "Int": str(r["scale_int"] or "-"),
            "Fai": str(r["scale_fai"] or "-"),
            "Arc": str(r["scale_arc"] or "-"),
        },
    )
    player = PlayerStats(
        strength=strength, dexterity=dexterity, intelligence=intelligence,
        faith=faith, arcane=arcane, two_handing=two_handing,
    )
    ar_result = calculate_attack_rating(weapon, player)
    total_ar  = ar_result["total"]

    # -- 6. Fetch boss data ----------------------------------------
    boss_row = pd.read_sql(
        text("SELECT hp, is_dlc FROM golden_order.dim_bosses WHERE boss_name = :b LIMIT 1"),
        engine, params={"b": boss_name}
    )
    boss_hp  = int(boss_row.iloc[0]["hp"]) if not boss_row.empty else 0
    is_dlc   = bool(boss_row.iloc[0]["is_dlc"]) if not boss_row.empty else False

    # -- 7. Pull historical stats for this boss --------------------
    hist = pd.read_sql(
        text("""
            SELECT
                COUNT(*)                                          AS total,
                ROUND(AVG(calculated_ar), 1)                     AS avg_ar,
                ROUND(100.0 * SUM(CASE WHEN is_victory THEN 1 ELSE 0 END) / COUNT(*), 1)
                                                                  AS win_rate,
                ROUND(AVG(player_level), 1)                      AS avg_level
            FROM golden_order.fact_encounters
            WHERE boss_name = :b
        """),
        engine, params={"b": boss_name}
    )

    # -- 8. Best weapons vs this boss -----------------------------
    best_weapons = pd.read_sql(
        text("""
            SELECT weapon_used, upgrade_level, win_rate_pct, avg_ar, uses
            FROM golden_order.v_best_weapons_per_boss
            WHERE boss_name = :b
            ORDER BY rank
            LIMIT 5
        """),
        engine, params={"b": boss_name}
    )

    # -- 9. How does my AR compare? --------------------------------
    ar_percentile = pd.read_sql(
        text("""
            SELECT ROUND(
                100.0 * SUM(CASE WHEN calculated_ar < :ar THEN 1 ELSE 0 END) / COUNT(*), 1
            ) AS pct
            FROM golden_order.fact_encounters
            WHERE boss_name = :b
        """),
        engine, params={"b": boss_name, "ar": float(total_ar)}
    )

    # -- 10. Print report ------------------------------------------
    print("\n" + "=" * 60)
    print(f"  REPORT: {weapon_name} +{upgrade_level}  vs  {boss_name}")
    print("=" * 60)

    print(f"\n  YOUR ATTACK RATING")
    print(f"  |- Total AR    : {total_ar:,.1f}")
    for dmg_type, val in ar_result.items():
        if dmg_type != "total" and float(val) > 0:
            print(f"  |- {dmg_type.capitalize():<12}: {float(val):,.1f}")
    print(f"  \- Two-handing : {'Yes (+50% STR)' if two_handing else 'No'}")

    print(f"\n  BOSS INFO")
    print(f"  |- Boss HP     : {boss_hp:,}")
    print(f"  |- DLC boss    : {'Yes' if is_dlc else 'No'}")
    if boss_hp > 0:
        hits = boss_hp / total_ar if total_ar > 0 else float("inf")
        print(f"  \- Hits to kill: ~{hits:.0f}  (ignoring resistances / phase changes)")

    if not hist.empty and hist.iloc[0]["total"] > 0:
        h = hist.iloc[0]
        pct = float(ar_percentile.iloc[0]["pct"]) if not ar_percentile.empty else 0
        print(f"\n  COMMUNITY STATS  ({int(h['total']):,} encounters in pipeline)")
        print(f"  |- Avg player AR   : {h['avg_ar']}")
        print(f"  |- Your AR ranks   : top {100 - pct:.0f}% of players who fought this boss")
        print(f"  |- Overall win rate: {h['win_rate']}%")
        print(f"  \- Avg player level: {h['avg_level']}")

    if not best_weapons.empty:
        print(f"\n  TOP 5 WEAPONS vs {boss_name}")
        print(f"  {'Weapon':<30} {'Upgrade':>7}  {'Win%':>6}  {'Avg AR':>7}  {'Uses':>5}")
        print("  " + "-" * 58)
        for _, row in best_weapons.iterrows():
            marker = " << YOU" if row["weapon_used"] == weapon_name else ""
            print(f"  {row['weapon_used']:<30} +{int(row['upgrade_level']):>6}  "
                  f"{row['win_rate_pct']:>5.1f}%  {row['avg_ar']:>7.1f}  {int(row['uses']):>5}{marker}")
    else:
        print(f"\n  (Not enough data yet for top weapons vs {boss_name} — run more encounters)")

    print("\n" + "=" * 60)
    another = input("\nCalculate another build? (y/N): ").strip().lower()
    if another == "y":
        run_calculator()


if __name__ == "__main__":
    try:
        run_calculator()
    except KeyboardInterrupt:
        print("\n\nExiting.")
        sys.exit(0)
