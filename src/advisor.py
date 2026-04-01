"""
Golden Order Build Advisor
===========================
Interactive advisor — pick your class, level, weapon and get:
  1. Your Attack Rating breakdown
  2. Soft-cap advisor (where to put next stat points)
  3. Boss gauntlet (all bosses ranked easiest -> hardest for YOUR build)
  4. Save / load / compare builds

Usage:
    python -m src.advisor
"""

from __future__ import annotations
import sys
from dataclasses import dataclass
from sqlalchemy import create_engine, text
import pandas as pd

from src.config import WarehouseConfig
from src.transformation.scaling_engine import (
    WeaponScaling, PlayerStats, calculate_attack_rating,
)

# ── Helpers ───────────────────────────────────────────────────────────────────

def engine():
    if not hasattr(engine, "_e"):
        engine._e = create_engine(WarehouseConfig.dsn())
    return engine._e


def sep(char="=", width=62):
    print(char * width)


def header(title: str):
    sep()
    print(f"  {title}")
    sep()


def pick(prompt: str, options: list[str]) -> str:
    print(f"\n{prompt}")
    for i, o in enumerate(options, 1):
        print(f"  {i:>3}. {o}")
    while True:
        raw = input("  Enter number or partial name: ").strip()
        if raw.isdigit() and 1 <= int(raw) <= len(options):
            return options[int(raw) - 1]
        matches = [o for o in options if raw.lower() in o.lower()]
        if len(matches) == 1:
            return matches[0]
        if len(matches) > 1:
            print(f"  Multiple matches: {[m for m in matches[:5]]} -- be more specific.")
            continue
        print("  Not found, try again.")


def ask_int(prompt: str, default: int, lo: int = 1, hi: int = 99) -> int:
    while True:
        raw = input(f"  {prompt} [{default}]: ").strip()
        if raw == "":
            return default
        if raw.lstrip("-").isdigit() and lo <= int(raw) <= hi:
            return int(raw)
        print(f"  Enter a number between {lo} and {hi}.")


# ── Build dataclass ───────────────────────────────────────────────────────────

@dataclass
class Build:
    class_name:      str  = "Wretch"
    character_level: int  = 1
    strength:        int  = 10
    dexterity:       int  = 10
    intelligence:    int  = 10
    faith:           int  = 10
    arcane:          int  = 10
    weapon_name:     str  = ""
    upgrade_level:   int  = 25
    two_handing:     bool = False

    def player_stats(self) -> PlayerStats:
        return PlayerStats(
            strength=self.strength,
            dexterity=self.dexterity,
            intelligence=self.intelligence,
            faith=self.faith,
            arcane=self.arcane,
            two_handing=self.two_handing,
        )

    def total_ar(self, weapon_row: pd.Series) -> float:
        w = WeaponScaling(
            base_physical  = float(weapon_row.get("base_physical")   or 0),
            base_magic     = float(weapon_row.get("base_magic")      or 0),
            base_fire      = float(weapon_row.get("base_fire")       or 0),
            base_lightning = float(weapon_row.get("base_lightning")  or 0),
            base_holy      = float(weapon_row.get("base_holy")       or 0),
            scaling={
                "Str": str(weapon_row.get("scale_str") or "-"),
                "Dex": str(weapon_row.get("scale_dex") or "-"),
                "Int": str(weapon_row.get("scale_int") or "-"),
                "Fai": str(weapon_row.get("scale_fai") or "-"),
                "Arc": str(weapon_row.get("scale_arc") or "-"),
            },
        )
        return calculate_attack_rating(w, self.player_stats())["total"]


# ── DB helpers ────────────────────────────────────────────────────────────────

def fetch_weapon_row(weapon_name: str, upgrade_level: int) -> pd.Series | None:
    df = pd.read_sql(text("""
        SELECT base_physical, base_magic, base_fire, base_lightning, base_holy,
               scale_str, scale_dex, scale_int, scale_fai, scale_arc,
               category, weight
        FROM golden_order.dim_weapons
        WHERE weapon_name = :w AND upgrade_level = :u LIMIT 1
    """), engine(), params={"w": weapon_name, "u": upgrade_level})
    return df.iloc[0] if not df.empty else None


def all_weapons() -> list[str]:
    return pd.read_sql(
        text("SELECT DISTINCT weapon_name FROM golden_order.dim_weapons ORDER BY weapon_name"),
        engine()
    )["weapon_name"].tolist()


def all_bosses() -> pd.DataFrame:
    return pd.read_sql(text("""
        SELECT boss_name, hp, is_dlc FROM golden_order.dim_bosses ORDER BY boss_name
    """), engine())


def all_classes() -> pd.DataFrame:
    return pd.read_sql(text("""
        SELECT * FROM golden_order.dim_classes ORDER BY base_level
    """), engine())


# ── Step 1: Choose class + level ──────────────────────────────────────────────

def step_class_and_level(build: Build) -> Build:
    header("STEP 1 — STARTING CLASS & LEVEL")
    classes_df = all_classes()
    class_names = classes_df["class_name"].tolist()

    print("\n  Class         Base Lvl  STR  DEX  INT  FAI  ARC")
    print("  " + "-" * 52)
    for _, row in classes_df.iterrows():
        print(f"  {row['class_name']:<14} {row['base_level']:>6}   "
              f"{row['base_strength']:>3}  {row['base_dexterity']:>3}  "
              f"{row['base_intelligence']:>3}  {row['base_faith']:>3}  {row['base_arcane']:>3}")

    chosen = pick("Choose your starting class:", class_names)
    c = classes_df[classes_df["class_name"] == chosen].iloc[0]

    lvl = ask_int("Character level", 60, lo=int(c["base_level"]), hi=713)
    free_points = lvl - int(c["base_level"])

    build.class_name      = chosen
    build.character_level = lvl
    build.strength        = int(c["base_strength"])
    build.dexterity       = int(c["base_dexterity"])
    build.intelligence    = int(c["base_intelligence"])
    build.faith           = int(c["base_faith"])
    build.arcane          = int(c["base_arcane"])

    print(f"\n  Base stats loaded for {chosen} (Level {lvl}).")
    print(f"  You have {free_points} points to distribute.\n")
    print("  Adjust stats manually? (or press Enter to skip and auto-advise later)")
    if input("  Edit stats? (y/N): ").strip().lower() == "y":
        build.strength     = ask_int("Strength",     build.strength)
        build.dexterity    = ask_int("Dexterity",    build.dexterity)
        build.intelligence = ask_int("Intelligence", build.intelligence)
        build.faith        = ask_int("Faith",        build.faith)
        build.arcane       = ask_int("Arcane",       build.arcane)
        actual_used = (
            (build.strength     - int(c["base_strength"]))     +
            (build.dexterity    - int(c["base_dexterity"]))    +
            (build.intelligence - int(c["base_intelligence"])) +
            (build.faith        - int(c["base_faith"]))        +
            (build.arcane       - int(c["base_arcane"]))
        )
        print(f"\n  Points used: {actual_used} / {free_points}  "
              f"({'OK' if actual_used <= free_points else 'OVER BUDGET by ' + str(actual_used - free_points)})")

    return build


# ── Step 2: Choose weapon ─────────────────────────────────────────────────────

def step_weapon(build: Build) -> Build:
    header("STEP 2 — WEAPON")
    weapons = all_weapons()
    build.weapon_name = pick("Choose your weapon:", weapons)

    upgrades = pd.read_sql(text("""
        SELECT DISTINCT upgrade_level FROM golden_order.dim_weapons
        WHERE weapon_name = :w ORDER BY upgrade_level
    """), engine(), params={"w": build.weapon_name})["upgrade_level"].tolist()

    print(f"\n  Available upgrade levels: {upgrades}")
    build.upgrade_level = ask_int("Upgrade level", upgrades[-1], lo=min(upgrades), hi=max(upgrades))
    build.two_handing = input("  Two-handing? (y/N): ").strip().lower() == "y"
    return build


# ── Step 3: AR + soft cap advisor ────────────────────────────────────────────

def step_ar_and_softcap(build: Build):
    header("STEP 3 — ATTACK RATING + SOFT CAP ADVISOR")
    row = fetch_weapon_row(build.weapon_name, build.upgrade_level)
    if row is None:
        print("  Weapon data not found."); return

    w = WeaponScaling(
        base_physical  = float(row.get("base_physical")  or 0),
        base_magic     = float(row.get("base_magic")     or 0),
        base_fire      = float(row.get("base_fire")      or 0),
        base_lightning = float(row.get("base_lightning") or 0),
        base_holy      = float(row.get("base_holy")      or 0),
        scaling={
            "Str": str(row.get("scale_str") or "-"),
            "Dex": str(row.get("scale_dex") or "-"),
            "Int": str(row.get("scale_int") or "-"),
            "Fai": str(row.get("scale_fai") or "-"),
            "Arc": str(row.get("scale_arc") or "-"),
        },
    )
    base_ar = calculate_attack_rating(w, build.player_stats())
    total   = base_ar["total"]

    print(f"\n  Weapon : {build.weapon_name} +{build.upgrade_level}")
    print(f"  Class  : {build.class_name}  |  Level: {build.character_level}")
    print(f"\n  ATTACK RATING BREAKDOWN")
    print(f"  {'Total AR':<16}: {total:>8.1f}")
    for dmg, val in base_ar.items():
        if dmg != "total" and float(val) > 0:
            print(f"  {dmg.capitalize():<16}: {float(val):>8.1f}")

    # Soft cap advisor — test +5 in each stat
    print(f"\n  SOFT CAP ADVISOR  (gain from next +5 stat points)")
    print(f"  {'Stat':<14} {'Current':>7}  {'AR +5pts':>9}  {'Gain':>7}  {'Verdict'}")
    print("  " + "-" * 55)

    stat_fields = [
        ("Strength",     "strength"),
        ("Dexterity",    "dexterity"),
        ("Intelligence", "intelligence"),
        ("Faith",        "faith"),
        ("Arcane",       "arcane"),
    ]

    for label, field in stat_fields:
        bumped = Build(
            strength     = build.strength     + (5 if field == "strength"     else 0),
            dexterity    = build.dexterity    + (5 if field == "dexterity"    else 0),
            intelligence = build.intelligence + (5 if field == "intelligence" else 0),
            faith        = build.faith        + (5 if field == "faith"        else 0),
            arcane       = build.arcane       + (5 if field == "arcane"       else 0),
            two_handing  = build.two_handing,
        )
        new_ar = calculate_attack_rating(w, bumped.player_stats())["total"]
        gain   = new_ar - total
        current_val = getattr(build, field)

        if gain >= 30:
            verdict = "*** Excellent"
        elif gain >= 15:
            verdict = "** Good"
        elif gain >= 5:
            verdict = "* Moderate"
        else:
            verdict = "  Diminishing returns"

        print(f"  {label:<14} {current_val:>7}   {new_ar:>8.1f}  {gain:>+7.1f}  {verdict}")


# ── Step 4: Boss gauntlet ─────────────────────────────────────────────────────

def step_boss_gauntlet(build: Build):
    header("STEP 4 -- BOSS GAUNTLET  (all bosses ranked for your build)")
    row = fetch_weapon_row(build.weapon_name, build.upgrade_level)
    if row is None:
        print("  Weapon data not found."); return

    ar = build.total_ar(row)

    bosses_df = all_bosses()
    # Pull community death rates
    community = pd.read_sql(text("""
        SELECT
            boss_name,
            COUNT(*) AS encounters,
            ROUND(100.0 * SUM(CASE WHEN NOT is_victory THEN 1 ELSE 0 END) / COUNT(*), 1)
                AS death_rate
        FROM golden_order.fact_encounters
        GROUP BY boss_name
        HAVING COUNT(*) >= 20
    """), engine())

    merged = bosses_df.merge(community, on="boss_name", how="left")
    merged["death_rate"] = merged["death_rate"].fillna(50.0)
    merged["encounters"] = merged["encounters"].fillna(0).astype(int)

    # Difficulty score: weighted combo of death rate + how many hits to kill
    merged["hits_to_kill"] = merged["hp"] / ar
    merged["difficulty"] = (
        merged["death_rate"] * 0.7 +
        merged["hits_to_kill"].clip(upper=100) * 0.3
    )
    merged = merged.sort_values("difficulty")

    print(f"\n  Your AR: {ar:.1f}  |  Weapon: {build.weapon_name} +{build.upgrade_level}")
    print(f"\n  {'#':>3}  {'Boss':<38} {'HP':>7}  {'Hits':>5}  {'Death%':>7}  {'DLC':>4}")
    print("  " + "-" * 68)

    for i, (_, r) in enumerate(merged.iterrows(), 1):
        dlc = "DLC" if r["is_dlc"] else "   "
        bar_count = int(r["difficulty"] / 5)
        bar = "[" + "#" * min(bar_count, 15) + " " * (15 - min(bar_count, 15)) + "]"
        print(f"  {i:>3}. {r['boss_name']:<38} {int(r['hp']):>7}  "
              f"{r['hits_to_kill']:>5.1f}  {r['death_rate']:>6.1f}%  {dlc}  {bar}")

    print(f"\n  Easiest: {merged.iloc[0]['boss_name']}")
    print(f"  Hardest: {merged.iloc[-1]['boss_name']}")


# ── Step 5: Save build ────────────────────────────────────────────────────────

def step_save_build(build: Build):
    header("STEP 5 -- SAVE YOUR BUILD")
    row = fetch_weapon_row(build.weapon_name, build.upgrade_level)
    ar  = build.total_ar(row) if row is not None else 0.0

    name = input("  Name your build (e.g. 'bleed samurai lv80'): ").strip()
    if not name:
        print("  Skipped."); return

    with engine().begin() as conn:
        conn.execute(text("""
            INSERT INTO golden_order.fact_builds
                (build_name, class_name, character_level,
                 strength, dexterity, intelligence, faith, arcane,
                 weapon_name, upgrade_level, two_handing, calculated_ar)
            VALUES
                (:bn, :cn, :cl, :s, :d, :i, :f, :a, :w, :u, :t, :ar)
            ON CONFLICT (build_name) DO UPDATE SET
                class_name=EXCLUDED.class_name,
                character_level=EXCLUDED.character_level,
                strength=EXCLUDED.strength, dexterity=EXCLUDED.dexterity,
                intelligence=EXCLUDED.intelligence, faith=EXCLUDED.faith,
                arcane=EXCLUDED.arcane, weapon_name=EXCLUDED.weapon_name,
                upgrade_level=EXCLUDED.upgrade_level,
                two_handing=EXCLUDED.two_handing,
                calculated_ar=EXCLUDED.calculated_ar,
                saved_at=NOW()
        """), dict(
            bn=name, cn=build.class_name, cl=build.character_level,
            s=build.strength, d=build.dexterity, i=build.intelligence,
            f=build.faith, a=build.arcane,
            w=build.weapon_name, u=build.upgrade_level,
            t=build.two_handing, ar=ar
        ))
    print(f"  Build '{name}' saved! (AR: {ar:.1f})")


# ── Step 6: Compare saved builds ─────────────────────────────────────────────

def step_compare_builds():
    header("STEP 6 -- COMPARE SAVED BUILDS")
    saved = pd.read_sql(text("""
        SELECT build_name, class_name, character_level,
               strength, dexterity, intelligence, faith, arcane,
               weapon_name, upgrade_level, two_handing, calculated_ar,
               saved_at
        FROM golden_order.fact_builds
        ORDER BY calculated_ar DESC
    """), engine())

    if saved.empty:
        print("  No builds saved yet. Run the advisor and save a build first.")
        return

    print(f"\n  {'Build':<28} {'Class':<12} {'Lvl':>4}  {'Weapon':<28} {'AR':>8}")
    print("  " + "-" * 86)
    for _, r in saved.iterrows():
        print(f"  {r['build_name']:<28} {r['class_name']:<12} {r['character_level']:>4}  "
              f"{r['weapon_name']:<28} {r['calculated_ar']:>8.1f}")

    if len(saved) < 2:
        print("\n  Save at least 2 builds to compare in detail.")
        return

    names = saved["build_name"].tolist()
    b1_name = pick("Select first build to compare:", names)
    b2_name = pick("Select second build to compare:", [n for n in names if n != b1_name])

    b1 = saved[saved["build_name"] == b1_name].iloc[0]
    b2 = saved[saved["build_name"] == b2_name].iloc[0]

    print(f"\n  {'Attribute':<22} {b1_name[:20]:>20}  {b2_name[:20]:>20}")
    print("  " + "-" * 64)
    attrs = [
        ("Class",         "class_name"),
        ("Level",         "character_level"),
        ("Strength",      "strength"),
        ("Dexterity",     "dexterity"),
        ("Intelligence",  "intelligence"),
        ("Faith",         "faith"),
        ("Arcane",        "arcane"),
        ("Weapon",        "weapon_name"),
        ("Upgrade",       "upgrade_level"),
        ("Attack Rating", "calculated_ar"),
    ]
    for label, col in attrs:
        v1 = str(b1[col])
        v2 = str(b2[col])
        marker = " <<" if col == "calculated_ar" and float(b1[col]) > float(b2[col]) else (
                 " <<" if col == "calculated_ar" and float(b2[col]) > float(b1[col]) else "")
        # Put << next to winner
        v1_m = v1 + (" <<" if col == "calculated_ar" and float(b1[col]) >= float(b2[col]) else "")
        v2_m = v2 + (" <<" if col == "calculated_ar" and float(b2[col]) >  float(b1[col]) else "")
        print(f"  {label:<22} {v1_m:>20}  {v2_m:>20}")


# ── Main menu ─────────────────────────────────────────────────────────────────

def main():
    build = Build()

    while True:
        header("GOLDEN ORDER BUILD ADVISOR")
        print("\n  Current build:", end=" ")
        if build.weapon_name:
            print(f"{build.class_name} Lv{build.character_level} | "
                  f"{build.weapon_name} +{build.upgrade_level} | "
                  f"STR {build.strength} DEX {build.dexterity} "
                  f"INT {build.intelligence} FAI {build.faith} ARC {build.arcane}")
        else:
            print("(none yet)")

        print("""
  1. Set class + level + stats
  2. Set weapon
  3. View AR + soft cap advisor
  4. Boss gauntlet (rank all bosses for my build)
  5. Save current build
  6. Compare saved builds
  7. Full run (1 -> 2 -> 3 -> 4 -> save)
  0. Exit
""")
        choice = input("  Choose: ").strip()

        if choice == "1":
            build = step_class_and_level(build)
        elif choice == "2":
            build = step_weapon(build)
        elif choice == "3":
            if not build.weapon_name:
                print("  Set a weapon first (option 2).")
            else:
                step_ar_and_softcap(build)
        elif choice == "4":
            if not build.weapon_name:
                print("  Set a weapon first (option 2).")
            else:
                step_boss_gauntlet(build)
        elif choice == "5":
            if not build.weapon_name:
                print("  Set a weapon first (option 2).")
            else:
                step_save_build(build)
        elif choice == "6":
            step_compare_builds()
        elif choice == "7":
            build = step_class_and_level(build)
            build = step_weapon(build)
            step_ar_and_softcap(build)
            step_boss_gauntlet(build)
            step_save_build(build)
        elif choice == "0":
            print("\n  May the grace guide thee.\n")
            sys.exit(0)
        else:
            print("  Invalid choice.")

        input("\n  Press Enter to continue...")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\nExiting.")
        sys.exit(0)
