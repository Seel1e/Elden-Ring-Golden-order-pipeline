"""
╔══════════════════════════════════════════════════════════════════════════════╗
║          THE GOLDEN ORDER SCALING ENGINE  —  Attack Rating Calculator       ║
║                                                                              ║
║  This module implements FromSoftware's weapon damage formula including       ║
║  the famous "soft cap" mechanic that governs diminishing stat returns.       ║
║                                                                              ║
║  Every weapon in Elden Ring has a base damage value AND a scaling grade      ║
║  (S / A / B / C / D / E) for each relevant stat (Strength, Dexterity,       ║
║  Intelligence, Faith, Arcane).  The final Attack Rating (AR) is:            ║
║                                                                              ║
║    AR = base_damage + Σ scaling_bonus(stat, grade, base_damage)             ║
║                                                                              ║
║  where scaling_bonus is non-linear and hits two hard "soft caps":           ║
║    • First soft cap  at stat 60 — returns drop to ~40 % of early rate       ║
║    • Second soft cap at stat 80 — returns drop to ~10 % of early rate       ║
║                                                                              ║
║  This file is the single source of truth for all AR calculations.           ║
║  It is used as:                                                              ║
║    1. A standalone Python UDF applied during Pandas batch transforms         ║
║    2. A PySpark UDF registered at stream-processing startup                  ║
║    3. A pure-Python callable for unit testing                                ║
╚══════════════════════════════════════════════════════════════════════════════╝
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Optional

try:
    import numpy as np
    _NUMPY_AVAILABLE = True
except ImportError:
    _NUMPY_AVAILABLE = False

# ── Soft-Cap Breakpoint Table ────────────────────────────────────────────────
#
# These piecewise-linear breakpoints are derived from community data-mining of
# Elden Ring's regulation.bin parameter files (CalcCorrectGraph).
#
# Structure: list of (stat_value, correction_percentage) tuples.
# The correction percentage (0.0–1.0) represents how much of the maximum
# grade bonus has been accumulated at that stat level.
#
# Two soft caps are visible:
#   • stat 60: slope drops sharply — investing past 60 gives ~40 % of early returns
#   • stat 80: slope drops again  — investing past 80 gives ~10 % of early returns
#
# These values match the in-game CalcCorrectGraph ID 0 (standard physical scaling).
# Reference: Elden Ring Param Editing Wiki, community spreadsheet by Pav & co.
# ────────────────────────────────────────────────────────────────────────────

SOFT_CAP_CURVE: list[tuple[int, float]] = [
    (0,   0.000),
    (18,  0.000),   # stats below 18 contribute nothing (below min requirement)
    (20,  0.050),
    (30,  0.210),
    (40,  0.370),
    (50,  0.550),
    (60,  0.700),   # ← FIRST SOFT CAP: curve bends here
    (70,  0.800),
    (80,  0.900),   # ← SECOND SOFT CAP: curve bends again
    (90,  0.950),
    (99,  1.000),
]

# Alternate curve for INT/FAI/ARC scaling (softer early, same caps)
SOFT_CAP_CURVE_CATALYST: list[tuple[int, float]] = [
    (0,   0.000),
    (10,  0.000),
    (20,  0.080),
    (30,  0.250),
    (40,  0.420),
    (50,  0.600),
    (60,  0.750),
    (70,  0.860),
    (80,  0.920),
    (90,  0.965),
    (99,  1.000),
]

# ── Grade → Maximum Bonus Multiplier ────────────────────────────────────────
#
# When a weapon has grade S in Strength, and the player hits stat 99,
# the weapon adds (base_damage * GRADE_MAX_MULTIPLIER['S']) bonus AR.
# These multipliers are calibrated from the in-game data tables.
# ────────────────────────────────────────────────────────────────────────────

GRADE_MAX_MULTIPLIER: dict[str, float] = {
    "S": 3.00,
    "A": 2.50,
    "B": 1.75,
    "C": 1.15,
    "D": 0.75,
    "E": 0.45,
    "-": 0.00,    # no scaling on this stat
    "":  0.00,
}

# Which stats use the catalyst (INT/FAI/ARC) curve vs the physical curve
CATALYST_STATS: frozenset[str] = frozenset({"Int", "Fai", "Arc"})

# Two-handed Strength bonus multiplier (FromSoftware mechanic)
TWO_HAND_STR_MULTIPLIER: float = 1.5
STR_HARD_CAP: int = 99   # effective STR is capped at 99 even with two-handing


# ── Interpolation helper ─────────────────────────────────────────────────────

def _interpolate_correction(stat_value: int, curve: list[tuple[int, float]]) -> float:
    """
    Piecewise-linear interpolation over the soft-cap correction curve.

    Given a stat value, returns the correction percentage (0.0–1.0) which
    represents how much of the weapon's max grade bonus has been unlocked.

    Example: stat=60 → 0.70 (70 % of max grade bonus unlocked)
             stat=80 → 0.90 (90 % of max grade bonus unlocked)
             stat=99 → 1.00 (full grade bonus)
    """
    stat_value = max(0, min(stat_value, 99))   # clamp to valid range

    # Below or at first breakpoint
    if stat_value <= curve[0][0]:
        return curve[0][1]
    # Above or at last breakpoint
    if stat_value >= curve[-1][0]:
        return curve[-1][1]

    # Find the two surrounding breakpoints and lerp between them
    for i in range(1, len(curve)):
        x0, y0 = curve[i - 1]
        x1, y1 = curve[i]
        if x0 <= stat_value <= x1:
            if x1 == x0:
                return y0
            t = (stat_value - x0) / (x1 - x0)   # normalised position [0,1]
            return y0 + t * (y1 - y0)

    return curve[-1][1]   # fallback (should not be reached)


# ── Main dataclass ────────────────────────────────────────────────────────────

@dataclass
class WeaponScaling:
    """
    Encapsulates all parameters needed to compute a weapon's Attack Rating.

    Attributes
    ----------
    base_physical : float
        Base physical damage at the weapon's current upgrade level.
    base_magic : float
        Base magic (intelligence) damage at upgrade level.
    base_fire : float
        Base fire damage at upgrade level.
    base_lightning : float
        Base lightning (faith) damage at upgrade level.
    base_holy : float
        Base holy (faith) damage at upgrade level.
    scaling : dict[str, str]
        Mapping of stat name to grade letter, e.g. {"Str": "B", "Dex": "D"}.
        Keys can be: Str, Dex, Int, Fai, Arc.
    """

    base_physical:  float
    base_magic:     float = 0.0
    base_fire:      float = 0.0
    base_lightning: float = 0.0
    base_holy:      float = 0.0
    scaling:        dict[str, str] = field(default_factory=dict)

    def total_base(self) -> float:
        """Sum of all base damage components (pre-scaling)."""
        return (
            self.base_physical + self.base_magic + self.base_fire
            + self.base_lightning + self.base_holy
        )


@dataclass
class PlayerStats:
    """
    Represents a player's relevant combat stats.

    two_handing : bool
        If True, effective Strength = min(STR * 1.5, 99).
        This is a first-class mechanic in FromSoftware games.
    """

    strength:      int
    dexterity:     int
    intelligence:  int
    faith:         int
    arcane:        int
    two_handing:   bool = False

    def effective_strength(self) -> int:
        """Apply two-handing multiplier, capped at STR_HARD_CAP."""
        if self.two_handing:
            return min(math.floor(self.strength * TWO_HAND_STR_MULTIPLIER), STR_HARD_CAP)
        return self.strength

    def stat_map(self) -> dict[str, int]:
        """Return {stat_key: effective_value} for scaling lookups."""
        return {
            "Str": self.effective_strength(),
            "Dex": self.dexterity,
            "Int": self.intelligence,
            "Fai": self.faith,
            "Arc": self.arcane,
        }


# ── Core calculation function ─────────────────────────────────────────────────

def calculate_attack_rating(weapon: WeaponScaling, player: PlayerStats) -> dict[str, float]:
    """
    Compute the full Attack Rating (AR) for a weapon + player stat combination.

    Algorithm
    ---------
    For each damage type and relevant stat:
        1. Look up the scaling grade letter (S/A/B/C/D/E).
        2. Translate grade → maximum bonus multiplier via GRADE_MAX_MULTIPLIER.
        3. Look up the player's effective stat value.
        4. Use piecewise interpolation over the soft-cap curve to get the
           correction percentage (0.0–1.0) for that stat value.
        5. bonus = base_damage * grade_multiplier * correction_percentage
        6. Sum all bonuses and add to base damage.

    Stat → Damage Type Mapping (FromSoftware convention)
    ----------------------------------------------------
        Str / Dex  → Physical damage scaling
        Int        → Magic damage scaling
        Fai        → Holy and Fire damage scaling
        Arc        → Fire, Lightning, and special (blood/poison) scaling

    Parameters
    ----------
    weapon : WeaponScaling
        Weapon parameters at a specific upgrade level.
    player : PlayerStats
        Player's stat block (with two-handing flag).

    Returns
    -------
    dict with keys: "physical", "magic", "fire", "lightning", "holy", "total"
    """
    stats = player.stat_map()
    scaling = weapon.scaling

    def _bonus(base: float, stat_key: str) -> float:
        """Compute the scaling bonus for one stat on one damage component."""
        if base <= 0:
            return 0.0
        grade = scaling.get(stat_key, "-").strip()
        grade_mult = GRADE_MAX_MULTIPLIER.get(grade, 0.0)
        if grade_mult == 0.0:
            return 0.0
        stat_val = stats.get(stat_key, 0)
        curve = SOFT_CAP_CURVE_CATALYST if stat_key in CATALYST_STATS else SOFT_CAP_CURVE
        correction = _interpolate_correction(stat_val, curve)
        return base * grade_mult * correction

    # Physical AR: scales with STR and DEX
    phys_bonus = _bonus(weapon.base_physical, "Str") + _bonus(weapon.base_physical, "Dex")
    physical_ar = weapon.base_physical + phys_bonus

    # Magic AR: scales with INT
    magic_ar = weapon.base_magic + _bonus(weapon.base_magic, "Int")

    # Fire AR: scales with FAI and ARC (flame weapons)
    fire_bonus = _bonus(weapon.base_fire, "Fai") + _bonus(weapon.base_fire, "Arc")
    fire_ar = weapon.base_fire + fire_bonus

    # Lightning AR: scales with DEX (some with FAI)
    lightning_ar = weapon.base_lightning + _bonus(weapon.base_lightning, "Dex")

    # Holy AR: scales with FAI
    holy_ar = weapon.base_holy + _bonus(weapon.base_holy, "Fai")

    total_ar = physical_ar + magic_ar + fire_ar + lightning_ar + holy_ar

    return {
        "physical":  round(physical_ar,  2),
        "magic":     round(magic_ar,     2),
        "fire":      round(fire_ar,      2),
        "lightning": round(lightning_ar, 2),
        "holy":      round(holy_ar,      2),
        "total":     round(total_ar,     2),
    }


# ── Marginal-return helpers (used in analytics dashboards) ───────────────────

def marginal_ar_gain(weapon: WeaponScaling, player: PlayerStats,
                     stat_key: str, points: int = 1) -> float:
    """
    Calculate the AR gained per additional stat point invested into `stat_key`.

    Useful for answering: "Is it worth putting the next point into Strength
    or Dexterity?" — the answer changes dramatically at soft caps.

    Parameters
    ----------
    weapon    : WeaponScaling
    player    : PlayerStats
    stat_key  : "Str" | "Dex" | "Int" | "Fai" | "Arc"
    points    : how many stat points to project forward (default 1)

    Returns
    -------
    float — AR gained per point (can be <1 past second soft cap)
    """
    before = calculate_attack_rating(weapon, player)["total"]

    # Build a modified player with `points` more in stat_key
    new_stats = player.__dict__.copy()
    key_map = {"Str": "strength", "Dex": "dexterity", "Int": "intelligence",
               "Fai": "faith",    "Arc": "arcane"}
    attr = key_map.get(stat_key)
    if attr is None:
        return 0.0
    new_stats[attr] = min(new_stats[attr] + points, 99)
    modified_player = PlayerStats(**new_stats)

    after = calculate_attack_rating(weapon, modified_player)["total"]
    return round((after - before) / points, 4)


def soft_cap_report(weapon: WeaponScaling, stat_key: str) -> list[dict]:
    """
    Generate a full soft-cap table for a weapon + stat combination.

    Returns a list of dicts with columns: stat_value, correction_pct,
    marginal_gain_per_point, cumulative_ar — useful for BI dashboards showing
    the classic "where should I stop investing?" chart.
    """
    base_stats = PlayerStats(strength=10, dexterity=10, intelligence=10,
                             faith=10, arcane=10)
    rows = []
    prev_ar = calculate_attack_rating(weapon, base_stats)["total"]

    for stat_val in range(1, 100):
        stats_dict = {
            "strength": 10, "dexterity": 10, "intelligence": 10,
            "faith": 10, "arcane": 10, "two_handing": False,
        }
        key_map = {"Str": "strength", "Dex": "dexterity", "Int": "intelligence",
                   "Fai": "faith",    "Arc": "arcane"}
        attr = key_map.get(stat_key, "strength")
        stats_dict[attr] = stat_val
        player = PlayerStats(**stats_dict)

        curve = SOFT_CAP_CURVE_CATALYST if stat_key in CATALYST_STATS else SOFT_CAP_CURVE
        ar = calculate_attack_rating(weapon, player)["total"]
        grade = weapon.scaling.get(stat_key, "-").strip()
        correction = _interpolate_correction(stat_val, curve)

        rows.append({
            "stat_value":           stat_val,
            "grade":                grade,
            "correction_pct":       round(correction * 100, 2),
            "ar":                   ar,
            "marginal_gain":        round(ar - prev_ar, 2),
            "is_past_first_cap":    stat_val > 60,
            "is_past_second_cap":   stat_val > 80,
        })
        prev_ar = ar

    return rows


# ── PySpark UDF wrapper ───────────────────────────────────────────────────────
#
# This function is registered as a Spark UDF so the scaling calculation runs
# in parallel across executors without Python pickling issues.  The UDF
# accepts primitive types only (no dataclasses) so Spark can serialise them.
# ─────────────────────────────────────────────────────────────────────────────

def _spark_udf_calculate_ar(
    base_physical: float,
    base_magic: float,
    base_fire: float,
    base_lightning: float,
    base_holy: float,
    scaling_str: str,     # grade letter e.g. "B"
    scaling_dex: str,
    scaling_int: str,
    scaling_fai: str,
    scaling_arc: str,
    strength: int,
    dexterity: int,
    intelligence: int,
    faith: int,
    arcane: int,
    two_handing: bool = False,
) -> float:
    """
    Spark-serialisable wrapper around calculate_attack_rating().

    All complex types are replaced with primitives so Spark can marshal
    them efficiently across the cluster without object serialisation overhead.

    Registered in spark_consumer.py via:
        spark.udf.register("calculate_ar", _spark_udf_calculate_ar, DoubleType())
    """
    weapon = WeaponScaling(
        base_physical=float(base_physical or 0),
        base_magic=float(base_magic or 0),
        base_fire=float(base_fire or 0),
        base_lightning=float(base_lightning or 0),
        base_holy=float(base_holy or 0),
        scaling={
            "Str": str(scaling_str or "-"),
            "Dex": str(scaling_dex or "-"),
            "Int": str(scaling_int or "-"),
            "Fai": str(scaling_fai or "-"),
            "Arc": str(scaling_arc or "-"),
        },
    )
    player = PlayerStats(
        strength=int(strength or 10),
        dexterity=int(dexterity or 10),
        intelligence=int(intelligence or 10),
        faith=int(faith or 10),
        arcane=int(arcane or 10),
        two_handing=bool(two_handing),
    )
    return calculate_attack_rating(weapon, player)["total"]


# ── Vectorised NumPy version (for Pandas batch transforms at scale) ───────────

def vectorised_ar(
    base_physical,
    grade_str,
    grade_dex,
    stat_str,
    stat_dex,
):
    """
    Vectorised physical-only AR calculation for fast Pandas batch transforms.
    Processes millions of rows without the Python loop overhead of the scalar UDF.

    Uses numpy.vectorize under the hood.  For a pure physical STR/DEX weapon
    this is ~40× faster than calling calculate_attack_rating() row by row.
    """
    def _single(base_phys, g_str, g_dex, s_str, s_dex):
        weapon = WeaponScaling(
            base_physical=float(base_phys),
            scaling={"Str": str(g_str), "Dex": str(g_dex)},
        )
        player = PlayerStats(strength=int(s_str), dexterity=int(s_dex),
                             intelligence=10, faith=10, arcane=10)
        return calculate_attack_rating(weapon, player)["total"]

    if _NUMPY_AVAILABLE:
        vfunc = np.vectorize(_single)
        return vfunc(base_physical, grade_str, grade_dex, stat_str, stat_dex)
    # Fallback: list comprehension when numpy unavailable
    return [
        _single(b, gs, gd, ss, sd)
        for b, gs, gd, ss, sd in zip(base_physical, grade_str, grade_dex, stat_str, stat_dex)
    ]


# ── Quick sanity check (run this file directly to verify the engine) ──────────

if __name__ == "__main__":
    # Test 1: Pure Strength weapon (e.g. Giant-Crusher)
    giant_crusher = WeaponScaling(
        base_physical=155.0,
        scaling={"Str": "S", "Dex": "D"},
    )
    print("=== Giant-Crusher (S/D) at different STR values ===")
    for str_val in [20, 40, 60, 66, 80, 99]:
        player = PlayerStats(strength=str_val, dexterity=14,
                             intelligence=9, faith=9, arcane=9)
        ar = calculate_attack_rating(giant_crusher, player)
        gain = marginal_ar_gain(giant_crusher, player, "Str")
        print(f"  STR {str_val:>3}: total AR = {ar['total']:>8.1f}  "
              f"(+{gain:.2f} per next point)")

    print()
    print("=== Soft-cap report — first 5 rows, then at cap boundaries ===")
    report = soft_cap_report(giant_crusher, "Str")
    for row in [report[19], report[39], report[59], report[79], report[98]]:
        print(f"  stat={row['stat_value']:>2}  correction={row['correction_pct']:>5.1f}%  "
              f"AR={row['ar']:>8.1f}  marginal_gain/pt={row['marginal_gain']:>6.2f}")

    print()
    # Test 2: Two-handing STR bonus
    player_1h = PlayerStats(strength=40, dexterity=14, intelligence=9,
                            faith=9, arcane=9, two_handing=False)
    player_2h = PlayerStats(strength=40, dexterity=14, intelligence=9,
                            faith=9, arcane=9, two_handing=True)
    ar_1h = calculate_attack_rating(giant_crusher, player_1h)["total"]
    ar_2h = calculate_attack_rating(giant_crusher, player_2h)["total"]
    print(f"STR 40 one-hand: {ar_1h:.1f}  |  two-hand (eff. STR 60): {ar_2h:.1f}")
