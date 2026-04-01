"""
Tests for the Scaling Engine (scaling_engine.py)
=================================================
These tests validate the soft-cap math, grade multipliers, two-handing bonus,
and the PySpark UDF wrapper using only the Python standard library and pytest
(no Spark or database required).
"""

import math
import pytest

from src.transformation.scaling_engine import (
    WeaponScaling,
    PlayerStats,
    calculate_attack_rating,
    marginal_ar_gain,
    soft_cap_report,
    _interpolate_correction,
    _spark_udf_calculate_ar,
    SOFT_CAP_CURVE,
    GRADE_MAX_MULTIPLIER,
    STR_HARD_CAP,
    TWO_HAND_STR_MULTIPLIER,
)


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture
def giant_crusher():
    """A high-base, S-STR scaling weapon (like Giant-Crusher in ER)."""
    return WeaponScaling(
        base_physical=155.0,
        scaling={"Str": "S", "Dex": "D"},
    )


@pytest.fixture
def moonveil():
    """A DEX/INT split-damage weapon (like Moonveil Katana)."""
    return WeaponScaling(
        base_physical=73.0,
        base_magic=87.0,
        scaling={"Str": "D", "Dex": "C", "Int": "B"},
    )


@pytest.fixture
def pure_dex_weapon():
    return WeaponScaling(
        base_physical=120.0,
        scaling={"Str": "-", "Dex": "A"},
    )


@pytest.fixture
def min_player():
    return PlayerStats(strength=10, dexterity=10, intelligence=9,
                       faith=9, arcane=9)


# ── Soft cap curve tests ──────────────────────────────────────────────────────

class TestSoftCapCurve:
    def test_stat_zero_returns_zero(self):
        assert _interpolate_correction(0, SOFT_CAP_CURVE) == 0.0

    def test_stat_99_returns_one(self):
        assert _interpolate_correction(99, SOFT_CAP_CURVE) == 1.0

    def test_first_soft_cap_at_60(self):
        c60 = _interpolate_correction(60, SOFT_CAP_CURVE)
        c61 = _interpolate_correction(61, SOFT_CAP_CURVE)
        # Slope should drop sharply at 60 → marginal gain at 61 < gain at 59
        c59 = _interpolate_correction(59, SOFT_CAP_CURVE)
        gain_before = c60 - c59   # gain from 59→60
        gain_after  = c61 - c60   # gain from 60→61
        assert gain_after < gain_before, (
            f"Expected diminished returns past 60 but gain_before={gain_before:.4f}, "
            f"gain_after={gain_after:.4f}"
        )

    def test_second_soft_cap_at_80(self):
        c79 = _interpolate_correction(79, SOFT_CAP_CURVE)
        c80 = _interpolate_correction(80, SOFT_CAP_CURVE)
        c81 = _interpolate_correction(81, SOFT_CAP_CURVE)
        gain_before = c80 - c79
        gain_after  = c81 - c80
        assert gain_after < gain_before

    def test_monotonically_increasing(self):
        """Correction must never decrease as stat goes up."""
        prev = 0.0
        for stat in range(0, 100):
            curr = _interpolate_correction(stat, SOFT_CAP_CURVE)
            assert curr >= prev, f"Correction dropped at stat={stat}"
            prev = curr

    def test_clamped_below_zero(self):
        assert _interpolate_correction(-5, SOFT_CAP_CURVE) == 0.0

    def test_clamped_above_99(self):
        assert _interpolate_correction(150, SOFT_CAP_CURVE) == 1.0


# ── Grade multiplier tests ────────────────────────────────────────────────────

class TestGradeMultipliers:
    def test_s_grade_is_highest(self):
        assert GRADE_MAX_MULTIPLIER["S"] > GRADE_MAX_MULTIPLIER["A"]

    def test_grade_ordering(self):
        grades = ["S", "A", "B", "C", "D", "E"]
        mults = [GRADE_MAX_MULTIPLIER[g] for g in grades]
        assert mults == sorted(mults, reverse=True), "Grades must be ordered S > A > B > C > D > E"

    def test_no_scaling_grade(self):
        assert GRADE_MAX_MULTIPLIER["-"] == 0.0
        assert GRADE_MAX_MULTIPLIER[""] == 0.0


# ── AR calculation tests ──────────────────────────────────────────────────────

class TestCalculateAR:
    def test_base_only_no_scaling(self):
        """A weapon with no scaling grades should return exactly base damage."""
        weapon = WeaponScaling(base_physical=100.0, scaling={})
        player = PlayerStats(strength=99, dexterity=99, intelligence=99,
                             faith=99, arcane=99)
        ar = calculate_attack_rating(weapon, player)
        assert ar["physical"] == 100.0
        assert ar["total"] == 100.0

    def test_ar_increases_with_stat(self, giant_crusher):
        """Higher STR should always produce higher AR for an S-STR weapon."""
        ars = []
        for str_val in [18, 30, 50, 60, 80, 99]:
            p = PlayerStats(strength=str_val, dexterity=14, intelligence=9,
                            faith=9, arcane=9)
            ars.append(calculate_attack_rating(giant_crusher, p)["total"])
        assert ars == sorted(ars), "AR should increase monotonically with STR"

    def test_ar_at_stat_18_minimum(self, giant_crusher):
        """Stats below 18 should contribute no scaling bonus (per soft cap table)."""
        p_below = PlayerStats(strength=17, dexterity=14, intelligence=9,
                              faith=9, arcane=9)
        p_at    = PlayerStats(strength=18, dexterity=14, intelligence=9,
                              faith=9, arcane=9)
        ar_below = calculate_attack_rating(giant_crusher, p_below)["total"]
        ar_at    = calculate_attack_rating(giant_crusher, p_at)["total"]
        # Both should be equal (zero scaling below the floor)
        assert ar_below == ar_at

    def test_split_damage_weapon(self, moonveil):
        """INT-heavy player should get significant magic damage from Moonveil."""
        int_heavy = PlayerStats(strength=12, dexterity=18, intelligence=60,
                                faith=9, arcane=9)
        str_heavy = PlayerStats(strength=60, dexterity=14, intelligence=9,
                                faith=9, arcane=9)
        ar_int = calculate_attack_rating(moonveil, int_heavy)
        ar_str = calculate_attack_rating(moonveil, str_heavy)
        assert ar_int["magic"] > ar_str["magic"], "INT build should have higher magic AR"
        assert ar_int["total"] > ar_str["total"], "INT build should win on Moonveil"

    def test_ar_dict_keys(self, giant_crusher, min_player):
        ar = calculate_attack_rating(giant_crusher, min_player)
        assert set(ar.keys()) == {"physical", "magic", "fire", "lightning", "holy", "total"}

    def test_total_equals_sum_of_components(self, moonveil):
        p = PlayerStats(strength=12, dexterity=18, intelligence=60,
                        faith=9, arcane=9)
        ar = calculate_attack_rating(moonveil, p)
        expected = round(ar["physical"] + ar["magic"] + ar["fire"]
                         + ar["lightning"] + ar["holy"], 2)
        assert ar["total"] == expected

    def test_zero_base_damage_gives_zero_ar(self):
        """A weapon with 0 base damage should always produce 0 AR."""
        weapon = WeaponScaling(base_physical=0.0, scaling={"Str": "S"})
        player = PlayerStats(strength=99, dexterity=14, intelligence=9,
                             faith=9, arcane=9)
        ar = calculate_attack_rating(weapon, player)
        assert ar["physical"] == 0.0


# ── Two-handing tests ─────────────────────────────────────────────────────────

class TestTwoHanding:
    def test_two_handing_increases_effective_str(self):
        player = PlayerStats(strength=40, dexterity=14, intelligence=9,
                             faith=9, arcane=9, two_handing=True)
        expected_eff = min(math.floor(40 * TWO_HAND_STR_MULTIPLIER), STR_HARD_CAP)
        assert player.effective_strength() == expected_eff

    def test_two_handing_caps_at_99(self):
        player = PlayerStats(strength=80, dexterity=14, intelligence=9,
                             faith=9, arcane=9, two_handing=True)
        assert player.effective_strength() <= STR_HARD_CAP

    def test_two_handing_boosts_ar_for_str_weapon(self, giant_crusher):
        one_hand = PlayerStats(strength=40, dexterity=14, intelligence=9,
                               faith=9, arcane=9, two_handing=False)
        two_hand = PlayerStats(strength=40, dexterity=14, intelligence=9,
                               faith=9, arcane=9, two_handing=True)
        ar_1h = calculate_attack_rating(giant_crusher, one_hand)["total"]
        ar_2h = calculate_attack_rating(giant_crusher, two_hand)["total"]
        assert ar_2h > ar_1h, "Two-handing should boost AR for S-STR weapon at STR 40"

    def test_two_handing_no_effect_on_pure_dex_weapon(self, pure_dex_weapon):
        """Two-handing only affects STR, so a pure DEX weapon should be unaffected."""
        one_hand = PlayerStats(strength=12, dexterity=60, intelligence=9,
                               faith=9, arcane=9, two_handing=False)
        two_hand = PlayerStats(strength=12, dexterity=60, intelligence=9,
                               faith=9, arcane=9, two_handing=True)
        ar_1h = calculate_attack_rating(pure_dex_weapon, one_hand)["total"]
        ar_2h = calculate_attack_rating(pure_dex_weapon, two_hand)["total"]
        # May differ slightly if weapon has any STR scaling, but pure DEX: exact same
        assert abs(ar_2h - ar_1h) < 1.0


# ── Marginal gain tests ───────────────────────────────────────────────────────

class TestMarginalGain:
    def test_marginal_gain_diminishes_past_first_cap(self, giant_crusher):
        p_before_cap = PlayerStats(strength=58, dexterity=14, intelligence=9,
                                   faith=9, arcane=9)
        p_past_cap   = PlayerStats(strength=62, dexterity=14, intelligence=9,
                                   faith=9, arcane=9)
        gain_before = marginal_ar_gain(giant_crusher, p_before_cap, "Str")
        gain_after  = marginal_ar_gain(giant_crusher, p_past_cap, "Str")
        assert gain_before > gain_after, (
            f"Expected lower marginal gain past soft cap: "
            f"before={gain_before:.3f}, after={gain_after:.3f}"
        )

    def test_marginal_gain_further_diminishes_past_second_cap(self, giant_crusher):
        p_past_first  = PlayerStats(strength=65, dexterity=14, intelligence=9,
                                    faith=9, arcane=9)
        p_past_second = PlayerStats(strength=85, dexterity=14, intelligence=9,
                                    faith=9, arcane=9)
        gain_first  = marginal_ar_gain(giant_crusher, p_past_first,  "Str")
        gain_second = marginal_ar_gain(giant_crusher, p_past_second, "Str")
        assert gain_first > gain_second

    def test_marginal_gain_wrong_stat_key_returns_zero(self, giant_crusher, min_player):
        gain = marginal_ar_gain(giant_crusher, min_player, "BadStat")
        assert gain == 0.0


# ── Soft cap report tests ─────────────────────────────────────────────────────

class TestSoftCapReport:
    def test_report_has_99_rows(self, giant_crusher):
        report = soft_cap_report(giant_crusher, "Str")
        assert len(report) == 99

    def test_report_ar_monotonically_increasing(self, giant_crusher):
        report = soft_cap_report(giant_crusher, "Str")
        ars = [r["ar"] for r in report]
        assert ars == sorted(ars), "AR must monotonically increase in soft cap report"

    def test_past_first_cap_flag(self, giant_crusher):
        report = soft_cap_report(giant_crusher, "Str")
        assert not report[59]["is_past_first_cap"]   # stat=60 → exactly at cap
        assert report[60]["is_past_first_cap"]        # stat=61 → past cap

    def test_past_second_cap_flag(self, giant_crusher):
        report = soft_cap_report(giant_crusher, "Str")
        assert not report[79]["is_past_second_cap"]
        assert report[80]["is_past_second_cap"]


# ── PySpark UDF wrapper tests ─────────────────────────────────────────────────

class TestSparkUDF:
    def test_udf_matches_native_calculation(self, giant_crusher):
        """The Spark UDF must produce the same result as the native Python call."""
        player = PlayerStats(strength=60, dexterity=14, intelligence=9,
                             faith=9, arcane=9)
        native_ar = calculate_attack_rating(giant_crusher, player)["total"]
        udf_ar = _spark_udf_calculate_ar(
            155.0, 0.0, 0.0, 0.0, 0.0,
            "S", "D", "-", "-", "-",
            60, 14, 9, 9, 9, False,
        )
        assert abs(udf_ar - native_ar) < 0.01, (
            f"UDF result {udf_ar:.2f} diverges from native {native_ar:.2f}"
        )

    def test_udf_handles_none_inputs_gracefully(self):
        """None inputs should not raise — return 0 base damage."""
        result = _spark_udf_calculate_ar(
            None, None, None, None, None,
            None, None, None, None, None,
            10, 10, 9, 9, 9, False,
        )
        assert isinstance(result, float)
        assert result >= 0.0

    def test_udf_two_handing(self):
        result_1h = _spark_udf_calculate_ar(
            155.0, 0.0, 0.0, 0.0, 0.0,
            "S", "-", "-", "-", "-",
            40, 14, 9, 9, 9, False,
        )
        result_2h = _spark_udf_calculate_ar(
            155.0, 0.0, 0.0, 0.0, 0.0,
            "S", "-", "-", "-", "-",
            40, 14, 9, 9, 9, True,
        )
        assert result_2h > result_1h
