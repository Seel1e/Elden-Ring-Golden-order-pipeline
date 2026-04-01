"""
Tests for the Telemetry Generator (telemetry_generator.py)
===========================================================
Validates event schema, outcome distribution, and rate controls.
Does NOT require Kafka or any external service.
"""

import json
import time

import pytest

from src.streaming.telemetry_generator import TelemetryGenerator


@pytest.fixture(scope="module")
def generator():
    """Reuse one generator (loading weapon/boss pools is expensive)."""
    return TelemetryGenerator(seed=42)


@pytest.fixture(scope="module")
def sample_events(generator):
    """Generate 500 events for distribution tests."""
    return list(generator.stream(max_events=500, target_eps=0))


# ── Schema tests ──────────────────────────────────────────────────────────────

REQUIRED_KEYS = {
    "event_id", "event_timestamp", "player_id", "player_level",
    "archetype", "strength_stat", "dexterity_stat", "intelligence_stat",
    "faith_stat", "arcane_stat", "two_handing",
    "boss_id", "boss_name", "weapon_used", "upgrade_level",
    "outcome", "win_probability",
}


class TestEventSchema:
    def test_all_required_keys_present(self, sample_events):
        for event in sample_events:
            missing = REQUIRED_KEYS - set(event.keys())
            assert not missing, f"Event missing keys: {missing}"

    def test_event_is_json_serialisable(self, generator):
        event = generator.generate_event()
        json.dumps(event)   # should not raise

    def test_outcome_is_valid(self, sample_events):
        outcomes = {e["outcome"] for e in sample_events}
        assert outcomes <= {"victory", "death"}

    def test_player_level_in_range(self, sample_events):
        for e in sample_events:
            assert 1 <= e["player_level"] <= 150

    def test_stats_in_range(self, sample_events):
        stat_keys = ["strength_stat", "dexterity_stat", "intelligence_stat",
                     "faith_stat", "arcane_stat"]
        for e in sample_events:
            for k in stat_keys:
                assert 1 <= e[k] <= 99, f"{k}={e[k]} out of [1, 99]"

    def test_win_probability_in_range(self, sample_events):
        for e in sample_events:
            assert 0.0 <= e["win_probability"] <= 1.0

    def test_two_handing_is_bool(self, sample_events):
        for e in sample_events:
            assert isinstance(e["two_handing"], bool)

    def test_event_ids_are_unique(self, sample_events):
        ids = [e["event_id"] for e in sample_events]
        assert len(ids) == len(set(ids)), "Duplicate event_ids detected"


# ── Distribution tests ────────────────────────────────────────────────────────

class TestOutcomeDistribution:
    def test_both_outcomes_present(self, sample_events):
        """With 500 events and probability range [0.05, 0.95], both outcomes must appear."""
        outcomes = [e["outcome"] for e in sample_events]
        assert "victory" in outcomes
        assert "death" in outcomes

    def test_death_rate_is_nonzero(self, sample_events):
        deaths = sum(1 for e in sample_events if e["outcome"] == "death")
        assert deaths > 0

    def test_win_rate_nonzero(self, sample_events):
        victories = sum(1 for e in sample_events if e["outcome"] == "victory")
        assert victories > 0

    def test_all_archetypes_appear(self, sample_events):
        archetypes = {e["archetype"] for e in sample_events}
        expected = {"str", "dex", "quality", "int", "faith", "arcane", "hybrid"}
        assert archetypes == expected

    def test_two_handing_appears(self, sample_events):
        """With 500 events at 30% two-handing probability, we expect some True."""
        two_handers = sum(1 for e in sample_events if e["two_handing"])
        assert two_handers > 0


# ── Rate limiter test ─────────────────────────────────────────────────────────

class TestRateLimiting:
    def test_max_events_stops_generator(self, generator):
        events = list(generator.stream(max_events=10, target_eps=0))
        assert len(events) == 10

    def test_throughput_at_zero_limit(self, generator):
        """With no rate limit, 100 events should complete in under 5 seconds."""
        start = time.monotonic()
        events = list(generator.stream(max_events=100, target_eps=0))
        elapsed = time.monotonic() - start
        assert len(events) == 100
        assert elapsed < 5.0, f"100 events took {elapsed:.2f}s — generator too slow"


# ── Weapon/boss pool tests ────────────────────────────────────────────────────

class TestPools:
    def test_weapons_loaded(self, generator):
        assert len(generator.weapons) > 0

    def test_bosses_loaded(self, generator):
        assert len(generator.bosses) > 0

    def test_multiple_bosses_appear(self, sample_events):
        bosses = {e["boss_name"] for e in sample_events}
        assert len(bosses) > 5, "Expected variety in bosses encountered"

    def test_multiple_weapons_appear(self, sample_events):
        weapons = {e["weapon_used"] for e in sample_events}
        assert len(weapons) > 10, "Expected variety in weapons used"
