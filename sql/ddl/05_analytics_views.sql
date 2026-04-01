-- ════════════════════════════════════════════════════════════════════════════
-- Analytics Views  —  Phase 4: BI Layer
-- ════════════════════════════════════════════════════════════════════════════
-- These materialised/regular views power the Metabase dashboard.
-- Connect Metabase (or any BI tool) to the PostgreSQL warehouse and
-- point it at these views.
--
-- Dashboard 1: "Most Lethal Bosses" — highest player death rate
-- Dashboard 2: "Highest Win-Rate Weapon Builds" — per category + archetype
-- ════════════════════════════════════════════════════════════════════════════

SET search_path TO golden_order, public;

-- ════════════════════════════════════════════════════════════════════════════
-- View 1: Boss Lethality Leaderboard
-- ════════════════════════════════════════════════════════════════════════════
-- Shows every boss ranked by death rate (deaths / total encounters).
-- Also shows average player level and average AR at time of death.
-- ════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW golden_order.v_boss_lethality AS
SELECT
    f.boss_name,
    b.hp                                            AS boss_hp,
    b.is_dlc,
    b.drops_runes,
    COUNT(*)                                        AS total_encounters,
    COUNT(*) FILTER (WHERE NOT f.is_victory)        AS total_deaths,
    COUNT(*) FILTER (WHERE f.is_victory)            AS total_victories,
    ROUND(
        COUNT(*) FILTER (WHERE NOT f.is_victory)::NUMERIC
        / NULLIF(COUNT(*), 0) * 100, 2
    )                                               AS death_rate_pct,
    ROUND(AVG(f.player_level), 1)                   AS avg_player_level,
    ROUND(AVG(f.calculated_ar), 1)                  AS avg_ar_at_encounter,
    ROUND(AVG(f.calculated_ar) FILTER (WHERE NOT f.is_victory), 1)
                                                    AS avg_ar_on_death,
    ROUND(AVG(f.calculated_ar) FILTER (WHERE f.is_victory), 1)
                                                    AS avg_ar_on_victory,
    MAX(f.event_ts)                                 AS last_encounter_ts
FROM golden_order.fact_encounters f
LEFT JOIN golden_order.dim_bosses b ON f.boss_name = b.boss_name
GROUP BY f.boss_name, b.hp, b.is_dlc, b.drops_runes
HAVING COUNT(*) >= 10    -- exclude bosses with too few samples
ORDER BY death_rate_pct DESC;

COMMENT ON VIEW golden_order.v_boss_lethality IS
    'Boss lethality leaderboard — death rate, average AR at encounter, '
    'and sample counts. Powers the "Most Lethal Bosses" dashboard.';


-- ════════════════════════════════════════════════════════════════════════════
-- View 2: Weapon Win-Rate by Category and Archetype
-- ════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW golden_order.v_weapon_win_rates AS
SELECT
    f.weapon_used,
    f.upgrade_level,
    f.category,
    f.archetype,
    f.is_dlc,
    COUNT(*)                                        AS total_uses,
    COUNT(*) FILTER (WHERE f.is_victory)            AS victories,
    ROUND(
        COUNT(*) FILTER (WHERE f.is_victory)::NUMERIC
        / NULLIF(COUNT(*), 0) * 100, 2
    )                                               AS win_rate_pct,
    ROUND(AVG(f.calculated_ar), 1)                  AS avg_ar,
    ROUND(MAX(f.calculated_ar), 1)                  AS max_ar_seen,
    ROUND(AVG(f.player_level), 1)                   AS avg_player_level
FROM golden_order.fact_encounters f
GROUP BY f.weapon_used, f.upgrade_level, f.category, f.archetype, f.is_dlc
HAVING COUNT(*) >= 20
ORDER BY win_rate_pct DESC;

COMMENT ON VIEW golden_order.v_weapon_win_rates IS
    'Weapon win rates by archetype. Powers "Highest Win-Rate Weapon Builds" dashboard.';


-- ════════════════════════════════════════════════════════════════════════════
-- View 3: Soft-Cap Efficiency — Are Players Investing Past the Cap?
-- ════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW golden_order.v_stat_distribution AS
SELECT
    archetype,
    ROUND(AVG(strength_stat),    1) AS avg_str,
    ROUND(AVG(dexterity_stat),   1) AS avg_dex,
    ROUND(AVG(intelligence_stat),1) AS avg_int,
    ROUND(AVG(faith_stat),       1) AS avg_fai,
    ROUND(AVG(arcane_stat),      1) AS avg_arc,
    ROUND(AVG(calculated_ar),    1) AS avg_ar,
    COUNT(*)                        AS sample_count,
    -- Soft cap breach detection: how many players are past the caps?
    ROUND(
        COUNT(*) FILTER (WHERE strength_stat > 80)::NUMERIC / NULLIF(COUNT(*),0) * 100, 1
    ) AS pct_str_past_second_softcap,
    ROUND(
        COUNT(*) FILTER (WHERE dexterity_stat > 80)::NUMERIC / NULLIF(COUNT(*),0) * 100, 1
    ) AS pct_dex_past_second_softcap
FROM golden_order.fact_encounters
GROUP BY archetype
ORDER BY avg_ar DESC;


-- ════════════════════════════════════════════════════════════════════════════
-- View 4: Rolling 1-Hour Win Rate (Near-Real-Time)
-- ════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW golden_order.v_rolling_win_rate_1h AS
SELECT
    DATE_TRUNC('minute', event_ts)                  AS minute_bucket,
    COUNT(*)                                        AS encounters,
    COUNT(*) FILTER (WHERE is_victory)              AS victories,
    ROUND(
        COUNT(*) FILTER (WHERE is_victory)::NUMERIC
        / NULLIF(COUNT(*),0) * 100, 2
    )                                               AS win_rate_pct,
    ROUND(AVG(calculated_ar), 1)                    AS avg_ar
FROM golden_order.fact_encounters
WHERE event_ts >= NOW() - INTERVAL '1 hour'
GROUP BY DATE_TRUNC('minute', event_ts)
ORDER BY minute_bucket DESC;


-- ════════════════════════════════════════════════════════════════════════════
-- View 5: Top Weapons per Boss (Best Tools for Each Fight)
-- ════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW golden_order.v_best_weapons_per_boss AS
WITH ranked AS (
    SELECT
        boss_name,
        weapon_used,
        upgrade_level,
        category,
        COUNT(*)                                        AS uses,
        ROUND(
            COUNT(*) FILTER (WHERE is_victory)::NUMERIC
            / NULLIF(COUNT(*),0) * 100, 2
        )                                               AS win_rate_pct,
        ROUND(AVG(calculated_ar), 1)                    AS avg_ar,
        ROW_NUMBER() OVER (
            PARTITION BY boss_name
            ORDER BY
                COUNT(*) FILTER (WHERE is_victory)::NUMERIC
                / NULLIF(COUNT(*),0) DESC
        )                                               AS rank
    FROM golden_order.fact_encounters
    GROUP BY boss_name, weapon_used, upgrade_level, category
    HAVING COUNT(*) >= 15
)
SELECT * FROM ranked WHERE rank <= 5
ORDER BY boss_name, rank;

COMMENT ON VIEW golden_order.v_best_weapons_per_boss IS
    'Top 5 weapons by win rate for each boss. Requires ≥15 samples per weapon×boss pair.';
