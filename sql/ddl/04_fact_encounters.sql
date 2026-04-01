-- ════════════════════════════════════════════════════════════════════════════
-- fact_encounters  —  Player Encounter Fact Table
-- ════════════════════════════════════════════════════════════════════════════
-- Grain: one row per player encounter event (death or victory).
-- Populated by the Spark Structured Streaming consumer at near-real-time.
--
-- This is the hottest table in the warehouse — receives thousands of rows
-- per minute from the Spark streaming job.  Indexes are lean to keep
-- INSERT throughput high; BRIN index on event_ts supports time-range scans.
-- ════════════════════════════════════════════════════════════════════════════

SET search_path TO golden_order, public;

CREATE TABLE IF NOT EXISTS golden_order.fact_encounters (
    encounter_id    BIGSERIAL   PRIMARY KEY,
    event_id        TEXT        NOT NULL,       -- UUID from telemetry producer
    event_ts        TIMESTAMPTZ NOT NULL,       -- event time (watermarked)
    loaded_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    -- Player attributes at time of encounter
    player_id       TEXT        NOT NULL,
    player_level    SMALLINT,
    archetype       TEXT,                       -- str / dex / quality / int / faith / arcane
    strength_stat   SMALLINT,
    dexterity_stat  SMALLINT,
    intelligence_stat SMALLINT,
    faith_stat      SMALLINT,
    arcane_stat     SMALLINT,
    two_handing     BOOLEAN     NOT NULL DEFAULT FALSE,

    -- Boss reference
    boss_id         TEXT,
    boss_name       TEXT,
    boss_hp         INTEGER,

    -- Weapon used
    weapon_used     TEXT,
    upgrade_level   SMALLINT,
    category        TEXT,
    weight          NUMERIC(5, 1),
    is_dlc          BOOLEAN,

    -- The core metric: Attack Rating computed by the Scaling Engine UDF
    -- using THIS player's actual stat values (not a pre-computed average)
    calculated_ar   NUMERIC(8, 2),

    -- Outcome
    outcome         TEXT        NOT NULL CHECK (outcome IN ('victory', 'death')),
    is_victory      BOOLEAN     NOT NULL GENERATED ALWAYS AS (outcome = 'victory') STORED,
    win_probability NUMERIC(5, 4),   -- stored for model validation

    CONSTRAINT uq_fact_event_id UNIQUE (event_id)   -- dedup on re-delivery
);

-- ── Partitioning hint (PostgreSQL 15 declarative) ─────────────────────────
-- For production scale: partition by RANGE on event_ts (daily/monthly).
-- Example:
--   CREATE TABLE fact_encounters_2025_01 PARTITION OF fact_encounters
--   FOR VALUES FROM ('2025-01-01') TO ('2025-02-01');

-- ── Indexes ───────────────────────────────────────────────────────────────────

-- Time-series scans (dashboard "last 24 hours")
CREATE INDEX IF NOT EXISTS idx_fact_enc_event_ts_brin
    ON golden_order.fact_encounters USING BRIN (event_ts)
    WITH (pages_per_range = 128);

-- Most-lethal-bosses query
CREATE INDEX IF NOT EXISTS idx_fact_enc_boss_outcome
    ON golden_order.fact_encounters (boss_name, is_victory);

-- Weapon win-rate query
CREATE INDEX IF NOT EXISTS idx_fact_enc_weapon_outcome
    ON golden_order.fact_encounters (weapon_used, upgrade_level, is_victory);

-- AR distribution analysis
CREATE INDEX IF NOT EXISTS idx_fact_enc_ar
    ON golden_order.fact_encounters (calculated_ar);

-- Player journey lookup
CREATE INDEX IF NOT EXISTS idx_fact_enc_player
    ON golden_order.fact_encounters (player_id);

COMMENT ON TABLE golden_order.fact_encounters IS
    'Fact table: one row per player encounter event streamed from Kafka. '
    'calculated_ar is the true AR from the Scaling Engine UDF at stream processing time.';

COMMENT ON COLUMN golden_order.fact_encounters.calculated_ar IS
    'Attack Rating computed by _spark_udf_calculate_ar() using the player''s '
    'actual strength/dex/int/faith/arcane at encounter time. Accounts for '
    'soft caps at stat levels 60 and 80.';
