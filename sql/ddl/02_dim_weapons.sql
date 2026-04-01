-- ════════════════════════════════════════════════════════════════════════════
-- dim_weapons  —  Weapon Dimension Table
-- ════════════════════════════════════════════════════════════════════════════
-- Grain: one row per (weapon_name, upgrade_level, upgrade_type).
--
-- Pre-computed AR columns
-- -----------------------
-- Rather than running the Scaling Engine UDF at BI query time, we pre-compute
-- AR for the most common player archetypes and store them as plain numerics.
-- This means a BI tool like Metabase can sort by "ar_str_build_60" instantly
-- without invoking any Python.
-- ════════════════════════════════════════════════════════════════════════════

SET search_path TO golden_order, public;

CREATE TABLE IF NOT EXISTS golden_order.dim_weapons (
    weapon_key          BIGSERIAL PRIMARY KEY,

    -- Natural key (used for upsert conflict detection)
    weapon_name         TEXT        NOT NULL,
    upgrade_level       SMALLINT    NOT NULL DEFAULT 0,
    upgrade_type        TEXT        NOT NULL DEFAULT 'Standard',

    -- Weapon classification
    category            TEXT,                   -- e.g. "Greatswords", "Katanas"
    damage_type         TEXT,                   -- e.g. "Standard/Pierce"
    weight              NUMERIC(5, 1),
    skill               TEXT,
    fp_cost             NUMERIC(6, 1),
    is_dlc              BOOLEAN     NOT NULL DEFAULT FALSE,

    -- Requirements (minimum stats to wield)
    req_str             SMALLINT    NOT NULL DEFAULT 0,
    req_dex             SMALLINT    NOT NULL DEFAULT 0,
    req_int             SMALLINT    NOT NULL DEFAULT 0,
    req_fai             SMALLINT    NOT NULL DEFAULT 0,
    req_arc             SMALLINT    NOT NULL DEFAULT 0,

    -- Base damage at this upgrade level (pre-scaling)
    base_physical       NUMERIC(7, 2) NOT NULL DEFAULT 0,
    base_magic          NUMERIC(7, 2) NOT NULL DEFAULT 0,
    base_fire           NUMERIC(7, 2) NOT NULL DEFAULT 0,
    base_lightning      NUMERIC(7, 2) NOT NULL DEFAULT 0,
    base_holy           NUMERIC(7, 2) NOT NULL DEFAULT 0,

    -- Stat scaling grades (S / A / B / C / D / E / -)
    scale_str           CHAR(1)     NOT NULL DEFAULT '-',
    scale_dex           CHAR(1)     NOT NULL DEFAULT '-',
    scale_int           CHAR(1)     NOT NULL DEFAULT '-',
    scale_fai           CHAR(1)     NOT NULL DEFAULT '-',
    scale_arc           CHAR(1)     NOT NULL DEFAULT '-',

    -- Damage reduction (shield/weapon guard stats)
    dr_physical         NUMERIC(5, 2),

    -- ── Pre-computed Attack Rating columns ───────────────────────────────
    -- Each column is the total AR at the named build profile.
    -- Calculated by the Scaling Engine during dim_builder.py Phase 2 run.
    ar_str_build_60     NUMERIC(8, 2),   -- STR 60 / DEX 15 / rest min
    ar_dex_build_60     NUMERIC(8, 2),   -- STR 13 / DEX 60 / rest min
    ar_quality_build_60 NUMERIC(8, 2),   -- STR 60 / DEX 60 / rest min
    ar_int_build_60     NUMERIC(8, 2),   -- INT 60 / rest min
    ar_faith_build_60   NUMERIC(8, 2),   -- FAI 60 / rest min
    ar_profiles_json    JSONB,           -- full AR map across all 10 profiles

    -- Metadata
    loaded_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    -- Unique constraint: drives the ON CONFLICT upsert in dim_builder.py
    CONSTRAINT uq_dim_weapons_natural UNIQUE (weapon_name, upgrade_level, upgrade_type)
);

-- ── Indexes ───────────────────────────────────────────────────────────────────

-- Category filter (most BI queries filter by weapon type)
CREATE INDEX IF NOT EXISTS idx_dim_weapons_category
    ON golden_order.dim_weapons (category);

-- AR sort (most BI queries ORDER BY one of these)
CREATE INDEX IF NOT EXISTS idx_dim_weapons_ar_str
    ON golden_order.dim_weapons (ar_str_build_60 DESC);
CREATE INDEX IF NOT EXISTS idx_dim_weapons_ar_dex
    ON golden_order.dim_weapons (ar_dex_build_60 DESC);
CREATE INDEX IF NOT EXISTS idx_dim_weapons_ar_quality
    ON golden_order.dim_weapons (ar_quality_build_60 DESC);

-- Fuzzy name search
CREATE INDEX IF NOT EXISTS idx_dim_weapons_name_trgm
    ON golden_order.dim_weapons USING GIN (weapon_name gin_trgm_ops);

-- DLC filter
CREATE INDEX IF NOT EXISTS idx_dim_weapons_dlc
    ON golden_order.dim_weapons (is_dlc);

COMMENT ON TABLE golden_order.dim_weapons IS
    'Weapon dimension: one row per weapon × upgrade level × upgrade type. '
    'AR columns are pre-computed by the Scaling Engine to avoid UDF calls at query time.';
