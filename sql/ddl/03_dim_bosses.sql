-- ════════════════════════════════════════════════════════════════════════════
-- dim_bosses  —  Boss Dimension Table
-- ════════════════════════════════════════════════════════════════════════════
-- Grain: one row per unique boss.
-- ════════════════════════════════════════════════════════════════════════════

SET search_path TO golden_order, public;

CREATE TABLE IF NOT EXISTS golden_order.dim_bosses (
    boss_key        BIGSERIAL PRIMARY KEY,
    boss_name       TEXT        NOT NULL,
    hp              INTEGER     NOT NULL DEFAULT 0,
    is_dlc          BOOLEAN     NOT NULL DEFAULT FALSE,
    drops_runes     BIGINT      NOT NULL DEFAULT 0,
    drops_items     TEXT,
    loaded_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT uq_dim_bosses_name UNIQUE (boss_name)
);

CREATE INDEX IF NOT EXISTS idx_dim_bosses_dlc
    ON golden_order.dim_bosses (is_dlc);
CREATE INDEX IF NOT EXISTS idx_dim_bosses_hp
    ON golden_order.dim_bosses (hp DESC);

COMMENT ON TABLE golden_order.dim_bosses IS
    'Boss dimension: one row per boss with HP, rune drops, and DLC flag.';
