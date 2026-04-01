-- ════════════════════════════════════════════════════════════════════════════
-- Staging tables + stored procedures for Airflow DAG tasks
-- ════════════════════════════════════════════════════════════════════════════

SET search_path TO golden_order, public;

-- ── Raw API landing tables ────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS golden_order.raw_api_weapons (
    id          BIGSERIAL PRIMARY KEY,
    fetched_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    run_date    DATE        NOT NULL,
    page_num    INTEGER     NOT NULL,
    payload     JSONB       NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_raw_api_weapons_page
    ON golden_order.raw_api_weapons (run_date, page_num);

CREATE TABLE IF NOT EXISTS golden_order.raw_api_bosses (
    id          BIGSERIAL PRIMARY KEY,
    fetched_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    run_date    DATE        NOT NULL,
    page_num    INTEGER     NOT NULL,
    payload     JSONB       NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_raw_api_bosses_page
    ON golden_order.raw_api_bosses (run_date, page_num);

-- ── Staging tables (empty shells) ────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS golden_order.stg_weapons (
    api_id           TEXT,
    weapon_name      TEXT,
    category         TEXT,
    weight_raw       TEXT,
    description      TEXT,
    base_physical_raw TEXT,
    base_magic_raw   TEXT,
    base_fire_raw    TEXT,
    base_lightning_raw TEXT,
    base_holy_raw    TEXT,
    scale_str        TEXT,
    scale_dex        TEXT,
    scale_int        TEXT,
    scale_fai        TEXT,
    scale_arc        TEXT,
    req_str_raw      TEXT,
    req_dex_raw      TEXT,
    req_int_raw      TEXT,
    req_fai_raw      TEXT,
    req_arc_raw      TEXT,
    staged_at        TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS golden_order.stg_bosses (
    api_id      TEXT,
    boss_name   TEXT,
    hp_raw      TEXT,
    drops_raw   TEXT,
    description TEXT,
    staged_at   TIMESTAMPTZ
);

-- ── Stored procedures (called by Airflow PostgresOperator) ────────────────────

CREATE OR REPLACE PROCEDURE golden_order.refresh_stg_weapons()
LANGUAGE plpgsql AS $$
BEGIN
    TRUNCATE TABLE golden_order.stg_weapons;
    INSERT INTO golden_order.stg_weapons
    SELECT
        item->>'id',
        item->>'name',
        item->>'category',
        item->>'weight',
        item->>'description',
        (item->'attack'->>'physical'),
        (item->'attack'->>'magic'),
        (item->'attack'->>'fire'),
        (item->'attack'->>'lightning'),
        (item->'attack'->>'holy'),
        (item->'scalesWith'->>'Str'),
        (item->'scalesWith'->>'Dex'),
        (item->'scalesWith'->>'Int'),
        (item->'scalesWith'->>'Fai'),
        (item->'scalesWith'->>'Arc'),
        (item->'requiredAttributes'->>'strength'),
        (item->'requiredAttributes'->>'dexterity'),
        (item->'requiredAttributes'->>'intelligence'),
        (item->'requiredAttributes'->>'faith'),
        (item->'requiredAttributes'->>'arcane'),
        NOW()
    FROM golden_order.raw_api_weapons,
         LATERAL jsonb_array_elements(payload->'data') AS item;
    RAISE NOTICE 'stg_weapons refreshed: % rows', (SELECT COUNT(*) FROM golden_order.stg_weapons);
END;
$$;

CREATE OR REPLACE PROCEDURE golden_order.refresh_stg_bosses()
LANGUAGE plpgsql AS $$
BEGIN
    TRUNCATE TABLE golden_order.stg_bosses;
    INSERT INTO golden_order.stg_bosses
    SELECT
        item->>'id',
        item->>'name',
        item->>'healthPoints',
        item->>'drops',
        item->>'description',
        NOW()
    FROM golden_order.raw_api_bosses,
         LATERAL jsonb_array_elements(payload->'data') AS item;
    RAISE NOTICE 'stg_bosses refreshed: % rows', (SELECT COUNT(*) FROM golden_order.stg_bosses);
END;
$$;
