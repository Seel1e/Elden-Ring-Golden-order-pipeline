-- ════════════════════════════════════════════════════════════════════════════
-- Staging Transform: Raw API JSON → stg_weapons
-- ════════════════════════════════════════════════════════════════════════════
-- Parses the deeply nested JSON arrays returned by the Elden Ring Fan API
-- (stored as raw JSONB in the landing table) into tidy, typed rows.
--
-- This script is idempotent: the TRUNCATE + INSERT pattern means re-running
-- always produces a clean, deterministic result.
-- ════════════════════════════════════════════════════════════════════════════

SET search_path TO golden_order, public;

-- ── Landing table (raw API response stored as JSONB) ─────────────────────────
CREATE TABLE IF NOT EXISTS golden_order.raw_api_weapons (
    id              BIGSERIAL PRIMARY KEY,
    fetched_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    run_date        DATE        NOT NULL,
    page_num        INTEGER     NOT NULL,
    payload         JSONB       NOT NULL
);

-- Dedup index: same (run_date, page_num) → idempotent
CREATE UNIQUE INDEX IF NOT EXISTS uq_raw_api_weapons_page
    ON golden_order.raw_api_weapons (run_date, page_num);


-- ── Staging table (flattened) ────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS golden_order.stg_weapons AS
SELECT
    item->>'id'                                     AS api_id,
    item->>'name'                                   AS weapon_name,
    item->>'category'                               AS category,
    item->>'weight'                                 AS weight_raw,
    item->>'description'                            AS description,
    -- Attack power: nested JSON object
    (item->'attack'->>'physical')                   AS base_physical_raw,
    (item->'attack'->>'magic')                      AS base_magic_raw,
    (item->'attack'->>'fire')                       AS base_fire_raw,
    (item->'attack'->>'lightning')                  AS base_lightning_raw,
    (item->'attack'->>'holy')                       AS base_holy_raw,
    -- Scaling grades: nested JSON object
    (item->'scalesWith'->>'Str')                    AS scale_str,
    (item->'scalesWith'->>'Dex')                    AS scale_dex,
    (item->'scalesWith'->>'Int')                    AS scale_int,
    (item->'scalesWith'->>'Fai')                    AS scale_fai,
    (item->'scalesWith'->>'Arc')                    AS scale_arc,
    -- Requirements
    (item->'requiredAttributes'->>'strength')       AS req_str_raw,
    (item->'requiredAttributes'->>'dexterity')      AS req_dex_raw,
    (item->'requiredAttributes'->>'intelligence')   AS req_int_raw,
    (item->'requiredAttributes'->>'faith')          AS req_fai_raw,
    (item->'requiredAttributes'->>'arcane')         AS req_arc_raw,
    NOW()                                           AS staged_at
FROM golden_order.raw_api_weapons,
     LATERAL jsonb_array_elements(payload->'data') AS item
WHERE 1=1
LIMIT 0;   -- create empty table with correct schema

-- ── Idempotent refresh: truncate and re-derive from raw ───────────────────────
-- Called by the Airflow staging_to_dim task.
-- Wrapped in a transaction so a partial failure leaves stg_weapons intact.

-- NOTE: This is a stored procedure so Airflow can call it via PostgresOperator.
CREATE OR REPLACE PROCEDURE golden_order.refresh_stg_weapons()
LANGUAGE plpgsql AS $$
BEGIN
    TRUNCATE TABLE golden_order.stg_weapons;

    INSERT INTO golden_order.stg_weapons
    SELECT
        item->>'id'                                     AS api_id,
        item->>'name'                                   AS weapon_name,
        item->>'category'                               AS category,
        item->>'weight'                                 AS weight_raw,
        item->>'description'                            AS description,
        (item->'attack'->>'physical')                   AS base_physical_raw,
        (item->'attack'->>'magic')                      AS base_magic_raw,
        (item->'attack'->>'fire')                       AS base_fire_raw,
        (item->'attack'->>'lightning')                  AS base_lightning_raw,
        (item->'attack'->>'holy')                       AS base_holy_raw,
        (item->'scalesWith'->>'Str')                    AS scale_str,
        (item->'scalesWith'->>'Dex')                    AS scale_dex,
        (item->'scalesWith'->>'Int')                    AS scale_int,
        (item->'scalesWith'->>'Fai')                    AS scale_fai,
        (item->'scalesWith'->>'Arc')                    AS scale_arc,
        (item->'requiredAttributes'->>'strength')       AS req_str_raw,
        (item->'requiredAttributes'->>'dexterity')      AS req_dex_raw,
        (item->'requiredAttributes'->>'intelligence')   AS req_int_raw,
        (item->'requiredAttributes'->>'faith')          AS req_fai_raw,
        (item->'requiredAttributes'->>'arcane')         AS req_arc_raw,
        NOW()
    FROM golden_order.raw_api_weapons,
         LATERAL jsonb_array_elements(payload->'data') AS item;

    RAISE NOTICE 'stg_weapons refreshed: % rows', (SELECT COUNT(*) FROM golden_order.stg_weapons);
END;
$$;
