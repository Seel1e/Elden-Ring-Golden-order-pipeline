-- ════════════════════════════════════════════════════════════════════════════
-- Staging Transform: Raw API JSON → stg_bosses
-- ════════════════════════════════════════════════════════════════════════════

SET search_path TO golden_order, public;

CREATE TABLE IF NOT EXISTS golden_order.raw_api_bosses (
    id          BIGSERIAL PRIMARY KEY,
    fetched_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    run_date    DATE        NOT NULL,
    page_num    INTEGER     NOT NULL,
    payload     JSONB       NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_raw_api_bosses_page
    ON golden_order.raw_api_bosses (run_date, page_num);


CREATE TABLE IF NOT EXISTS golden_order.stg_bosses AS
SELECT
    item->>'id'         AS api_id,
    item->>'name'       AS boss_name,
    item->>'healthPoints' AS hp_raw,
    item->>'drops'      AS drops_raw,
    item->>'description' AS description,
    NOW()               AS staged_at
FROM golden_order.raw_api_bosses,
     LATERAL jsonb_array_elements(payload->'data') AS item
WHERE 1=1
LIMIT 0;


CREATE OR REPLACE PROCEDURE golden_order.refresh_stg_bosses()
LANGUAGE plpgsql AS $$
BEGIN
    TRUNCATE TABLE golden_order.stg_bosses;

    INSERT INTO golden_order.stg_bosses
    SELECT
        item->>'id'             AS api_id,
        item->>'name'           AS boss_name,
        item->>'healthPoints'   AS hp_raw,
        item->>'drops'          AS drops_raw,
        item->>'description'    AS description,
        NOW()
    FROM golden_order.raw_api_bosses,
         LATERAL jsonb_array_elements(payload->'data') AS item;

    RAISE NOTICE 'stg_bosses refreshed: % rows', (SELECT COUNT(*) FROM golden_order.stg_bosses);
END;
$$;
