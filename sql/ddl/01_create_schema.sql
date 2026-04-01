-- ════════════════════════════════════════════════════════════════════════════
-- Golden Order Data Warehouse — Schema Bootstrap
-- ════════════════════════════════════════════════════════════════════════════
-- Run order: 01 → 02 → 03 → 04 → 05
-- Compatible with: PostgreSQL 15+
-- Cloud equivalents: Replace schema keyword with BigQuery dataset or
--                    Snowflake schema as appropriate.
-- ════════════════════════════════════════════════════════════════════════════

-- Create the warehouse schema (namespace for all Golden Order objects)
CREATE SCHEMA IF NOT EXISTS golden_order;

-- Set search_path so subsequent DDL files don't need to qualify every name
SET search_path TO golden_order, public;

-- Enable pg_trgm for text similarity searches (weapon name fuzzy search)
CREATE EXTENSION IF NOT EXISTS pg_trgm;

COMMENT ON SCHEMA golden_order IS
    'Elden Ring Combat & Build Analytics — all dimensional model objects';
