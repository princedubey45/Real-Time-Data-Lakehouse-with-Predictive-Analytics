-- warehouse/schema.sql
-- ══════════════════════════════════════════════════════════════════
--  Enterprise Data Warehouse — Core Schema
--  Execution order (via docker-compose init):
--    01_schema.sql  ← this file
--    02_dim_customer.sql
--    03_dim_product.sql
--    04_dim_date.sql
--    05_fact_sales.sql
-- ══════════════════════════════════════════════════════════════════

-- Extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_trgm";    -- fast text search on product titles

-- ── Schemas ────────────────────────────────────────────────────────────────────
CREATE SCHEMA IF NOT EXISTS warehouse;
CREATE SCHEMA IF NOT EXISTS reporting;

-- Set default search path so we don't need schema prefixes
ALTER DATABASE data_warehouse SET search_path TO public, warehouse, reporting;

-- ── Load Audit ─────────────────────────────────────────────────────────────────
-- Written by load_warehouse.py after every table load
CREATE TABLE IF NOT EXISTS load_audit (
    id            SERIAL       PRIMARY KEY,
    table_name    VARCHAR(100) NOT NULL,
    rows_loaded   INTEGER      NOT NULL DEFAULT 0,
    run_ts        TIMESTAMPTZ  NOT NULL,
    status        VARCHAR(20)  NOT NULL DEFAULT 'success',  -- success | failed
    error_message TEXT,
    created_at    TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_audit_table_run ON load_audit(table_name, run_ts DESC);

-- ── Anomaly Log ────────────────────────────────────────────────────────────────
-- Written by anomaly_detection/anomaly.py
CREATE TABLE IF NOT EXISTS anomaly_log (
    id              SERIAL       PRIMARY KEY,
    detected_at     TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    entity          VARCHAR(100) NOT NULL,
    anomaly_type    VARCHAR(100) NOT NULL,
    affected_key    VARCHAR(255),
    metric_name     VARCHAR(100),
    metric_value    NUMERIC,
    expected_range  VARCHAR(100),
    z_score         NUMERIC(8,4),
    severity        VARCHAR(20)  NOT NULL DEFAULT 'medium',  -- low|medium|high|critical
    description     TEXT,
    resolved        BOOLEAN      NOT NULL DEFAULT FALSE,
    resolved_at     TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_anomaly_entity     ON anomaly_log(entity, detected_at DESC);
CREATE INDEX IF NOT EXISTS idx_anomaly_severity   ON anomaly_log(severity, detected_at DESC);
CREATE INDEX IF NOT EXISTS idx_anomaly_unresolved ON anomaly_log(resolved) WHERE resolved = FALSE;

-- ── Forecast Store ─────────────────────────────────────────────────────────────
-- Written by forecasting/forecast.py
CREATE TABLE IF NOT EXISTS forecast_results (
    id              SERIAL       PRIMARY KEY,
    forecast_run_ts TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    model_name      VARCHAR(100) NOT NULL,
    entity          VARCHAR(100) NOT NULL,     -- 'daily_revenue' | 'product:123'
    forecast_date   DATE         NOT NULL,
    predicted_value NUMERIC(14,4),
    lower_bound     NUMERIC(14,4),
    upper_bound     NUMERIC(14,4),
    confidence      NUMERIC(5,4),
    horizon_day     INTEGER,                   -- 1 = tomorrow, 2 = day after, …
    UNIQUE (forecast_run_ts, entity, forecast_date)
);

CREATE INDEX IF NOT EXISTS idx_forecast_entity_date
    ON forecast_results(entity, forecast_date DESC);
