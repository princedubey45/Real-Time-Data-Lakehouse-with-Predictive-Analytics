-- warehouse/dim_customer.sql
-- ══════════════════════════════════════════════════════════════════
--  Dimension: Customer
--  Type: SCD Type 1 (last-write-wins — overwrite on change)
--  Grain: one row per customer
--  Loaded by: etl/load_warehouse.py
-- ══════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS dim_customer (
    -- Surrogate key (matches API user id)
    customer_sk     INTEGER      PRIMARY KEY,

    -- Descriptive attributes
    username        VARCHAR(100),
    full_name       VARCHAR(255),

    -- PII stored as SHA-256 hashes only
    email_hash      CHAR(64),
    phone_hash      CHAR(64),
    has_email       BOOLEAN      NOT NULL DEFAULT FALSE,
    has_phone       BOOLEAN      NOT NULL DEFAULT FALSE,

    -- Location
    city            VARCHAR(100),
    zipcode         VARCHAR(20),
    geo_lat         VARCHAR(30),   -- stored as text — precision varies in source
    geo_long        VARCHAR(30),

    -- SCD Type 1 tracking
    effective_from  DATE,
    is_current      BOOLEAN      NOT NULL DEFAULT TRUE,

    -- Metadata
    loaded_at       TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

-- Indexes for common join / filter patterns
CREATE INDEX IF NOT EXISTS idx_dim_customer_city     ON dim_customer(city);
CREATE INDEX IF NOT EXISTS idx_dim_customer_current  ON dim_customer(is_current) WHERE is_current = TRUE;

-- ── Reporting view ─────────────────────────────────────────────────────────────
-- Joins customer with lifetime value aggregate for Power BI
CREATE OR REPLACE VIEW v_customer_overview AS
SELECT
    c.customer_sk,
    c.username,
    c.full_name,
    c.city,
    c.zipcode,
    c.has_email,
    c.has_phone,
    COALESCE(ltv.total_orders,    0)    AS total_orders,
    COALESCE(ltv.total_items,     0)    AS total_items,
    COALESCE(ltv.total_spend,     0.00) AS total_spend,
    COALESCE(ltv.avg_order_value, 0.00) AS avg_order_value,
    COALESCE(ltv.ltv_tier::TEXT,  'unknown') AS ltv_tier
FROM dim_customer c
LEFT JOIN agg_customer_ltv ltv USING (customer_sk)
WHERE c.is_current = TRUE;

COMMENT ON TABLE  dim_customer IS 'Customer dimension — SCD Type 1. PII fields are hashed.';
COMMENT ON COLUMN dim_customer.email_hash IS 'SHA-256 of lowercase email. Used for deduplication only.';
COMMENT ON COLUMN dim_customer.phone_hash IS 'SHA-256 of phone number.';
