-- warehouse/dim_product.sql
-- ══════════════════════════════════════════════════════════════════
--  Dimension: Product
--  Type: SCD Type 1
--  Grain: one row per product
--  Loaded by: etl/load_warehouse.py
-- ══════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS dim_product (
    product_sk       INTEGER       PRIMARY KEY,

    -- Core attributes
    title            TEXT          NOT NULL,
    category         VARCHAR(100)  NOT NULL,

    -- Pricing
    price            NUMERIC(10,2) NOT NULL CHECK (price > 0),
    price_tier       VARCHAR(20)   NOT NULL DEFAULT 'mid',
                                   -- budget | mid | premium

    -- Quality indicators
    rating_score     NUMERIC(3,1)  CHECK (rating_score BETWEEN 0 AND 5),
    rating_count     INTEGER       NOT NULL DEFAULT 0 CHECK (rating_count >= 0),
    popularity       VARCHAR(20)   NOT NULL DEFAULT 'low',
                                   -- low | medium | high
    value_score      NUMERIC(6,4),  -- composite score 0–1

    -- Content
    description_len  INTEGER       NOT NULL DEFAULT 0,

    -- Metadata
    loaded_at        TIMESTAMPTZ   NOT NULL DEFAULT NOW()
);

-- Full-text search on product title (uses pg_trgm extension from schema.sql)
CREATE INDEX IF NOT EXISTS idx_dim_product_title_trgm
    ON dim_product USING GIN (title gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_dim_product_category  ON dim_product(category);
CREATE INDEX IF NOT EXISTS idx_dim_product_price_tier ON dim_product(price_tier);
CREATE INDEX IF NOT EXISTS idx_dim_product_popularity ON dim_product(popularity);

-- Aggregate tables referenced by views (created by load_warehouse.py)
CREATE TABLE IF NOT EXISTS agg_product_perf (
    product_sk       INTEGER       PRIMARY KEY REFERENCES dim_product(product_sk),
    total_qty_sold   INTEGER       NOT NULL DEFAULT 0,
    total_revenue    NUMERIC(14,2) NOT NULL DEFAULT 0,
    order_count      INTEGER       NOT NULL DEFAULT 0,
    title            TEXT,
    category         VARCHAR(100),
    price            NUMERIC(10,2),
    rating_score     NUMERIC(3,1),
    rating_count     INTEGER,
    revenue_per_unit NUMERIC(10,2),
    loaded_at        TIMESTAMPTZ   NOT NULL DEFAULT NOW()
);

-- ── Reporting views ────────────────────────────────────────────────────────────

CREATE OR REPLACE VIEW v_product_catalogue AS
SELECT
    p.product_sk,
    p.title,
    p.category,
    p.price,
    p.price_tier,
    p.rating_score,
    p.rating_count,
    p.popularity,
    p.value_score,
    COALESCE(perf.total_qty_sold,  0)    AS units_sold,
    COALESCE(perf.total_revenue,   0.00) AS total_revenue,
    COALESCE(perf.order_count,     0)    AS order_count,
    COALESCE(perf.revenue_per_unit,0.00) AS revenue_per_unit
FROM dim_product p
LEFT JOIN agg_product_perf perf USING (product_sk);


CREATE OR REPLACE VIEW v_top_products AS
SELECT
    p.product_sk,
    p.title,
    p.category,
    p.price,
    p.price_tier,
    p.rating_score,
    COALESCE(perf.total_revenue, 0) AS total_revenue,
    COALESCE(perf.total_qty_sold, 0) AS units_sold,
    RANK() OVER (PARTITION BY p.category ORDER BY COALESCE(perf.total_revenue, 0) DESC)
        AS revenue_rank_in_category
FROM dim_product p
LEFT JOIN agg_product_perf perf USING (product_sk)
ORDER BY total_revenue DESC;

COMMENT ON TABLE dim_product IS 'Product dimension — SCD Type 1. Category stored as snake_case.';
