-- warehouse/fact_sales.sql
-- ══════════════════════════════════════════════════════════════════
--  Fact Table: Sales
--  Grain: one row per order line-item (one product on one order)
--  Additive measures: quantity, revenue, net_revenue
--  Foreign keys: customer_sk, product_sk, date_sk
-- ══════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS fact_sales (
    -- Degenerate dimensions (natural keys from source)
    order_sk         INTEGER       NOT NULL,
    product_sk       INTEGER       NOT NULL REFERENCES dim_product(product_sk),

    -- Foreign keys to dimensions
    customer_sk      INTEGER       REFERENCES dim_customer(customer_sk),
    date_sk          INTEGER       REFERENCES dim_date(date_sk),

    -- Measures
    quantity         INTEGER       NOT NULL DEFAULT 1 CHECK (quantity > 0),
    unit_price       NUMERIC(10,2) NOT NULL DEFAULT 0.00,
    revenue          NUMERIC(14,2) NOT NULL DEFAULT 0.00,
    discount_rate    NUMERIC(5,4)  NOT NULL DEFAULT 0.00
                                   CHECK (discount_rate BETWEEN 0 AND 1),
    net_revenue      NUMERIC(14,2) NOT NULL DEFAULT 0.00,

    -- Denormalised for query performance (avoids joins in common queries)
    category         VARCHAR(100),
    price_tier       VARCHAR(20),

    -- Audit
    order_date       TIMESTAMPTZ,
    loaded_at        TIMESTAMPTZ   NOT NULL DEFAULT NOW(),

    -- Composite primary key
    PRIMARY KEY (order_sk, product_sk)
);

-- ── Indexes ────────────────────────────────────────────────────────────────────
CREATE INDEX IF NOT EXISTS idx_fact_sales_customer  ON fact_sales(customer_sk);
CREATE INDEX IF NOT EXISTS idx_fact_sales_product   ON fact_sales(product_sk);
CREATE INDEX IF NOT EXISTS idx_fact_sales_date      ON fact_sales(date_sk);
CREATE INDEX IF NOT EXISTS idx_fact_sales_category  ON fact_sales(category);
CREATE INDEX IF NOT EXISTS idx_fact_sales_order_date ON fact_sales(order_date DESC);

-- ── Aggregation tables (loaded alongside fact) ─────────────────────────────────

CREATE TABLE IF NOT EXISTS agg_daily_sales (
    date_sk           INTEGER       PRIMARY KEY REFERENCES dim_date(date_sk),
    total_orders      INTEGER       NOT NULL DEFAULT 0,
    total_items       INTEGER       NOT NULL DEFAULT 0,
    gross_revenue     NUMERIC(14,2) NOT NULL DEFAULT 0.00,
    net_revenue       NUMERIC(14,2) NOT NULL DEFAULT 0.00,
    avg_order_value   NUMERIC(10,2),
    loaded_at         TIMESTAMPTZ   NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS agg_customer_ltv (
    customer_sk       INTEGER       PRIMARY KEY REFERENCES dim_customer(customer_sk),
    total_orders      INTEGER       NOT NULL DEFAULT 0,
    total_items       INTEGER       NOT NULL DEFAULT 0,
    total_spend       NUMERIC(14,2) NOT NULL DEFAULT 0.00,
    avg_order_value   NUMERIC(10,2),
    first_order       TIMESTAMPTZ,
    last_order        TIMESTAMPTZ,
    ltv_tier          VARCHAR(20),   -- low | medium | high | vip
    loaded_at         TIMESTAMPTZ   NOT NULL DEFAULT NOW()
);

-- ── Reporting views ────────────────────────────────────────────────────────────

-- Daily revenue trend (used in Power BI line chart)
CREATE OR REPLACE VIEW v_daily_revenue AS
SELECT
    d.full_date,
    d.year,
    d.year_quarter,
    d.year_month,
    d.month_name,
    d.day_name,
    d.is_weekend,
    COALESCE(a.total_orders,    0)    AS total_orders,
    COALESCE(a.total_items,     0)    AS total_items,
    COALESCE(a.gross_revenue,   0.00) AS gross_revenue,
    COALESCE(a.net_revenue,     0.00) AS net_revenue,
    COALESCE(a.avg_order_value, 0.00) AS avg_order_value
FROM   dim_date d
LEFT JOIN agg_daily_sales a USING (date_sk)
ORDER  BY d.full_date;


-- Monthly revenue summary (Power BI bar chart)
CREATE OR REPLACE VIEW v_monthly_revenue AS
SELECT
    d.year,
    d.month,
    d.month_name,
    d.year_month,
    SUM(COALESCE(a.gross_revenue, 0)) AS gross_revenue,
    SUM(COALESCE(a.net_revenue,   0)) AS net_revenue,
    SUM(COALESCE(a.total_orders,  0)) AS total_orders,
    SUM(COALESCE(a.total_items,   0)) AS total_items,
    AVG(COALESCE(a.avg_order_value,0)) AS avg_order_value
FROM   dim_date d
LEFT JOIN agg_daily_sales a USING (date_sk)
GROUP  BY d.year, d.month, d.month_name, d.year_month
ORDER  BY d.year, d.month;


-- Category performance (Power BI pie / treemap)
CREATE OR REPLACE VIEW v_category_revenue AS
SELECT
    category,
    SUM(quantity)    AS total_units_sold,
    SUM(revenue)     AS gross_revenue,
    SUM(net_revenue) AS net_revenue,
    COUNT(DISTINCT order_sk)   AS order_count,
    COUNT(DISTINCT customer_sk) AS unique_customers,
    ROUND(SUM(net_revenue) / NULLIF(SUM(SUM(net_revenue)) OVER (), 0) * 100, 2)
        AS revenue_share_pct
FROM   fact_sales
GROUP  BY category
ORDER  BY gross_revenue DESC;


-- Full sales detail (Power BI drill-through)
CREATE OR REPLACE VIEW v_sales_detail AS
SELECT
    fs.order_sk,
    fs.order_date,
    d.full_date,
    d.year_month,
    c.username          AS customer_username,
    c.full_name         AS customer_name,
    c.city              AS customer_city,
    c.ltv_tier,
    p.title             AS product_title,
    p.category,
    p.price_tier,
    fs.quantity,
    fs.unit_price,
    fs.revenue,
    fs.discount_rate,
    fs.net_revenue
FROM  fact_sales fs
LEFT JOIN dim_date     d ON fs.date_sk     = d.date_sk
LEFT JOIN v_customer_overview c ON fs.customer_sk = c.customer_sk
LEFT JOIN dim_product  p ON fs.product_sk  = p.product_sk;

COMMENT ON TABLE fact_sales IS
    'Sales fact table. Grain = one order line-item. '
    'Primary key = (order_sk, product_sk).';
