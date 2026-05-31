# Enterprise Data Platform

A production-grade data engineering project demonstrating an end-to-end
pipeline from REST API ingestion through a Bronze/Silver/Gold Data Lake,
PostgreSQL Data Warehouse, anomaly detection, forecasting, and Power BI.

---

## Architecture Overview

```
┌───────────────────────────────────────────────────────────────────────┐
│                          REST APIs                                    │
│           Orders (carts)  ·  Customers (users)  ·  Products          │
└────────────────────────────┬──────────────────────────────────────────┘
                             │ JSON
                             ▼
┌───────────────────────────────────────────────────────────────────────┐
│                   Mini Data Lake  (MinIO / S3)                        │
│                                                                       │
│  ┌──────────────┐   clean    ┌──────────────┐   agg    ┌──────────┐  │
│  │    Bronze    │  ────────► │    Silver    │ ───────► │   Gold   │  │
│  │  raw/ JSON   │            │  processed/  │          │  gold/   │  │
│  │  as-received │            │  Parquet     │          │  Parquet │  │
│  └──────────────┘            └──────────────┘          └──────────┘  │
│                                                                       │
│  quality_reports/   forecasts/                                        │
└──────────────────────────────────────┬────────────────────────────────┘
                                       │ upsert
                                       ▼
┌───────────────────────────────────────────────────────────────────────┐
│                    PostgreSQL Data Warehouse                          │
│                                                                       │
│  dim_customer  dim_product  dim_date   ◄── Dimensions                 │
│  fact_sales                            ◄── Fact                      │
│  agg_daily_sales  agg_product_perf  agg_customer_ltv  ◄── Aggregates │
│  anomaly_log  forecast_results  load_audit  ◄── Operational          │
│  v_daily_revenue  v_category_revenue  v_top_products  ◄── Views      │
└──────────────────────────────────────┬────────────────────────────────┘
                                       │ DirectQuery / Import
                                       ▼
                              ┌─────────────────┐
                              │    Power BI      │
                              │    Dashboard     │
                              └─────────────────┘

Anomaly Detection ─► anomaly_log (written to warehouse)
Forecasting       ─► forecast_results (warehouse + MinIO Parquet)
Orchestration     ─► Apache Airflow (2 DAGs)
```

---

## Project Structure

```
enterprise-data-platform/
├── config.py                        ← Central config (all credentials)
├── requirements.txt
├── docker-compose.yml               ← Full stack: PG · MinIO · Redis · Airflow · Grafana
│
├── api_ingestion/
│   ├── fetch_orders.py              ← Orders: API → raw JSON → MinIO Bronze
│   ├── fetch_customers.py           ← Customers: API → raw JSON (PII flagged)
│   └── fetch_products.py            ← Products: API → raw JSON + enrichment
│
├── etl/
│   ├── clean_data.py                ← Silver: raw JSON → cleaned Parquet
│   ├── transform.py                 ← Gold: cleaned Parquet → business tables
│   ├── validate.py                  ← Quality checks: schema, nulls, ranges, RI
│   └── load_warehouse.py            ← Load: Gold Parquet → PostgreSQL (upsert)
│
├── airflow/
│   └── dags/
│       ├── etl_pipeline.py          ← Main DAG: ingest → clean → transform → validate → load
│       └── quality_check.py         ← Daily: quality → anomaly → forecast → notify
│
├── warehouse/
│   ├── schema.sql                   ← Extensions, audit/anomaly/forecast tables
│   ├── dim_customer.sql             ← Customer dimension + v_customer_overview
│   ├── dim_product.sql              ← Product dimension + v_top_products
│   ├── dim_date.sql                 ← Date dimension + v_last_90_days
│   └── fact_sales.sql               ← Sales fact + aggregates + reporting views
│
├── anomaly_detection/
│   └── anomaly.py                   ← Z-score, IQR, day-over-day revenue checks
│
├── forecasting/
│   └── forecast.py                  ← Holt-Winters 30-day revenue forecast
│
├── data_lake/
│   ├── raw/                         ← Local mirror of MinIO raw/ prefix
│   ├── processed/                   ← Local mirror of MinIO processed/ prefix
│   └── archive/                     ← Cold storage (files > 90 days)
│
└── docs/
    ├── README.md                    ← This file
    ├── architecture.png             ← Architecture diagram
    └── ERD.png                      ← Entity-Relationship Diagram
```

---

## Quick Start

### Prerequisites
- Docker + Docker Compose
- Python 3.11+

### 1 — Install Python dependencies

```bash
pip install -r requirements.txt
```

### 2 — Start all services

```bash
docker-compose up -d
```

| Service       | URL                      | Credentials              |
|---------------|--------------------------|--------------------------|
| Airflow UI    | http://localhost:8080    | admin / admin            |
| MinIO Console | http://localhost:9001    | minioadmin / minioadmin  |
| PostgreSQL    | localhost:5432           | warehouse / warehouse    |
| Grafana       | http://localhost:3000    | admin / admin            |

### 3 — Run the pipeline manually (without Airflow)

```bash
# Step 1: Ingest raw data
python api_ingestion/fetch_orders.py
python api_ingestion/fetch_customers.py
python api_ingestion/fetch_products.py

# Step 2: Clean (Silver layer)
python etl/clean_data.py

# Step 3: Transform (Gold layer)
python etl/transform.py

# Step 4: Validate quality
python etl/validate.py

# Step 5: Load warehouse
python etl/load_warehouse.py

# Step 6: Detect anomalies
python anomaly_detection/anomaly.py

# Step 7: Generate forecast
python forecasting/forecast.py
```

### 4 — Via Airflow

1. Open http://localhost:8080
2. Enable the `etl_pipeline` DAG → triggers daily at 06:00 UTC
3. Enable the `quality_check` DAG → triggers daily at 06:30 UTC
4. Or click **Trigger DAG** for an immediate run

---

## Data Lake Layers

| Layer   | Format  | MinIO Prefix                              | Contents                         |
|---------|---------|-------------------------------------------|----------------------------------|
| Bronze  | JSON    | `raw/<entity>/date=YYYY-MM-DD/*.json`     | Raw API responses + `_meta`      |
| Silver  | Parquet | `processed/<entity>/date=YYYY-MM-DD/*.parquet` | Cleaned, validated, typed   |
| Gold    | Parquet | `processed/gold/<table>/date=YYYY-MM-DD/*.parquet` | Joined, aggregated       |
| Reports | JSON    | `quality_reports/date=YYYY-MM-DD/*.json`  | Validation audit reports         |
| Forecast| Parquet | `processed/forecasts/*.parquet`           | 30-day revenue forecasts         |

---

## Warehouse Tables

| Object                | Type      | Description                                         |
|-----------------------|-----------|-----------------------------------------------------|
| `dim_customer`        | Dimension | SCD Type 1. PII fields SHA-256 hashed               |
| `dim_product`         | Dimension | Product catalogue with value score                  |
| `dim_date`            | Dimension | Date spine, YYYYMMDD integer key                    |
| `fact_sales`          | Fact      | Order line items, grain = order × product           |
| `agg_daily_sales`     | Aggregate | Daily revenue KPIs                                  |
| `agg_product_perf`    | Aggregate | Revenue, units sold per product                     |
| `agg_customer_ltv`    | Aggregate | Lifetime value tier per customer                    |
| `anomaly_log`         | Operational | Flagged data anomalies with severity              |
| `forecast_results`    | Operational | 30-day revenue forecast with confidence bands     |
| `load_audit`          | Operational | Load audit trail per table per run                |
| `v_daily_revenue`     | View      | Daily revenue joined with dim_date                  |
| `v_monthly_revenue`   | View      | Monthly rollup for trend charts                     |
| `v_category_revenue`  | View      | Revenue by product category                         |
| `v_sales_detail`      | View      | Full drill-through fact with all dimension labels   |
| `v_customer_overview` | View      | Customer + LTV tier joined                          |
| `v_top_products`      | View      | Revenue-ranked products per category                |

---

## Power BI Connection

**Server:** `localhost`  **Port:** `5432`  **Database:** `data_warehouse`
**User:** `warehouse`    **Password:** `warehouse`

Suggested pages:
1. **Executive Summary** — KPI cards from `agg_daily_sales`
2. **Revenue Trends** — Line chart from `v_daily_revenue`
3. **Product Performance** — Bar chart from `v_top_products`
4. **Customer LTV** — Scatter from `agg_customer_ltv`
5. **Anomalies** — Table from `anomaly_log` filtered to `resolved=false`
6. **Forecast** — Line from `forecast_results` (actual + predicted)

---

## Anomaly Detection

Five checks run daily via `quality_check` DAG:

| Check                 | Method    | Threshold                          |
|-----------------------|-----------|------------------------------------|
| Revenue outliers      | Z-score   | > 3σ from order revenue mean       |
| Quantity outliers     | IQR fence | > Q3 + 1.5×IQR                     |
| Price discrepancy     | % diff    | Avg sold price > 10% from catalogue|
| Daily revenue drop    | Rolling   | > 40% below 7-day rolling average  |
| Customer spend spike  | Z-score   | Orders or spend > 3σ               |

All flagged records are written to `anomaly_log` with severity `low/medium/high/critical`.

---

## Forecasting

Uses **Holt-Winters Exponential Smoothing** (additive trend + weekly seasonality).

- Horizon: 30 days
- Confidence interval: 95%
- Fallback: linear regression when < 14 days of history

Results stored in `forecast_results` (DB) and `processed/forecasts/` (MinIO Parquet).

---

## Key Design Principles

- **Separation of concerns**: each layer (Bronze/Silver/Gold/Load) is independently runnable
- **Idempotency**: all loads use `INSERT … ON CONFLICT DO UPDATE` — re-runs are safe
- **PII protection**: customer email and phone are SHA-256 hashed in the Silver layer
- **Data lineage**: raw Bronze files preserved indefinitely for replay
- **Observability**: `load_audit` + `anomaly_log` provide full operational visibility
- **Quality gates**: `validate.py` raises `DataQualityError` on critical failures, blocking the load
