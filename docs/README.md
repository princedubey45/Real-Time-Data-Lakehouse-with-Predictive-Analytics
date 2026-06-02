# Enterprise Data Platform

A **production-grade, end-to-end data engineering project** demonstrating a
complete modern data stack — from real-time event streaming through a
Bronze/Silver/Gold Data Lake, distributed PySpark transforms, a PostgreSQL
Data Warehouse, and Power BI analytics.

---

## Full Stack Architecture

```
┌────────────────────────────────────────────────────────────────────────────┐
│                        REST APIs  (FakeStore API)                          │
│              Orders (carts)  ·  Customers (users)  ·  Products            │
└───────────────────────────────┬────────────────────────────────────────────┘
                                │  JSON
                                ▼
┌────────────────────────────────────────────────────────────────────────────┐
│                  Apache Kafka  (Confluent Platform 7.6)                    │
│                                                                            │
│   ┌─────────────────┐  ┌──────────────────┐  ┌─────────────────────┐      │
│   │   orders-raw    │  │  customers-raw   │  │   products-raw      │      │
│   │  (3 partitions) │  │  (3 partitions)  │  │  (3 partitions)     │      │
│   └─────────────────┘  └──────────────────┘  └─────────────────────┘      │
│                           + pipeline-events topic                          │
└───────────────────────────────┬────────────────────────────────────────────┘
                                │  batch consume (Airflow task)
                                ▼
┌────────────────────────────────────────────────────────────────────────────┐
│                  Apache Airflow  (2.9.1 · CeleryExecutor)                  │
│                                                                            │
│  DAG 1 — etl_pipeline    (daily 06:00 UTC)                                 │
│    ingest_* → clean → transform (Pandas) → validate → load_warehouse       │
│                                                                            │
│  DAG 2 — kafka_spark_etl (every 15 min)      ◄── NEW                      │
│    produce_* → consume_kafka → spark_transform → load_warehouse            │
│                                                                            │
│  DAG 3 — quality_check   (daily 06:30 UTC)                                 │
│    quality → anomaly_detection → forecasting → notify                     │
└───────────────────────────────┬────────────────────────────────────────────┘
                                │
                    ┌───────────┴───────────┐
                    │                       │
                    ▼                       ▼
┌──────────────────────────┐   ┌────────────────────────────────────────────┐
│   Apache PySpark  3.5    │   │          MinIO / S3  (Data Lake)           │
│   (Standalone cluster)   │   │                                            │
│                          │   │  ┌──────────┐  ┌──────────┐  ┌──────────┐ │
│  spark-master :8181      │   │  │  Bronze  │  │  Silver  │  │   Gold   │ │
│  spark-worker :8282      │◄──┤  │  raw/    │  │processed/│  │ gold/    │ │
│                          │   │  │  JSON    │  │ Parquet  │  │ Parquet  │ │
│  Distributed transforms: │   │  └──────────┘  └──────────┘  └──────────┘ │
│  · dim_* dimensions      │   │  + quality_reports/  forecasts/            │
│  · fact_sales            │   └────────────────────────────────────────────┘
│  · agg_daily_sales       │                       │
│  · agg_product_perf      │                       │ upsert (Parquet → PG)
│  · agg_customer_ltv      │                       ▼
└──────────────────────────┘   ┌────────────────────────────────────────────┐
                               │           PostgreSQL  (Data Warehouse)      │
                               │                                            │
                               │  dim_customer  dim_product  dim_date       │
                               │  fact_sales                                │
                               │  agg_daily_sales  agg_product_perf         │
                               │  agg_customer_ltv                          │
                               │  anomaly_log  forecast_results  load_audit │
                               │  v_daily_revenue  v_category_revenue       │
                               │  v_top_products  v_sales_detail            │
                               └──────────────────┬─────────────────────────┘
                                                  │  DirectQuery / Import
                                                  ▼
                                        ┌──────────────────┐
                                        │    Power BI       │
                                        │    Dashboard      │
                                        └──────────────────┘

Monitoring:  Grafana  :3000  ·  Kafka-UI  :8090  ·  Airflow  :8080
```

---

## Project Structure

```
enterprise-data-platform/
├── config.py                         ← Central config (all credentials + Kafka + Spark)
├── requirements.txt                  ← kafka-python · confluent-kafka · pyspark · ...
├── docker-compose.yml                ← Full stack (10 services)
├── Dockerfile.airflow
├── Dockerfile.spark                  ← Custom Spark + S3A jars     ◄ NEW
├── Dockerfile.postgres
│
├── kafka_streaming/                                                 ◄ NEW
│   ├── __init__.py
│   ├── topics.py                     ← Topic registry (TopicSpec dataclasses)
│   ├── schema.py                     ← Pydantic event schemas (Order/Customer/Product)
│   ├── producer.py                   ← API → Kafka topics (confluent_kafka / kafka-python)
│   └── consumer.py                   ← Kafka topics → MinIO Bronze JSON
│
├── spark/                                                           ◄ NEW
│   ├── __init__.py
│   ├── spark_session.py              ← Singleton SparkSession (S3A / MinIO config)
│   ├── schemas.py                    ← PySpark StructType schemas for Silver tables
│   └── spark_transform.py            ← Distributed Gold transform (7 tables)
│
├── api_ingestion/
│   ├── fetch_orders.py               ← Direct: API → MinIO Bronze (batch pipeline)
│   ├── fetch_customers.py
│   └── fetch_products.py
│
├── etl/
│   ├── clean_data.py                 ← Silver: raw JSON → cleaned Parquet
│   ├── transform.py                  ← Gold (Pandas): used by etl_pipeline DAG
│   ├── validate.py                   ← Quality checks on Gold tables
│   └── load_warehouse.py             ← Gold Parquet → PostgreSQL upsert
│
├── airflow/dags/
│   ├── etl_pipeline.py               ← Batch DAG (daily, Pandas transform)
│   ├── kafka_spark_pipeline.py       ← Streaming DAG (15-min, PySpark)  ◄ NEW
│   └── quality_check.py             ← Quality / anomaly / forecast DAG
│
├── warehouse/
│   ├── schema.sql · dim_*.sql · fact_sales.sql
│
├── anomaly_detection/anomaly.py      ← Z-score, IQR, day-over-day checks
├── forecasting/forecast.py           ← Holt-Winters 30-day forecast
├── data_lake/raw/ processed/ archive/
└── docs/README.md
```

---

## Services & Ports

| Service           | URL / Port                | Credentials              |
|-------------------|---------------------------|--------------------------|
| Airflow UI        | http://localhost:8080     | admin / admin            |
| Kafka-UI          | http://localhost:8090     | —                        |
| Spark Master UI   | http://localhost:8181     | —                        |
| Spark Worker UI   | http://localhost:8282     | —                        |
| MinIO Console     | http://localhost:9001     | minioadmin / minioadmin  |
| PostgreSQL        | localhost:5432            | warehouse / warehouse    |
| Grafana           | http://localhost:3000     | admin / admin            |
| Kafka Broker      | localhost:29092           | —  (external)            |
| Zookeeper         | localhost:2181            | —                        |

---

## Quick Start

### Prerequisites
- Docker + Docker Compose
- Python 3.11+
- 8 GB RAM recommended (Spark + Kafka + Airflow)

### 1 — Install Python dependencies

```bash
pip install -r requirements.txt
```

### 2 — Start all services

```bash
docker-compose up -d
```

Wait ~60 seconds for Kafka and Airflow to fully initialise.

### 3 — Verify services are healthy

```bash
docker-compose ps                          # All services should show "healthy" or "running"
curl -s http://localhost:8080/health       # Airflow → {"status":"healthy"}
curl -s http://localhost:9000/minio/health/live  # MinIO → OK
```

### 4 — Run the Kafka + PySpark pipeline (streaming)

**Via Airflow UI:**
1. Open http://localhost:8080 → login `admin / admin`
2. Enable `kafka_spark_etl` DAG → triggers every 15 minutes
3. Or click **Trigger DAG** for an immediate run

**Via CLI (standalone, no Airflow):**
```bash
# Step 1: Produce events to Kafka topics
python kafka_streaming/producer.py --entity all

# Step 2: Consume Kafka → write Bronze JSON to MinIO
python kafka_streaming/consumer.py

# Step 3: PySpark Gold transform (MinIO Silver → Gold)
python spark/spark_transform.py

# Step 4: Load Gold Parquet into PostgreSQL
python etl/load_warehouse.py
```

### 5 — Run the classic batch pipeline (daily)

```bash
# All steps in one sequence:
python api_ingestion/fetch_orders.py
python api_ingestion/fetch_customers.py
python api_ingestion/fetch_products.py
python etl/clean_data.py
python etl/transform.py
python etl/validate.py
python etl/load_warehouse.py
python anomaly_detection/anomaly.py
python forecasting/forecast.py
```

---

## Kafka Topics

| Topic              | Partitions | Retention | Contents                              |
|--------------------|-----------|-----------|---------------------------------------|
| `orders-raw`       | 3         | 7 days    | Order/cart events (JSON, Pydantic schema) |
| `customers-raw`    | 3         | 7 days    | Customer events (PII SHA-256 hashed)  |
| `products-raw`     | 3         | 7 days    | Product catalogue events              |
| `pipeline-events`  | 1         | 30 days   | Pipeline lifecycle audit events       |

**Producer:** `KafkaEventProducer` (confluent_kafka preferred, kafka-python fallback)
**Consumer:** `KafkaMinIOConsumer` (batch mode, Airflow-friendly, timeout-based)

---

## PySpark Gold Transform

The `spark/spark_transform.py` module is the distributed counterpart of `etl/transform.py`.

| Feature             | Pandas (etl/transform.py) | PySpark (spark/spark_transform.py) |
|---------------------|---------------------------|------------------------------------|
| Engine              | Single-node               | Distributed (Spark cluster or local[*]) |
| Date spine          | pd.date_range             | Spark SQL `sequence()` function    |
| Aggregations        | GroupBy + lambda          | Spark DataFrames + native functions|
| Output              | Parquet via pyarrow       | Parquet via S3A connector          |
| Scale               | ~millions of rows         | Billions of rows                   |

### S3A connector config (MinIO)

```python
spark.hadoop.fs.s3a.endpoint           = http://minio:9000
spark.hadoop.fs.s3a.path.style.access  = true        # required for MinIO
spark.hadoop.fs.s3a.access.key         = minioadmin
spark.hadoop.fs.s3a.secret.key         = minioadmin
```

---

## Data Lake Layers

| Layer    | Format  | MinIO Prefix                                | Written by           |
|----------|---------|---------------------------------------------|----------------------|
| Bronze   | JSON    | `raw/{entity}/date=YYYY-MM-DD/*.json`       | fetch_*.py / Kafka consumer |
| Silver   | Parquet | `processed/{entity}/date=YYYY-MM-DD/*.parquet` | etl/clean_data.py |
| Gold     | Parquet | `processed/gold/{table}/date=YYYY-MM-DD/`   | etl/transform.py **or** spark/spark_transform.py |
| Reports  | JSON    | `quality_reports/date=YYYY-MM-DD/*.json`    | etl/validate.py      |
| Forecast | Parquet | `processed/forecasts/*.parquet`             | forecasting/forecast.py |

---

## Warehouse Tables

| Object                | Type      | Description                                         |
|-----------------------|-----------|-----------------------------------------------------|
| `dim_customer`        | Dimension | SCD Type 1. PII SHA-256 hashed                      |
| `dim_product`         | Dimension | Product catalogue with value score                  |
| `dim_date`            | Dimension | Date spine, YYYYMMDD integer key                    |
| `fact_sales`          | Fact      | Order line items (order × product grain)            |
| `agg_daily_sales`     | Aggregate | Daily revenue KPIs                                  |
| `agg_product_perf`    | Aggregate | Revenue, units sold per product                     |
| `agg_customer_ltv`    | Aggregate | Lifetime value tier per customer                    |
| `anomaly_log`         | Ops       | Flagged anomalies with severity                     |
| `forecast_results`    | Ops       | 30-day revenue forecast with confidence bands       |
| `load_audit`          | Ops       | Load audit trail per table per run                  |
| `v_daily_revenue`     | View      | Daily revenue joined with dim_date                  |
| `v_monthly_revenue`   | View      | Monthly rollup for trend charts                     |
| `v_category_revenue`  | View      | Revenue by product category                         |
| `v_sales_detail`      | View      | Full drill-through with dimension labels            |
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

| Check                | Method  | Threshold                           |
|----------------------|---------|-------------------------------------|
| Revenue outliers     | Z-score | > 3σ from order revenue mean        |
| Quantity outliers    | IQR     | > Q3 + 1.5×IQR                      |
| Price discrepancy    | % diff  | Avg sold price > 10% from catalogue |
| Daily revenue drop   | Rolling | > 40% below 7-day rolling average   |
| Customer spend spike | Z-score | Orders or spend > 3σ                |

---

## Forecasting

Uses **Holt-Winters Exponential Smoothing** (additive trend + weekly seasonality).

- Horizon: 30 days  ·  Confidence: 95%
- Fallback: linear regression when < 14 days of history
- Results: `forecast_results` table + `processed/forecasts/` Parquet

---

## Key Design Principles

| Principle            | Implementation                                              |
|----------------------|-------------------------------------------------------------|
| **Separation of concerns** | Each layer (Bronze/Silver/Gold/Load) independently runnable |
| **Idempotency**      | All loads use `INSERT … ON CONFLICT DO UPDATE`              |
| **PII protection**   | Customer email/phone SHA-256 hashed at Kafka producer level |
| **Data lineage**     | Bronze files preserved indefinitely for replay              |
| **Dual pipelines**   | Batch (daily) + Streaming (15-min) share the same warehouse |
| **Graceful fallback**| Kafka clients: confluent_kafka → kafka-python → dry-run     |
| **Observability**    | `load_audit` + `anomaly_log` + Kafka-UI + Spark UI          |
| **Quality gates**    | `validate.py` raises `DataQualityError` blocking the load   |
