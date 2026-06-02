# spark/spark_transform.py
"""
PySpark Gold Transform — Enterprise Data Platform
══════════════════════════════════════════════════
Distributed replacement for etl/transform.py.

Reads Silver Parquet from MinIO via S3A connector, applies business-level
transformations using Spark DataFrames + Spark SQL, then writes Gold Parquet
back to MinIO.

Gold tables produced
────────────────────
  dim_customer      Customer dimension with SCD Type 1 fields
  dim_product       Product dimension with value score
  dim_date          Date spine (generated with Spark sequence)
  fact_sales        Central fact: order × product grain
  agg_daily_sales   Pre-aggregated daily KPIs
  agg_product_perf  Product-level revenue and quantity
  agg_customer_ltv  Customer lifetime value with spend tiers

Usage (standalone)::

    python spark/spark_transform.py

Usage (from Airflow)::

    from spark.spark_transform import run_spark_transform
    keys = run_spark_transform(run_ts=datetime.now(timezone.utc))
"""

from __future__ import annotations

import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from config import LAKE, LOG_LEVEL, LOG_FORMAT
from spark.spark_session import get_spark_session, stop_spark

logging.basicConfig(level=LOG_LEVEL, format=LOG_FORMAT)
log = logging.getLogger("spark.transform")

GOLD_PREFIX = f"{LAKE.processed_prefix}/gold"
S3_BUCKET   = f"s3a://{LAKE.bucket}"


# ── S3A path helpers ───────────────────────────────────────────────────────────

def _silver_path(entity: str, date_str: str) -> str:
    return f"{S3_BUCKET}/{LAKE.processed_prefix}/{entity}/date={date_str}/"


def _gold_path(table: str, date_str: str) -> str:
    return f"{S3_BUCKET}/{GOLD_PREFIX}/{table}/date={date_str}/"


# ── Silver readers ─────────────────────────────────────────────────────────────

def _read_silver(spark, entity: str, date_str: str):
    """Read Silver Parquet for an entity from MinIO. Returns Spark DataFrame."""
    path = _silver_path(entity, date_str)
    log.info("Reading Silver | entity=%s  path=%s", entity, path)
    df = spark.read.parquet(path)
    log.info("Loaded %s: %d rows", entity, df.count())
    return df


# ── Dimension builders ─────────────────────────────────────────────────────────

def _build_dim_customer(customers):
    """SCD Type 1 customer dimension."""
    from pyspark.sql import functions as F

    df = (
        customers
        .withColumnRenamed("customer_id", "customer_sk")
        .withColumn("customer_sk", F.col("customer_sk").cast("integer"))
        .withColumn("effective_from", F.lit(datetime.now(timezone.utc).date().isoformat()))
        .withColumn("is_current", F.lit(True))
    )
    cols = [
        "customer_sk", "username", "full_name",
        "email_hash", "phone_hash",
        "city", "zipcode", "geo_lat", "geo_long",
        "effective_from", "is_current",
    ]
    existing = [c for c in cols if c in df.columns]
    log.info("dim_customer: %d rows", df.count())
    return df.select(existing)


def _build_dim_product(products):
    """Product dimension with computed value_score."""
    from pyspark.sql import functions as F

    max_count = products.agg(F.max("rating_count")).collect()[0][0] or 1

    df = (
        products
        .withColumnRenamed("product_id", "product_sk")
        .withColumn("product_sk", F.col("product_sk").cast("integer"))
        .withColumn(
            "value_score",
            F.round(
                F.col("rating_score") * 0.6
                + (F.col("rating_count") / max_count) * 0.4,
                4,
            ),
        )
    )
    cols = [
        "product_sk", "title", "category", "price", "price_tier",
        "rating_score", "rating_count", "value_score",
    ]
    existing = [c for c in cols if c in df.columns]
    log.info("dim_product: %d rows", df.count())
    return df.select(existing)


def _build_dim_date(spark, orders):
    """
    Date dimension spanning the order date range ±30 days.
    Uses Spark's sequence() to generate dates — no Pandas dependency.
    """
    from pyspark.sql import functions as F
    from pyspark.sql.types import DateType

    date_col = F.col("order_date").cast(DateType())
    bounds   = orders.agg(
        F.min(date_col).alias("min_d"),
        F.max(date_col).alias("max_d"),
    ).collect()[0]

    if bounds["min_d"] is None:
        from datetime import date, timedelta
        today = date.today()
        start = (today.replace(year=today.year - 1)).isoformat()
        end   = today.isoformat()
    else:
        from datetime import timedelta
        start = (bounds["min_d"] - timedelta(days=30)).isoformat()
        end   = (bounds["max_d"] + timedelta(days=30)).isoformat()

    # Generate date spine with sequence
    df = spark.sql(f"""
        SELECT
            explode(sequence(date'{start}', date'{end}', interval 1 day)) AS full_date
    """)

    df = (
        df
        .withColumn("date_sk",      F.date_format("full_date", "yyyyMMdd").cast("integer"))
        .withColumn("year",          F.year("full_date"))
        .withColumn("quarter",       F.quarter("full_date"))
        .withColumn("month",         F.month("full_date"))
        .withColumn("month_name",    F.date_format("full_date", "MMMM"))
        .withColumn("week",          F.weekofyear("full_date"))
        .withColumn("day_of_month",  F.dayofmonth("full_date"))
        .withColumn("day_of_week",   F.dayofweek("full_date"))   # 1=Sunday
        .withColumn("day_name",      F.date_format("full_date", "EEEE"))
        .withColumn("is_weekend",    F.dayofweek("full_date").isin(1, 7))
        .withColumn("is_month_end",  F.col("full_date") == F.last_day("full_date"))
        .withColumn("full_date",     F.col("full_date").cast("string"))
    )
    log.info("dim_date: %d rows  (%s → %s)", df.count(), start, end)
    return df


# ── Fact & aggregate builders ──────────────────────────────────────────────────

def _build_fact_sales(orders, products):
    """Join order line-items with product prices → sales fact."""
    from pyspark.sql import functions as F

    prod_ref = (
        products
        .select("product_id", "price", "category", "price_tier")
        .withColumnRenamed("product_id", "_prod_id")
    )

    df = (
        orders
        .join(prod_ref, orders["product_id"] == prod_ref["_prod_id"], how="left")
        .drop("_prod_id")
        .withColumn("unit_price",    F.coalesce(F.col("price"), F.lit(0.0)))
        .withColumn("quantity",      F.col("quantity").cast("integer"))
        .withColumn("revenue",       F.round(F.col("unit_price") * F.col("quantity"), 2))
        .withColumn("discount_rate", F.lit(0.0))
        .withColumn("net_revenue",   F.round(F.col("revenue") * (1 - F.col("discount_rate")), 2))
        .withColumn(
            "date_sk",
            F.date_format(F.col("order_date").cast("date"), "yyyyMMdd").cast("integer"),
        )
        .withColumnRenamed("order_id",   "order_sk")
        .withColumnRenamed("user_id",    "customer_sk")
        .withColumnRenamed("product_id", "product_sk")
    )

    cols = [
        "order_sk", "customer_sk", "product_sk", "date_sk",
        "quantity", "unit_price", "revenue", "discount_rate", "net_revenue",
        "category", "price_tier", "order_date",
    ]
    existing = [c for c in cols if c in df.columns]
    log.info("fact_sales: %d rows", df.count())
    return df.select(existing)


def _build_agg_daily_sales(fact):
    from pyspark.sql import functions as F

    df = (
        fact
        .groupBy("date_sk")
        .agg(
            F.countDistinct("order_sk").alias("total_orders"),
            F.sum("quantity").alias("total_items"),
            F.round(F.sum("revenue"),     2).alias("gross_revenue"),
            F.round(F.sum("net_revenue"), 2).alias("net_revenue"),
            F.round(F.avg("revenue"),     2).alias("avg_order_value"),
        )
        .orderBy("date_sk")
    )
    log.info("agg_daily_sales: %d date buckets", df.count())
    return df


def _build_agg_product_perf(fact, products):
    from pyspark.sql import functions as F

    agg = (
        fact
        .groupBy("product_sk")
        .agg(
            F.sum("quantity").alias("total_qty_sold"),
            F.round(F.sum("net_revenue"), 2).alias("total_revenue"),
            F.countDistinct("order_sk").alias("order_count"),
        )
    )

    prod_meta = (
        products
        .select("product_id", "title", "category", "price", "rating_score", "rating_count")
        .withColumnRenamed("product_id", "product_sk")
    )

    df = (
        agg
        .join(prod_meta, on="product_sk", how="left")
        .withColumn(
            "revenue_per_unit",
            F.round(F.col("total_revenue") / F.when(F.col("total_qty_sold") == 0, 1)
                    .otherwise(F.col("total_qty_sold")), 2),
        )
    )
    log.info("agg_product_perf: %d products", df.count())
    return df


def _build_agg_customer_ltv(fact):
    from pyspark.sql import functions as F

    df = (
        fact
        .groupBy("customer_sk")
        .agg(
            F.countDistinct("order_sk").alias("total_orders"),
            F.sum("quantity").alias("total_items"),
            F.round(F.sum("net_revenue"), 2).alias("total_spend"),
            F.min("order_date").alias("first_order"),
            F.max("order_date").alias("last_order"),
        )
        .withColumn(
            "avg_order_value",
            F.round(F.col("total_spend") / F.when(F.col("total_orders") == 0, 1)
                    .otherwise(F.col("total_orders")), 2),
        )
        .withColumn(
            "ltv_tier",
            F.when(F.col("total_spend") < 50,   "low")
             .when(F.col("total_spend") < 200,   "medium")
             .when(F.col("total_spend") < 500,   "high")
             .otherwise("vip"),
        )
    )
    log.info("agg_customer_ltv: %d customers", df.count())
    return df


# ── Gold writer ────────────────────────────────────────────────────────────────

def _write_gold(df, table: str, date_str: str) -> str:
    """Write a Spark DataFrame as snappy Parquet to MinIO Gold layer."""
    path = _gold_path(table, date_str)
    (
        df.coalesce(1)          # single file per run (suitable for this data volume)
        .write
        .mode("overwrite")
        .option("compression", "snappy")
        .parquet(path)
    )
    log.info("Gold → %s  (%s)", path, table)
    return path


# ── Public entry-point ─────────────────────────────────────────────────────────

def run_spark_transform(run_ts: datetime | None = None) -> dict[str, str]:
    """
    Execute the full PySpark Gold transform.

    Parameters
    ----------
    run_ts : datetime, optional
        Logical run timestamp (defaults to now UTC).

    Returns
    -------
    dict mapping table_name → S3 path of written Gold Parquet directory.
    """
    run_ts   = run_ts or datetime.now(timezone.utc)
    date_str = run_ts.strftime("%Y-%m-%d")

    log.info("PySpark Gold transform start  date=%s", date_str)
    spark = get_spark_session()

    # ── Load Silver ────────────────────────────────────────────────────────────
    orders    = _read_silver(spark, "orders",    date_str)
    customers = _read_silver(spark, "customers", date_str)
    products  = _read_silver(spark, "products",  date_str)

    # ── Build Gold tables ──────────────────────────────────────────────────────
    dim_customer    = _build_dim_customer(customers)
    dim_product     = _build_dim_product(products)
    dim_date        = _build_dim_date(spark, orders)
    fact_sales      = _build_fact_sales(orders, products)
    agg_daily       = _build_agg_daily_sales(fact_sales)
    agg_product     = _build_agg_product_perf(fact_sales, products)
    agg_ltv         = _build_agg_customer_ltv(fact_sales)

    # ── Write Gold ─────────────────────────────────────────────────────────────
    tables = {
        "dim_customer":    dim_customer,
        "dim_product":     dim_product,
        "dim_date":        dim_date,
        "fact_sales":      fact_sales,
        "agg_daily_sales": agg_daily,
        "agg_product_perf": agg_product,
        "agg_customer_ltv": agg_ltv,
    }

    keys: dict[str, str] = {}
    for table, df in tables.items():
        keys[table] = _write_gold(df, table, date_str)

    log.info("PySpark Gold transform complete. %d tables written.", len(keys))
    return keys


# ── CLI ────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    result = run_spark_transform()
    for table, path in result.items():
        print(f"  {table:25s} → {path}")
    stop_spark()
