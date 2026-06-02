# spark/schemas.py
"""
PySpark StructType schemas for each Silver (processed) table.

Defining schemas explicitly (instead of inferring from Parquet) gives us:
  - Faster reads — Spark skips schema inference scan
  - Type safety — column type mismatches fail early with clear errors
  - Documentation — column names/types are self-explanatory

Usage::

    from spark.schemas import SILVER_SCHEMAS
    df = spark.read.schema(SILVER_SCHEMAS["orders"]).parquet(path)
"""

from __future__ import annotations

try:
    from pyspark.sql.types import (
        DoubleType, FloatType, IntegerType, LongType,
        StringType, StructField, StructType, TimestampType,
    )
except ImportError:
    # Allow import without PySpark installed (e.g. for IDE introspection)
    StructType = StructField = StringType = IntegerType = None    # type: ignore
    LongType = FloatType = DoubleType = TimestampType = None      # type: ignore


def _field(name: str, dtype, nullable: bool = True) -> "StructField":
    return StructField(name, dtype(), nullable)


# ── Silver: Orders ─────────────────────────────────────────────────────────────

ORDERS_SCHEMA = StructType([
    _field("order_id",      IntegerType, nullable=False),
    _field("user_id",       IntegerType),
    _field("order_date",    StringType),
    _field("product_id",    IntegerType),
    _field("quantity",      IntegerType),
    _field("ingested_at",   StringType),
    _field("source",        StringType),
]) if StructType else None


# ── Silver: Customers ──────────────────────────────────────────────────────────

CUSTOMERS_SCHEMA = StructType([
    _field("customer_id",   IntegerType, nullable=False),
    _field("username",      StringType),
    _field("full_name",     StringType),
    _field("email_hash",    StringType),
    _field("phone_hash",    StringType),
    _field("has_email",     StringType),
    _field("has_phone",     StringType),
    _field("city",          StringType),
    _field("zipcode",       StringType),
    _field("geo_lat",       DoubleType),
    _field("geo_long",      DoubleType),
    _field("ingested_at",   StringType),
]) if StructType else None


# ── Silver: Products ───────────────────────────────────────────────────────────

PRODUCTS_SCHEMA = StructType([
    _field("product_id",        IntegerType, nullable=False),
    _field("title",             StringType),
    _field("category",          StringType),
    _field("price",             DoubleType),
    _field("price_tier",        StringType),
    _field("rating_score",      DoubleType),
    _field("rating_count",      IntegerType),
    _field("popularity",        StringType),
    _field("description_len",   IntegerType),
    _field("ingested_at",       StringType),
]) if StructType else None


# ── Registry ───────────────────────────────────────────────────────────────────

SILVER_SCHEMAS: dict[str, "StructType"] = {
    "orders":    ORDERS_SCHEMA,
    "customers": CUSTOMERS_SCHEMA,
    "products":  PRODUCTS_SCHEMA,
}
