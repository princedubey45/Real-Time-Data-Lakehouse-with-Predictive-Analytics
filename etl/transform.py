# etl/transform.py
"""
ETL — Transform  (Gold Layer)
══════════════════════════════
Reads cleaned Parquet from processed/, applies business-level
transformations and joins, then writes Gold Parquet back to processed/gold/.

Gold tables produced
────────────────────
dim_customer    — Customer dimension with SCD Type 1 fields
dim_product     — Product dimension with category and price tier
dim_date        — Date spine covering the data range
fact_sales      — Central fact: one row per order line-item with
                  full measures (revenue, quantity, discounts)
agg_daily_sales — Pre-aggregated daily revenue for fast dashboard queries
agg_product_perf— Product-level performance: revenue, qty sold, avg rating
agg_customer_ltv— Customer lifetime value estimate
"""

import io
import logging
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import boto3
import pandas as pd
from botocore.client import Config

sys.path.insert(0, str(Path(__file__).parent.parent))
from config import LAKE, LOG_LEVEL, LOG_FORMAT

logging.basicConfig(level=LOG_LEVEL, format=LOG_FORMAT)
log = logging.getLogger("etl.transform")

GOLD_PREFIX = f"{LAKE.processed_prefix}/gold"


# ── MinIO helpers ──────────────────────────────────────────────────────────────

def _s3():
    return boto3.client(
        "s3",
        endpoint_url=LAKE.endpoint,
        aws_access_key_id=LAKE.access_key,
        aws_secret_access_key=LAKE.secret_key,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )


def _latest_processed(client, entity: str, date_str: str) -> pd.DataFrame:
    prefix = f"{LAKE.processed_prefix}/{entity}/date={date_str}/"
    resp   = client.list_objects_v2(Bucket=LAKE.bucket, Prefix=prefix)
    objs   = resp.get("Contents", [])
    if not objs:
        raise FileNotFoundError(f"No processed Parquet at {LAKE.bucket}/{prefix}")
    key = sorted(objs, key=lambda o: o["LastModified"], reverse=True)[0]["Key"]
    obj = client.get_object(Bucket=LAKE.bucket, Key=key)
    df  = pd.read_parquet(io.BytesIO(obj["Body"].read()), engine="pyarrow")
    log.info("Loaded %s: %d rows", entity, len(df))
    return df


def _write_gold(client, table: str, df: pd.DataFrame, run_ts: datetime) -> str:
    date_str = run_ts.strftime("%Y-%m-%d")
    ts_str   = run_ts.strftime("%Y%m%dT%H%M%SZ")
    key      = f"{GOLD_PREFIX}/{table}/date={date_str}/{table}_{ts_str}.parquet"

    buf = io.BytesIO()
    df.to_parquet(buf, index=False, engine="pyarrow")
    buf.seek(0)
    client.put_object(
        Bucket=LAKE.bucket, Key=key,
        Body=buf.getvalue(), ContentType="application/octet-stream",
        Metadata={"table": table, "rows": str(len(df))},
    )
    log.info("Gold written → s3://%s/%s  (%d rows)", LAKE.bucket, key, len(df))
    return key


# ── Dimension builders ─────────────────────────────────────────────────────────

def _build_dim_customer(customers: pd.DataFrame) -> pd.DataFrame:
    df = customers.rename(columns={"customer_id": "customer_sk"}).copy()
    df["customer_sk"] = df["customer_sk"].astype(int)
    df["effective_from"] = datetime.now(timezone.utc).date().isoformat()
    df["is_current"]     = True
    col_order = [
        "customer_sk", "username", "full_name",
        "email_hash", "phone_hash", "has_email", "has_phone",
        "city", "zipcode", "geo_lat", "geo_long",
        "effective_from", "is_current",
    ]
    return df[[c for c in col_order if c in df.columns]]


def _build_dim_product(products: pd.DataFrame) -> pd.DataFrame:
    df = products.rename(columns={"product_id": "product_sk"}).copy()
    df["product_sk"] = df["product_sk"].astype(int)
    # Compute value score: combined rating weight and popularity
    df["value_score"] = (
        df["rating_score"] * 0.6 +
        (df["rating_count"] / df["rating_count"].max()) * 0.4
    ).round(4)
    col_order = [
        "product_sk", "title", "category", "price", "price_tier",
        "rating_score", "rating_count", "popularity", "value_score", "description_len",
    ]
    return df[[c for c in col_order if c in df.columns]]


def _build_dim_date(orders: pd.DataFrame) -> pd.DataFrame:
    """Generate a date dimension spanning the order date range ± 30 days."""
    valid_dates = orders["order_date"].dropna()
    if valid_dates.empty:
        start = datetime.now(timezone.utc) - timedelta(days=365)
        end   = datetime.now(timezone.utc)
    else:
        start = valid_dates.min() - timedelta(days=30)
        end   = valid_dates.max() + timedelta(days=30)

    date_range = pd.date_range(start=start, end=end, freq="D", tz="UTC")
    df = pd.DataFrame({"full_date": date_range})
    df["date_sk"]       = df["full_date"].dt.strftime("%Y%m%d").astype(int)
    df["year"]          = df["full_date"].dt.year
    df["quarter"]       = df["full_date"].dt.quarter
    df["month"]         = df["full_date"].dt.month
    df["month_name"]    = df["full_date"].dt.strftime("%B")
    df["week"]          = df["full_date"].dt.isocalendar().week.astype(int)
    df["day_of_month"]  = df["full_date"].dt.day
    df["day_of_week"]   = df["full_date"].dt.dayofweek        # 0=Monday
    df["day_name"]      = df["full_date"].dt.strftime("%A")
    df["is_weekend"]    = df["day_of_week"].isin([5, 6])
    df["is_month_end"]  = df["full_date"].dt.is_month_end
    df["full_date"]     = df["full_date"].dt.date.astype(str)
    log.info("dim_date: %d rows (%s to %s)", len(df), df["full_date"].min(), df["full_date"].max())
    return df


# ── Fact and aggregate builders ────────────────────────────────────────────────

def _build_fact_sales(orders: pd.DataFrame, products: pd.DataFrame) -> pd.DataFrame:
    """
    Join order line-items with product prices to produce a full sales fact.
    Each row = one product on one order.
    """
    prod_prices = products[["product_id", "price", "category", "price_tier"]].rename(
        columns={"product_id": "product_id_ref"}
    )

    df = orders.copy()
    df = df.merge(
        prod_prices,
        left_on="product_id", right_on="product_id_ref", how="left",
    )
    df = df.drop(columns=["product_id_ref"], errors="ignore")

    # Measures
    df["unit_price"]    = df["price"].fillna(0.0)
    df["revenue"]       = (df["unit_price"] * df["quantity"]).round(2)
    df["discount_rate"] = 0.0    # placeholder — real discount data would come from API
    df["net_revenue"]   = (df["revenue"] * (1 - df["discount_rate"])).round(2)

    # Date key for joining dim_date
    df["date_sk"] = (
        pd.to_datetime(df["order_date"], utc=True, errors="coerce")
        .dt.strftime("%Y%m%d")
    )
    df["date_sk"] = pd.to_numeric(df["date_sk"], errors="coerce").astype("Int64")

    # Surrogate keys
    df = df.rename(columns={
        "order_id":   "order_sk",
        "user_id":    "customer_sk",
        "product_id": "product_sk",
    })

    col_order = [
        "order_sk", "customer_sk", "product_sk", "date_sk",
        "quantity", "unit_price", "revenue", "discount_rate", "net_revenue",
        "category", "price_tier", "order_date",
    ]
    return df[[c for c in col_order if c in df.columns]]


def _build_agg_daily_sales(fact: pd.DataFrame) -> pd.DataFrame:
    df = (
        fact.groupby("date_sk", as_index=False)
        .agg(
            total_orders=("order_sk",    "nunique"),
            total_items=("quantity",      "sum"),
            gross_revenue=("revenue",     "sum"),
            net_revenue=("net_revenue",   "sum"),
            avg_order_value=("revenue",   lambda x: (
                x.groupby(fact.loc[x.index, "order_sk"]).sum().mean()
            )),
        )
    )
    df["gross_revenue"]   = df["gross_revenue"].round(2)
    df["net_revenue"]     = df["net_revenue"].round(2)
    df["avg_order_value"] = df["avg_order_value"].round(2)
    log.info("agg_daily_sales: %d date buckets", len(df))
    return df


def _build_agg_product_perf(fact: pd.DataFrame, products: pd.DataFrame) -> pd.DataFrame:
    agg = (
        fact.groupby("product_sk", as_index=False)
        .agg(
            total_qty_sold=("quantity",    "sum"),
            total_revenue=("net_revenue",  "sum"),
            order_count=("order_sk",       "nunique"),
        )
    )
    prod_meta = products[["product_id", "title", "category", "price", "rating_score", "rating_count"]].rename(
        columns={"product_id": "product_sk"}
    )
    df = agg.merge(prod_meta, on="product_sk", how="left")
    df["revenue_per_unit"] = (df["total_revenue"] / df["total_qty_sold"].replace(0, 1)).round(2)
    df["total_revenue"]    = df["total_revenue"].round(2)
    log.info("agg_product_perf: %d products", len(df))
    return df


def _build_agg_customer_ltv(fact: pd.DataFrame) -> pd.DataFrame:
    df = (
        fact.groupby("customer_sk", as_index=False)
        .agg(
            total_orders=("order_sk",     "nunique"),
            total_items=("quantity",       "sum"),
            total_spend=("net_revenue",    "sum"),
            first_order=("order_date",     "min"),
            last_order=("order_date",      "max"),
        )
    )
    df["total_spend"]      = df["total_spend"].round(2)
    df["avg_order_value"]  = (df["total_spend"] / df["total_orders"].replace(0, 1)).round(2)
    # Simple LTV tier
    df["ltv_tier"] = pd.cut(
        df["total_spend"],
        bins=[0, 50, 200, 500, float("inf")],
        labels=["low", "medium", "high", "vip"],
        right=False,
    )
    log.info("agg_customer_ltv: %d customers", len(df))
    return df


# ── Public callable ────────────────────────────────────────────────────────────

def run_transform(run_ts: datetime | None = None) -> dict[str, str]:
    """
    Build all Gold tables from processed Silver data.
    Returns mapping: table_name → S3 key.
    """
    run_ts   = run_ts or datetime.now(timezone.utc)
    date_str = run_ts.strftime("%Y-%m-%d")
    client   = _s3()

    log.info("Loading Silver DataFrames…")
    orders    = _latest_processed(client, "orders",    date_str)
    customers = _latest_processed(client, "customers", date_str)
    products  = _latest_processed(client, "products",  date_str)

    # Rename processed col for joins
    products_join = products.rename(columns={"product_id": "product_id"}).copy()
    # Ensure product_id column exists (rename from product_sk if needed)
    if "product_sk" in products_join.columns and "product_id" not in products_join.columns:
        products_join["product_id"] = products_join["product_sk"]

    tables = {
        "dim_customer":      _build_dim_customer(customers),
        "dim_product":       _build_dim_product(products),
        "dim_date":          _build_dim_date(orders),
        "fact_sales":        _build_fact_sales(orders, products),
    }

    # Aggregates built from fact_sales
    tables["agg_daily_sales"]    = _build_agg_daily_sales(tables["fact_sales"])
    tables["agg_product_perf"]   = _build_agg_product_perf(tables["fact_sales"], products)
    tables["agg_customer_ltv"]   = _build_agg_customer_ltv(tables["fact_sales"])

    keys = {}
    for table, df in tables.items():
        keys[table] = _write_gold(client, table, df, run_ts)

    log.info("Transform complete. %d Gold tables written.", len(keys))
    return keys


if __name__ == "__main__":
    result = run_transform()
    for table, key in result.items():
        print(f"  {table:25s} → {key}")
