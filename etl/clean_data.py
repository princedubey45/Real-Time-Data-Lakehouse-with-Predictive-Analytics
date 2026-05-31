# etl/clean_data.py
"""
ETL — Clean Data  (Silver Layer)
═════════════════════════════════
Reads raw JSON from the Data Lake, applies entity-specific cleaning rules,
and writes clean Parquet to the processed/ prefix.

Cleaning rules per entity
─────────────────────────
orders:
  • Flatten nested product list → explode to one row per line-item
  • Parse and standardise date field
  • Drop orders with zero products
  • Cast quantity and price to correct numeric types

customers:
  • Normalise name (title-case)
  • Validate and standardise email (lowercase, strip whitespace)
  • Hash email and phone for PII compliance in processed layer
  • Flatten address sub-object
  • Drop records missing both email and phone

products:
  • Strip leading/trailing whitespace from title/description
  • Clip price to sensible range [0.01, 99_999.99]
  • Validate rating is in [0.0, 5.0]
  • Normalise category to snake_case
"""

import hashlib
import io
import json
import logging
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

import boto3
import pandas as pd
from botocore.client import Config
from botocore.exceptions import ClientError

sys.path.insert(0, str(Path(__file__).parent.parent))
from config import LAKE, LOG_LEVEL, LOG_FORMAT

logging.basicConfig(level=LOG_LEVEL, format=LOG_FORMAT)
log = logging.getLogger("etl.clean")

EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")


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


def _latest_raw_key(client, entity: str, date_str: str) -> str:
    prefix = f"{LAKE.raw_prefix}/{entity}/date={date_str}/"
    resp   = client.list_objects_v2(Bucket=LAKE.bucket, Prefix=prefix)
    objs   = resp.get("Contents", [])
    if not objs:
        raise FileNotFoundError(f"No raw files at {LAKE.bucket}/{prefix}")
    return sorted(objs, key=lambda o: o["LastModified"], reverse=True)[0]["Key"]


def _read_raw(client, key: str) -> list[dict]:
    obj  = client.get_object(Bucket=LAKE.bucket, Key=key)
    body = json.loads(obj["Body"].read())
    return body["data"]


def _write_processed(client, entity: str, df: pd.DataFrame, run_ts: datetime) -> str:
    date_str = run_ts.strftime("%Y-%m-%d")
    ts_str   = run_ts.strftime("%Y%m%dT%H%M%SZ")
    key      = f"{LAKE.processed_prefix}/{entity}/date={date_str}/{entity}_{ts_str}.parquet"

    buf = io.BytesIO()
    df.to_parquet(buf, index=False, engine="pyarrow")
    buf.seek(0)
    client.put_object(
        Bucket=LAKE.bucket, Key=key,
        Body=buf.getvalue(), ContentType="application/octet-stream",
        Metadata={"entity": entity, "date": date_str, "rows": str(len(df))},
    )
    log.info("Processed written → s3://%s/%s  (%d rows)", LAKE.bucket, key, len(df))
    return key


# ── PII helpers ────────────────────────────────────────────────────────────────

def _sha256(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


# ── Entity cleaners ────────────────────────────────────────────────────────────

def _clean_orders(raw: list[dict]) -> pd.DataFrame:
    rows = []
    for order in raw:
        order_id    = order.get("id")
        user_id     = order.get("userId")
        raw_date    = order.get("date", "")
        products    = order.get("products", [])

        if not products:
            log.debug("Order %s has no products — skipping", order_id)
            continue

        # Parse date (ISO format from API)
        try:
            order_date = pd.to_datetime(raw_date, utc=True)
        except Exception:
            order_date = pd.NaT

        for product in products:
            rows.append({
                "order_id":      order_id,
                "user_id":       user_id,
                "order_date":    order_date,
                "product_id":    product.get("productId"),
                "quantity":      int(product.get("quantity", 0)),
            })

    df = pd.DataFrame(rows)
    if df.empty:
        return df

    # Type enforcement
    df["order_id"]   = pd.to_numeric(df["order_id"],   errors="coerce").astype("Int64")
    df["user_id"]    = pd.to_numeric(df["user_id"],    errors="coerce").astype("Int64")
    df["product_id"] = pd.to_numeric(df["product_id"], errors="coerce").astype("Int64")
    df["quantity"]   = pd.to_numeric(df["quantity"],   errors="coerce").fillna(0).astype(int)

    # Drop rows with missing keys
    before = len(df)
    df = df.dropna(subset=["order_id", "user_id", "product_id"])
    df = df[df["quantity"] > 0]
    log.info("Orders cleaned: %d → %d rows", before, len(df))
    return df


def _clean_customers(raw: list[dict]) -> pd.DataFrame:
    records = []
    for c in raw:
        name_obj = c.get("name", {})
        full_name = f"{name_obj.get('firstname', '')} {name_obj.get('lastname', '')}".strip().title()
        email     = (c.get("email") or "").strip().lower()
        phone     = (c.get("phone") or "").strip()
        address   = c.get("address", {})

        # Validate email
        valid_email = bool(EMAIL_RE.match(email)) if email else False
        if not valid_email and not phone:
            log.debug("Customer %s missing both email and phone — skipping", c.get("id"))
            continue

        records.append({
            "customer_id":   c.get("id"),
            "username":      c.get("username", ""),
            "full_name":     full_name,
            "email_hash":    _sha256(email) if valid_email else None,
            "phone_hash":    _sha256(phone) if phone else None,
            "has_email":     valid_email,
            "has_phone":     bool(phone),
            "city":          address.get("city", ""),
            "zipcode":       address.get("zipcode", ""),
            "geo_lat":       address.get("geolocation", {}).get("lat"),
            "geo_long":      address.get("geolocation", {}).get("long"),
        })

    df = pd.DataFrame(records)
    if not df.empty:
        df["customer_id"] = pd.to_numeric(df["customer_id"], errors="coerce").astype("Int64")
        df = df.dropna(subset=["customer_id"])
        log.info("Customers cleaned: %d records", len(df))
    return df


def _clean_products(raw: list[dict]) -> pd.DataFrame:
    records = []
    for p in raw:
        price  = float(p.get("price", 0))
        rating = p.get("rating", {})
        score  = float(rating.get("rate", 0))
        count  = int(rating.get("count", 0))
        cat    = re.sub(r"[\s\-]+", "_", (p.get("category") or "unknown").lower().strip())

        records.append({
            "product_id":   p.get("id"),
            "title":        (p.get("title") or "").strip(),
            "category":     cat,
            "price":        max(0.01, min(price, 99_999.99)),
            "price_tier":   p.get("_price_tier", "mid"),
            "rating_score": max(0.0, min(score, 5.0)),
            "rating_count": max(0, count),
            "popularity":   p.get("_popularity", "low"),
            "description_len": len((p.get("description") or "").strip()),
        })

    df = pd.DataFrame(records)
    if not df.empty:
        df["product_id"] = pd.to_numeric(df["product_id"], errors="coerce").astype("Int64")
        df = df.dropna(subset=["product_id"])
        df = df[df["title"].str.len() > 0]
        log.info("Products cleaned: %d records", len(df))
    return df


CLEANERS = {
    "orders":    _clean_orders,
    "customers": _clean_customers,
    "products":  _clean_products,
}


# ── Public callable ────────────────────────────────────────────────────────────

def run_clean(run_ts: datetime | None = None) -> dict[str, str]:
    """
    Read raw JSON for all entities, clean, and write Parquet to processed/.
    Returns mapping: entity → S3 key.
    """
    run_ts   = run_ts or datetime.now(timezone.utc)
    date_str = run_ts.strftime("%Y-%m-%d")
    client   = _s3()

    keys = {}
    for entity, cleaner in CLEANERS.items():
        log.info("Cleaning: %s", entity)
        raw_key = _latest_raw_key(client, entity, date_str)
        raw     = _read_raw(client, raw_key)
        df      = cleaner(raw)
        keys[entity] = _write_processed(client, entity, df, run_ts)

    log.info("Clean complete. %d entities processed.", len(keys))
    return keys


if __name__ == "__main__":
    result = run_clean()
    for entity, key in result.items():
        print(f"  {entity:12s} → {key}")
