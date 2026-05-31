# api_ingestion/fetch_orders.py
"""
Ingestion — Orders
══════════════════
Fetches cart / order data from the API and writes raw JSON to the
MinIO Data Lake under:
  raw/orders/date=YYYY-MM-DD/orders_<timestamp>.json

Each JSON file contains:
  {
    "_meta": { source, fetched_at, record_count },
    "data":  [ ...raw order records... ]
  }

Can be run standalone or called by the Airflow DAG as a Python callable.
"""

import json
import logging
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import boto3
import requests
from botocore.client import Config
from botocore.exceptions import ClientError

sys.path.insert(0, str(Path(__file__).parent.parent))
from config import API, LAKE, LOG_LEVEL, LOG_FORMAT

logging.basicConfig(level=LOG_LEVEL, format=LOG_FORMAT)
log = logging.getLogger("ingestion.orders")


# ── MinIO ──────────────────────────────────────────────────────────────────────

def _s3():
    return boto3.client(
        "s3",
        endpoint_url=LAKE.endpoint,
        aws_access_key_id=LAKE.access_key,
        aws_secret_access_key=LAKE.secret_key,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )


def _ensure_bucket(client) -> None:
    try:
        client.head_bucket(Bucket=LAKE.bucket)
    except ClientError:
        client.create_bucket(Bucket=LAKE.bucket)
        log.info("Created bucket: %s", LAKE.bucket)


# ── HTTP fetch with retry ──────────────────────────────────────────────────────

def _get_with_retry(url: str) -> list[dict]:
    """GET url with exponential backoff retry. Returns parsed JSON list."""
    last_exc = None
    for attempt in range(1, API.max_retries + 1):
        try:
            log.info("GET %s (attempt %d/%d)", url, attempt, API.max_retries)
            resp = requests.get(url, timeout=API.timeout_sec)
            resp.raise_for_status()
            data = resp.json()
            if isinstance(data, dict):
                data = [data]
            log.info("  ✓ %d records", len(data))
            return data
        except requests.RequestException as exc:
            last_exc = exc
            wait = API.retry_backoff ** attempt
            log.warning("  Attempt %d failed: %s — retrying in %.0fs", attempt, exc, wait)
            time.sleep(wait)
    raise RuntimeError(f"All {API.max_retries} attempts failed for {url}") from last_exc


# ── Enrich order records ───────────────────────────────────────────────────────

def _enrich(orders: list[dict]) -> list[dict]:
    """
    Add computed fields to raw order records before writing to Bronze.
    We do NOT clean or transform here — that belongs in the ETL layer.
    We only add ingestion-time metadata that would otherwise be lost.
    """
    for order in orders:
        # Flatten product list into item_count and total_quantity
        products = order.get("products", [])
        order["_item_count"]     = len(products)
        order["_total_quantity"] = sum(p.get("quantity", 0) for p in products)
        order["_ingested_at"]    = datetime.now(timezone.utc).isoformat()
    return orders


# ── Write to lake ──────────────────────────────────────────────────────────────

def _write_raw(client, orders: list[dict], run_ts: datetime) -> str:
    date_str = run_ts.strftime("%Y-%m-%d")
    ts_str   = run_ts.strftime("%Y%m%dT%H%M%SZ")
    key      = f"{LAKE.raw_prefix}/orders/date={date_str}/orders_{ts_str}.json"

    payload = {
        "_meta": {
            "entity":        "orders",
            "source":        f"{API.base_url}{API.endpoints['orders']}",
            "fetched_at":    run_ts.isoformat(),
            "record_count":  len(orders),
            "schema_version": "1.0",
        },
        "data": orders,
    }

    body = json.dumps(payload, indent=2, ensure_ascii=False).encode("utf-8")
    client.put_object(
        Bucket=LAKE.bucket, Key=key,
        Body=body, ContentType="application/json",
        Metadata={"entity": "orders", "date": date_str},
    )
    log.info("Raw written → s3://%s/%s (%d bytes)", LAKE.bucket, key, len(body))
    return key


# ── Public callable ────────────────────────────────────────────────────────────

def fetch_orders(run_ts: datetime | None = None) -> dict:
    """
    Fetch orders from API and persist raw JSON to Data Lake.
    Returns: { "key": s3_key, "record_count": int }
    """
    run_ts = run_ts or datetime.now(timezone.utc)
    url    = f"{API.base_url}{API.endpoints['orders']}"

    client = _s3()
    _ensure_bucket(client)

    raw     = _get_with_retry(url)
    records = _enrich(raw)
    key     = _write_raw(client, records, run_ts)

    log.info("Orders ingestion complete — %d records → %s", len(records), key)
    return {"key": key, "record_count": len(records)}


if __name__ == "__main__":
    result = fetch_orders()
    print(f"\nDone: {result['record_count']} orders → {result['key']}")
