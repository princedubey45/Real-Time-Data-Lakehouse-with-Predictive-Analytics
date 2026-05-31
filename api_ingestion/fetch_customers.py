# api_ingestion/fetch_customers.py
"""
Ingestion — Customers
═════════════════════
Fetches user / customer records from the API and writes raw JSON to:
  raw/customers/date=YYYY-MM-DD/customers_<timestamp>.json

PII Notice: raw records contain name, email, phone, address.
The Silver layer (clean_data.py) will hash PII fields before
writing processed data. Raw files are retained for audit purposes
and should be stored with restricted bucket policies in production.
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
log = logging.getLogger("ingestion.customers")


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


def _get_with_retry(url: str) -> list[dict]:
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
    raise RuntimeError(f"All {API.max_retries} attempts failed") from last_exc


def _enrich(customers: list[dict]) -> list[dict]:
    """Add ingestion metadata without modifying original fields."""
    for c in customers:
        # Flatten address sub-object for easier downstream access
        addr = c.get("address", {})
        c["_city"]       = addr.get("city", "")
        c["_state"]      = addr.get("zipcode", "")
        c["_geo_lat"]    = addr.get("geolocation", {}).get("lat", None)
        c["_geo_long"]   = addr.get("geolocation", {}).get("long", None)
        c["_ingested_at"] = datetime.now(timezone.utc).isoformat()
        # Flag records with missing critical fields for downstream validation
        c["_has_email"]  = bool(c.get("email"))
        c["_has_phone"]  = bool(c.get("phone"))
    return customers


def _write_raw(client, customers: list[dict], run_ts: datetime) -> str:
    date_str = run_ts.strftime("%Y-%m-%d")
    ts_str   = run_ts.strftime("%Y%m%dT%H%M%SZ")
    key      = f"{LAKE.raw_prefix}/customers/date={date_str}/customers_{ts_str}.json"

    payload = {
        "_meta": {
            "entity":         "customers",
            "source":         f"{API.base_url}{API.endpoints['customers']}",
            "fetched_at":     run_ts.isoformat(),
            "record_count":   len(customers),
            "schema_version": "1.0",
            "pii_present":    True,
        },
        "data": customers,
    }

    body = json.dumps(payload, indent=2, ensure_ascii=False).encode("utf-8")
    client.put_object(
        Bucket=LAKE.bucket, Key=key,
        Body=body, ContentType="application/json",
        Metadata={"entity": "customers", "date": date_str, "pii": "true"},
    )
    log.info("Raw written → s3://%s/%s (%d bytes)", LAKE.bucket, key, len(body))
    return key


def fetch_customers(run_ts: datetime | None = None) -> dict:
    """
    Fetch customers from API and persist raw JSON to Data Lake.
    Returns: { "key": s3_key, "record_count": int }
    """
    run_ts = run_ts or datetime.now(timezone.utc)
    url    = f"{API.base_url}{API.endpoints['customers']}"

    client = _s3()
    _ensure_bucket(client)

    raw     = _get_with_retry(url)
    records = _enrich(raw)
    key     = _write_raw(client, records, run_ts)

    log.info("Customers ingestion complete — %d records → %s", len(records), key)
    return {"key": key, "record_count": len(records)}


if __name__ == "__main__":
    result = fetch_customers()
    print(f"\nDone: {result['record_count']} customers → {result['key']}")
