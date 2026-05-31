# api_ingestion/fetch_products.py
"""
Ingestion — Products
════════════════════
Fetches product catalogue from the API and writes raw JSON to:
  raw/products/date=YYYY-MM-DD/products_<timestamp>.json

Products include: id, title, price, description, category, image, rating.
We capture all fields as-is and add category-level enrichment metadata.
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
log = logging.getLogger("ingestion.products")


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


def _enrich(products: list[dict]) -> list[dict]:
    """Compute price tier and rating band at ingest time."""
    for p in products:
        price = float(p.get("price", 0))
        rating_count = p.get("rating", {}).get("count", 0)
        rating_score = p.get("rating", {}).get("rate", 0)

        # Price tier
        if price < 20:
            p["_price_tier"] = "budget"
        elif price < 100:
            p["_price_tier"] = "mid"
        else:
            p["_price_tier"] = "premium"

        # Popularity band based on review count
        if rating_count >= 400:
            p["_popularity"] = "high"
        elif rating_count >= 200:
            p["_popularity"] = "medium"
        else:
            p["_popularity"] = "low"

        p["_rating_score"] = rating_score
        p["_rating_count"] = rating_count
        p["_ingested_at"]  = datetime.now(timezone.utc).isoformat()
    return products


def _write_raw(client, products: list[dict], run_ts: datetime) -> str:
    date_str = run_ts.strftime("%Y-%m-%d")
    ts_str   = run_ts.strftime("%Y%m%dT%H%M%SZ")
    key      = f"{LAKE.raw_prefix}/products/date={date_str}/products_{ts_str}.json"

    # Compute category breakdown for metadata
    categories: dict[str, int] = {}
    for p in products:
        cat = p.get("category", "unknown")
        categories[cat] = categories.get(cat, 0) + 1

    payload = {
        "_meta": {
            "entity":         "products",
            "source":         f"{API.base_url}{API.endpoints['products']}",
            "fetched_at":     run_ts.isoformat(),
            "record_count":   len(products),
            "categories":     categories,
            "schema_version": "1.0",
        },
        "data": products,
    }

    body = json.dumps(payload, indent=2, ensure_ascii=False).encode("utf-8")
    client.put_object(
        Bucket=LAKE.bucket, Key=key,
        Body=body, ContentType="application/json",
        Metadata={"entity": "products", "date": date_str},
    )
    log.info("Raw written → s3://%s/%s (%d bytes)", LAKE.bucket, key, len(body))
    return key


def fetch_products(run_ts: datetime | None = None) -> dict:
    """
    Fetch products from API and persist raw JSON to Data Lake.
    Returns: { "key": s3_key, "record_count": int, "categories": dict }
    """
    run_ts = run_ts or datetime.now(timezone.utc)
    url    = f"{API.base_url}{API.endpoints['products']}"

    client = _s3()
    _ensure_bucket(client)

    raw     = _get_with_retry(url)
    records = _enrich(raw)
    key     = _write_raw(client, records, run_ts)

    categories: dict[str, int] = {}
    for p in records:
        cat = p.get("category", "unknown")
        categories[cat] = categories.get(cat, 0) + 1

    log.info("Products ingestion complete — %d records, %d categories → %s",
             len(records), len(categories), key)
    return {"key": key, "record_count": len(records), "categories": categories}


if __name__ == "__main__":
    result = fetch_products()
    print(f"\nDone: {result['record_count']} products → {result['key']}")
    for cat, count in result["categories"].items():
        print(f"  {cat}: {count}")
