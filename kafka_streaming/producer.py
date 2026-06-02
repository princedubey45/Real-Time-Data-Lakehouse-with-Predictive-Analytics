# kafka_streaming/producer.py
"""
Kafka Event Producer — Enterprise Data Platform
════════════════════════════════════════════════
Fetches data from the FakeStore REST API and produces each record
as a typed JSON event to the corresponding Kafka topic.

  Orders    → topic: orders-raw
  Customers → topic: customers-raw
  Products  → topic: products-raw

The producer acts as the *entry point* of the streaming pipeline,
replacing the old direct-to-MinIO fetch pattern.

Usage (standalone)::

    python kafka_streaming/producer.py --entity all
    python kafka_streaming/producer.py --entity orders

Usage (in code)::

    from kafka_streaming.producer import KafkaEventProducer
    producer = KafkaEventProducer()
    result = producer.produce_orders()
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests

sys.path.insert(0, str(Path(__file__).parent.parent))
from config import API, KAFKA, LOG_LEVEL, LOG_FORMAT
from kafka_streaming.topics import TOPICS
from kafka_streaming.schema import (
    CustomerEvent, OrderEvent, OrderProductItem, PipelineEvent, ProductEvent,
)

logging.basicConfig(level=LOG_LEVEL, format=LOG_FORMAT)
log = logging.getLogger("kafka.producer")


# ── Delivery callback ──────────────────────────────────────────────────────────

def _delivery_report(err, msg) -> None:
    if err:
        log.error("Delivery failed | topic=%s  err=%s", msg.topic(), err)
    else:
        log.debug(
            "Delivered | topic=%s  partition=%d  offset=%d",
            msg.topic(), msg.partition(), msg.offset(),
        )


# ── Producer class ─────────────────────────────────────────────────────────────

class KafkaEventProducer:
    """
    Wraps confluent_kafka.Producer with entity-specific produce methods.

    Falls back to kafka-python if confluent_kafka is unavailable, or to
    dry-run mode (logging only) if neither is installed.
    """

    def __init__(self) -> None:
        self._producer = self._build_producer()
        self._session  = requests.Session()
        self._session.headers["User-Agent"] = "enterprise-data-platform/1.0"
        log.info(
            "KafkaEventProducer ready  bootstrap=%s",
            KAFKA.bootstrap_servers,
        )

    # ── Internal helpers ───────────────────────────────────────────────────────

    @staticmethod
    def _build_producer():
        """Return a confluent_kafka.Producer (preferred) or kafka-python fallback."""
        try:
            from confluent_kafka import Producer
            return Producer(KAFKA.producer_config)
        except ImportError:
            pass

        try:
            from kafka import KafkaProducer as KP
            log.warning("confluent_kafka not found — using kafka-python fallback")
            return _KafkaPythonProducerAdapter(
                bootstrap_servers=KAFKA.bootstrap_servers.split(","),
                value_serializer=lambda v: v if isinstance(v, bytes) else v.encode(),
                acks=KAFKA.acks,
                retries=KAFKA.retries,
            )
        except ImportError:
            log.warning("No Kafka client installed — running in DRY-RUN mode")
            return _DryRunProducer()

    def _fetch(self, endpoint: str) -> list[dict]:
        """GET from FakeStore API with retry logic."""
        url = f"{API.base_url}{endpoint}"
        for attempt in range(1, API.max_retries + 1):
            try:
                resp = self._session.get(url, timeout=API.timeout_sec)
                resp.raise_for_status()
                data = resp.json()
                return data if isinstance(data, list) else [data]
            except requests.RequestException as exc:
                log.warning("Attempt %d/%d failed: %s", attempt, API.max_retries, exc)
                if attempt < API.max_retries:
                    time.sleep(2 ** attempt)
        raise RuntimeError(f"All retries exhausted for {url}")

    def _produce(self, topic: str, key: str, value: dict) -> None:
        """Serialise and produce a single message."""
        payload = json.dumps(value, default=str).encode("utf-8")
        self._producer.produce(
            topic=topic,
            key=key.encode("utf-8"),
            value=payload,
            on_delivery=_delivery_report,
        )

    def _flush(self, timeout: float = 30.0) -> None:
        self._producer.flush(timeout=timeout)

    def _emit_pipeline_event(self, stage: str, event_type: str, details: dict) -> None:
        event = PipelineEvent(
            event_type=event_type,
            pipeline_id=f"producer-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S')}",
            stage=stage,
            details=details,
        )
        self._produce(TOPICS.PIPELINE_EVENTS.name, key=stage, value=event.model_dump())

    # ── Public produce methods ─────────────────────────────────────────────────

    def produce_orders(self, run_ts: datetime | None = None) -> dict:
        """
        Fetch orders from API → produce each to orders-raw topic.
        Returns summary dict with topic, count, run_ts.
        """
        run_ts = run_ts or datetime.now(timezone.utc)
        log.info("Producing orders → %s", TOPICS.ORDERS.name)
        raw = self._fetch(API.endpoints["orders"])

        produced = 0
        for cart in raw:
            try:
                products = [
                    OrderProductItem(
                        product_id=p.get("productId", p.get("product_id", 0)),
                        quantity=p.get("quantity", 1),
                    )
                    for p in cart.get("products", [])
                ]
                event = OrderEvent(
                    order_id=cart["id"],
                    user_id=cart["userId"],
                    order_date=cart.get("date", run_ts.isoformat()),
                    products=products,
                )
                self._produce(
                    TOPICS.ORDERS.name,
                    key=str(event.order_id),
                    value=event.model_dump(),
                )
                produced += 1
            except Exception as exc:
                log.warning("Skipping malformed cart %s: %s", cart.get("id"), exc)

        self._flush()
        self._emit_pipeline_event(
            "produce_orders", "pipeline_complete",
            {"topic": TOPICS.ORDERS.name, "produced": produced},
        )
        log.info("Orders produced: %d → %s", produced, TOPICS.ORDERS.name)
        return {"topic": TOPICS.ORDERS.name, "produced": produced, "run_ts": run_ts.isoformat()}

    def produce_customers(self, run_ts: datetime | None = None) -> dict:
        """
        Fetch customers from API → produce each to customers-raw topic.
        PII (email, phone) is SHA-256 hashed before producing.
        """
        run_ts = run_ts or datetime.now(timezone.utc)
        log.info("Producing customers → %s", TOPICS.CUSTOMERS.name)
        raw = self._fetch(API.endpoints["customers"])

        produced = 0
        for user in raw:
            try:
                name  = user.get("name", {})
                addr  = user.get("address", {})
                geo   = addr.get("geolocation", {})
                event = CustomerEvent(
                    customer_id=user["id"],
                    username=user.get("username", ""),
                    full_name=f"{name.get('firstname','')} {name.get('lastname','')}".strip(),
                    email_hash=CustomerEvent.hash_pii(user.get("email", "")),
                    phone_hash=CustomerEvent.hash_pii(user.get("phone", "")),
                    city=addr.get("city", ""),
                    zipcode=str(addr.get("zipcode", "")),
                    geo_lat=float(geo.get("lat", 0.0) or 0.0),
                    geo_long=float(geo.get("long", 0.0) or 0.0),
                )
                self._produce(
                    TOPICS.CUSTOMERS.name,
                    key=str(event.customer_id),
                    value=event.model_dump(),
                )
                produced += 1
            except Exception as exc:
                log.warning("Skipping malformed user %s: %s", user.get("id"), exc)

        self._flush()
        log.info("Customers produced: %d → %s", produced, TOPICS.CUSTOMERS.name)
        return {"topic": TOPICS.CUSTOMERS.name, "produced": produced, "run_ts": run_ts.isoformat()}

    def produce_products(self, run_ts: datetime | None = None) -> dict:
        """
        Fetch products from API → produce each to products-raw topic.
        Enriches with price_tier classification.
        """
        run_ts = run_ts or datetime.now(timezone.utc)
        log.info("Producing products → %s", TOPICS.PRODUCTS.name)
        raw = self._fetch(API.endpoints["products"])

        def _price_tier(price: float) -> str:
            if price < 20:   return "budget"
            if price < 75:   return "standard"
            if price < 200:  return "premium"
            return "luxury"

        produced = 0
        for prod in raw:
            try:
                rating = prod.get("rating", {})
                price  = float(prod.get("price", 0.0))
                event  = ProductEvent(
                    product_id=prod["id"],
                    title=prod.get("title", ""),
                    price=price,
                    category=prod.get("category", ""),
                    description=prod.get("description", ""),
                    rating_score=float(rating.get("rate", 0.0)),
                    rating_count=int(rating.get("count", 0)),
                    price_tier=_price_tier(price),
                )
                self._produce(
                    TOPICS.PRODUCTS.name,
                    key=str(event.product_id),
                    value=event.model_dump(),
                )
                produced += 1
            except Exception as exc:
                log.warning("Skipping malformed product %s: %s", prod.get("id"), exc)

        self._flush()
        log.info("Products produced: %d → %s", produced, TOPICS.PRODUCTS.name)
        return {"topic": TOPICS.PRODUCTS.name, "produced": produced, "run_ts": run_ts.isoformat()}

    def produce_all(self, run_ts: datetime | None = None) -> dict[str, dict]:
        """Produce all three entity topics in sequence. Returns summary per entity."""
        run_ts = run_ts or datetime.now(timezone.utc)
        return {
            "orders":    self.produce_orders(run_ts),
            "customers": self.produce_customers(run_ts),
            "products":  self.produce_products(run_ts),
        }


# ── Adapter for kafka-python fallback ─────────────────────────────────────────

class _KafkaPythonProducerAdapter:
    """Thin adapter so kafka-python feels like confluent_kafka.Producer."""
    def __init__(self, **kwargs):
        from kafka import KafkaProducer
        self._p = KafkaProducer(**kwargs)

    def produce(self, topic, key, value, on_delivery=None):
        future = self._p.send(topic, key=key, value=value)
        try:
            meta = future.get(timeout=10)
            if on_delivery:
                on_delivery(None, _FakeMeta(topic, meta.partition, meta.offset))
        except Exception as exc:
            if on_delivery:
                on_delivery(exc, _FakeMeta(topic, -1, -1))

    def flush(self, timeout=30):
        self._p.flush(timeout=timeout)


class _FakeMeta:
    def __init__(self, topic, partition, offset):
        self._topic, self._partition, self._offset = topic, partition, offset
    def topic(self): return self._topic
    def partition(self): return self._partition
    def offset(self): return self._offset


# ── Dry-run producer (no Kafka installed) ─────────────────────────────────────

class _DryRunProducer:
    def produce(self, topic, key, value, on_delivery=None):
        log.info("[DRY-RUN] Would produce to %s | key=%s | %d bytes",
                 topic, key, len(value))

    def flush(self, timeout=30):
        pass


# ── CLI entry-point ────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Kafka Event Producer")
    parser.add_argument(
        "--entity", choices=["orders", "customers", "products", "all"],
        default="all", help="Which entity to produce (default: all)",
    )
    args = parser.parse_args()

    producer = KafkaEventProducer()
    ts = datetime.now(timezone.utc)

    if args.entity == "all":
        results = producer.produce_all(ts)
    elif args.entity == "orders":
        results = {"orders": producer.produce_orders(ts)}
    elif args.entity == "customers":
        results = {"customers": producer.produce_customers(ts)}
    else:
        results = {"products": producer.produce_products(ts)}

    for entity, summary in results.items():
        print(f"  {entity:12s} → {summary['produced']:4d} messages → {summary['topic']}")
