# kafka_streaming/consumer.py
"""
Kafka → MinIO Bronze Consumer — Enterprise Data Platform
Reads event batches from Kafka topics and writes Bronze JSON to MinIO.

Flow:
  Kafka (orders-raw, customers-raw, products-raw)
      ↓  batch poll (timeout window)
  KafkaMinIOConsumer
      ↓  group by entity
  MinIO  raw/{entity}/date=YYYY-MM-DD/{entity}_{ts}_kafka.json
      ↓  (identical Bronze format to fetch_*.py output)
  etl/clean_data.py → Silver Parquet
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

import boto3
from botocore.client import Config

sys.path.insert(0, str(Path(__file__).parent.parent))
from config import KAFKA, LAKE, LOG_LEVEL, LOG_FORMAT
from kafka_streaming.topics import TOPICS

logging.basicConfig(level=LOG_LEVEL, format=LOG_FORMAT)
log = logging.getLogger("kafka.consumer")

_TOPIC_TO_ENTITY: dict[str, str] = {
    TOPICS.ORDERS.name:    "orders",
    TOPICS.CUSTOMERS.name: "customers",
    TOPICS.PRODUCTS.name:  "products",
}


# ── MinIO helper ───────────────────────────────────────────────────────────────

def _s3_client():
    return boto3.client(
        "s3",
        endpoint_url=LAKE.endpoint,
        aws_access_key_id=LAKE.access_key,
        aws_secret_access_key=LAKE.secret_key,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )


def _ensure_bucket(client, bucket: str) -> None:
    try:
        client.head_bucket(Bucket=bucket)
    except Exception:
        client.create_bucket(Bucket=bucket)
        log.info("Created bucket: %s", bucket)


# ── Consumer ───────────────────────────────────────────────────────────────────

class KafkaMinIOConsumer:
    """
    Batch consumer: poll Kafka → write Bronze JSON to MinIO.

    Designed to be called from an Airflow task (short-lived, not a daemon).
    Uses confluent_kafka if available, falls back to kafka-python, then dry-run.
    """

    def __init__(
        self,
        topics: list[str] | None = None,
        timeout_ms: int | None = None,
        max_records: int = 10_000,
    ) -> None:
        self._topics      = topics or list(_TOPIC_TO_ENTITY.keys())
        self._timeout_ms  = timeout_ms or KAFKA.consumer_timeout_ms
        self._max_records = max_records
        self._consumer    = self._build_consumer()
        self._s3          = _s3_client()
        _ensure_bucket(self._s3, LAKE.bucket)
        log.info("KafkaMinIOConsumer ready  topics=%s  timeout=%dms", self._topics, self._timeout_ms)

    @staticmethod
    def _build_consumer():
        try:
            from confluent_kafka import Consumer
            return Consumer(KAFKA.consumer_config)
        except ImportError:
            pass
        try:
            from kafka import KafkaConsumer
            log.warning("confluent_kafka not found — using kafka-python")
            return _KafkaPythonAdapter(
                bootstrap_servers=KAFKA.bootstrap_servers.split(","),
                group_id=KAFKA.consumer_group,
                auto_offset_reset=KAFKA.auto_offset_reset,
                consumer_timeout_ms=KAFKA.consumer_timeout_ms,
            )
        except ImportError:
            log.warning("No Kafka client installed — DRY-RUN mode")
            return _DryRunConsumer()

    def _write_bronze(self, entity: str, records: list[dict], run_ts: datetime) -> str:
        date_str = run_ts.strftime("%Y-%m-%d")
        ts_str   = run_ts.strftime("%Y%m%dT%H%M%SZ")
        key      = f"{LAKE.raw_prefix}/{entity}/date={date_str}/{entity}_{ts_str}_kafka.json"
        payload  = {
            "_meta": {
                "entity": entity, "source": "kafka",
                "run_ts": run_ts.isoformat(), "record_count": len(records),
            },
            "data": records,
        }
        body = json.dumps(payload, default=str, indent=2).encode("utf-8")
        self._s3.put_object(
            Bucket=LAKE.bucket, Key=key, Body=body,
            ContentType="application/json",
            Metadata={"entity": entity, "records": str(len(records)), "source": "kafka"},
        )
        log.info("Bronze ← Kafka | entity=%s  records=%d  key=%s", entity, len(records), key)
        return key

    def _poll_confluent(self, deadline: float) -> dict[str, list[dict]]:
        from confluent_kafka import KafkaException
        self._consumer.subscribe(self._topics)
        buckets: dict[str, list[dict]] = defaultdict(list)
        total = 0
        while time.time() < deadline and total < self._max_records:
            msg = self._consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                raise KafkaException(msg.error())
            entity = _TOPIC_TO_ENTITY.get(msg.topic(), msg.topic())
            try:
                buckets[entity].append(json.loads(msg.value().decode()))
                total += 1
            except Exception as exc:
                log.warning("Bad message from %s: %s", msg.topic(), exc)
        self._consumer.close()
        return dict(buckets)

    def consume_all(self, run_ts: datetime | None = None) -> dict[str, dict]:
        """Poll all entity topics → write Bronze JSON → return {entity: {key, records}}."""
        run_ts   = run_ts or datetime.now(timezone.utc)
        deadline = time.time() + self._timeout_ms / 1000
        log.info("Kafka poll start  timeout=%dms  max=%d", self._timeout_ms, self._max_records)

        try:
            from confluent_kafka import Consumer
            if isinstance(self._consumer, Consumer):
                buckets = self._poll_confluent(deadline)
            else:
                buckets = self._consumer.poll_all(self._topics, _TOPIC_TO_ENTITY)
        except ImportError:
            buckets = self._consumer.poll_all(self._topics, _TOPIC_TO_ENTITY)

        results: dict[str, dict] = {}
        for entity, records in buckets.items():
            if records:
                key = self._write_bronze(entity, records, run_ts)
                results[entity] = {"key": key, "records": len(records)}

        log.info("Consume done  entities=%d  total=%d",
                 len(results), sum(r["records"] for r in results.values()))
        return results


# ── Adapters ───────────────────────────────────────────────────────────────────

class _KafkaPythonAdapter:
    def __init__(self, **kwargs):
        from kafka import KafkaConsumer
        self._kc = KafkaConsumer(
            value_deserializer=lambda v: json.loads(v.decode()),
            **kwargs,
        )

    def poll_all(self, topics, topic_map) -> dict[str, list[dict]]:
        self._kc.subscribe(topics)
        buckets: dict[str, list[dict]] = defaultdict(list)
        try:
            for msg in self._kc:
                buckets[topic_map.get(msg.topic, msg.topic)].append(msg.value)
        except Exception:
            pass
        return dict(buckets)

    def close(self): self._kc.close()


class _DryRunConsumer:
    def poll_all(self, topics, topic_map) -> dict[str, list[dict]]:
        log.info("[DRY-RUN] Would consume: %s", topics)
        return {}


# ── CLI ────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Kafka → MinIO Bronze Consumer")
    parser.add_argument("--topic",       default=None)
    parser.add_argument("--max-records", type=int, default=10_000)
    parser.add_argument("--timeout-ms",  type=int, default=None)
    args = parser.parse_args()

    consumer = KafkaMinIOConsumer(
        topics=[args.topic] if args.topic else None,
        timeout_ms=args.timeout_ms,
        max_records=args.max_records,
    )
    results = consumer.consume_all(run_ts=datetime.now(timezone.utc))
    if results:
        for entity, s in results.items():
            print(f"  {entity:12s} → {s['records']:5d} records → {s['key']}")
    else:
        print("  No messages consumed.")
