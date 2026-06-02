# kafka_streaming/topics.py
"""
Topic registry for the Enterprise Data Platform.

All Kafka topic names, partition counts, and retention policies live here.
Import TOPICS wherever you need a topic name to avoid hardcoded strings.
"""

from __future__ import annotations
from dataclasses import dataclass


@dataclass(frozen=True)
class TopicSpec:
    """Metadata about a single Kafka topic."""
    name:            str
    partitions:      int
    replication:     int
    retention_hours: int
    description:     str


class _Topics:
    """
    Registry of all platform Kafka topics.

    Usage::

        from kafka_streaming.topics import TOPICS
        print(TOPICS.ORDERS.name)       # "orders-raw"
        print(TOPICS.all_names())       # ["orders-raw", "customers-raw", ...]
    """

    ORDERS = TopicSpec(
        name="orders-raw",
        partitions=3,
        replication=1,
        retention_hours=168,
        description="Raw order/cart events from FakeStore API",
    )

    CUSTOMERS = TopicSpec(
        name="customers-raw",
        partitions=3,
        replication=1,
        retention_hours=168,
        description="Raw customer/user events from FakeStore API",
    )

    PRODUCTS = TopicSpec(
        name="products-raw",
        partitions=3,
        replication=1,
        retention_hours=168,
        description="Raw product catalogue events from FakeStore API",
    )

    PIPELINE_EVENTS = TopicSpec(
        name="pipeline-events",
        partitions=1,
        replication=1,
        retention_hours=720,   # 30 days of pipeline audit events
        description="Internal pipeline lifecycle events (start/complete/fail)",
    )

    @classmethod
    def all_specs(cls) -> list[TopicSpec]:
        return [cls.ORDERS, cls.CUSTOMERS, cls.PRODUCTS, cls.PIPELINE_EVENTS]

    @classmethod
    def all_names(cls) -> list[str]:
        return [t.name for t in cls.all_specs()]

    @classmethod
    def entity_topics(cls) -> dict[str, TopicSpec]:
        """Return {entity_name: TopicSpec} for the three entity topics."""
        return {
            "orders":    cls.ORDERS,
            "customers": cls.CUSTOMERS,
            "products":  cls.PRODUCTS,
        }


TOPICS = _Topics()
