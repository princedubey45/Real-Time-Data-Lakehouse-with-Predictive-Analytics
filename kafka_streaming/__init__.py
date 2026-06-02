# kafka_streaming/__init__.py
"""
kafka_streaming — Real-time event streaming layer.

Wraps Kafka producer/consumer logic so Airflow tasks and standalone scripts
can produce API data to Kafka topics and consume them back into MinIO Bronze.

Public API
──────────
    from kafka_streaming.producer import KafkaEventProducer
    from kafka_streaming.consumer import KafkaMinIOConsumer
    from kafka_streaming.topics   import TOPICS
"""

from kafka_streaming.topics import TOPICS  # noqa: F401

__all__ = ["TOPICS"]
