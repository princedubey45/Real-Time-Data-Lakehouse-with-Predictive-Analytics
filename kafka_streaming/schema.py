# kafka_streaming/schema.py
"""
Pydantic event schemas for each Kafka topic entity.

Every message produced to Kafka is serialised as JSON conforming to
one of these schemas. This gives us:
  - Type safety at produce time
  - Self-documenting contracts for consumers
  - Easy validation when writing to the Data Lake

Usage::

    from kafka_streaming.schema import OrderEvent, CustomerEvent, ProductEvent
    event = OrderEvent(order_id=1, user_id=2, products=[...])
    payload = event.model_dump_json()
"""

from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

try:
    from pydantic import BaseModel, Field, field_validator
except ImportError:
    # Fallback: plain dataclasses if pydantic not installed
    from dataclasses import dataclass as BaseModel  # type: ignore
    Field = lambda *a, **kw: None  # type: ignore


# ── Helpers ────────────────────────────────────────────────────────────────────

def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _event_id() -> str:
    return str(uuid4())


# ── Order / Cart Event ─────────────────────────────────────────────────────────

class OrderProductItem(BaseModel):
    product_id: int
    quantity:   int


class OrderEvent(BaseModel):
    """
    Represents a single cart/order from the FakeStore /carts API.

    Kafka topic  : orders-raw
    Partition key: str(order_id)
    """
    event_id:    str                  = Field(default_factory=_event_id)
    event_type:  str                  = "order_created"
    event_ts:    str                  = Field(default_factory=_now_iso)
    schema_version: str               = "1.0"

    order_id:    int
    user_id:     int
    order_date:  str
    products:    list[OrderProductItem]

    # Computed at produce time
    product_count: int = 0
    total_quantity: int = 0

    def model_post_init(self, __context: Any) -> None:
        object.__setattr__(self, "product_count", len(self.products))
        object.__setattr__(
            self, "total_quantity",
            sum(p.quantity for p in self.products)
        )


# ── Customer / User Event ──────────────────────────────────────────────────────

class CustomerEvent(BaseModel):
    """
    Represents a user from the FakeStore /users API.
    PII (email, phone) is SHA-256 hashed before producing.

    Kafka topic  : customers-raw
    Partition key: str(customer_id)
    """
    event_id:    str = Field(default_factory=_event_id)
    event_type:  str = "customer_upsert"
    event_ts:    str = Field(default_factory=_now_iso)
    schema_version: str = "1.0"

    customer_id: int
    username:    str
    full_name:   str
    email_hash:  str        # SHA-256 of email — never emit raw PII to Kafka
    phone_hash:  str        # SHA-256 of phone
    city:        str  = ""
    zipcode:     str  = ""
    geo_lat:     float = 0.0
    geo_long:    float = 0.0

    @staticmethod
    def hash_pii(value: str) -> str:
        return hashlib.sha256(value.encode()).hexdigest()


# ── Product Event ──────────────────────────────────────────────────────────────

class ProductEvent(BaseModel):
    """
    Represents a product from the FakeStore /products API.

    Kafka topic  : products-raw
    Partition key: str(product_id)
    """
    event_id:    str   = Field(default_factory=_event_id)
    event_type:  str   = "product_upsert"
    event_ts:    str   = Field(default_factory=_now_iso)
    schema_version: str = "1.0"

    product_id:       int
    title:            str
    price:            float
    category:         str
    description:      str  = ""
    description_len:  int  = 0
    rating_score:     float = 0.0
    rating_count:     int   = 0
    price_tier:       str   = "standard"   # budget | standard | premium | luxury

    def model_post_init(self, __context: Any) -> None:
        object.__setattr__(self, "description_len", len(self.description))


# ── Pipeline Lifecycle Event ───────────────────────────────────────────────────

class PipelineEvent(BaseModel):
    """
    Internal lifecycle event — produced on pipeline start/complete/fail.

    Kafka topic: pipeline-events
    """
    event_id:    str = Field(default_factory=_event_id)
    event_type:  str   # "pipeline_start" | "pipeline_complete" | "pipeline_fail"
    event_ts:    str = Field(default_factory=_now_iso)
    pipeline_id: str
    stage:       str
    details:     dict = Field(default_factory=dict)
