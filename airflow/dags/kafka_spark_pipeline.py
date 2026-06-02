# airflow/dags/kafka_spark_pipeline.py
"""
Airflow DAG — Kafka + PySpark Streaming Pipeline
══════════════════════════════════════════════════
Real-time ingestion pipeline using Kafka and PySpark.

Architecture
────────────
  produce_orders ──┐
  produce_customers─┼── consume_from_kafka ── spark_transform ── load_warehouse
  produce_products ─┘

Task breakdown
──────────────
1. produce_*        — Parallel: fetch each API entity → produce JSON events to Kafka topics
2. consume_kafka    — Batch-consume Kafka topics → write Bronze JSON to MinIO (raw/)
3. spark_transform  — PySpark Gold layer: Silver Parquet → distributed transforms → Gold Parquet
4. load_warehouse   — Upsert Gold Parquet into PostgreSQL (reuses existing load module)

Schedule: every 15 minutes (near real-time simulation)
Retries:  2 per task, 3-minute backoff
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

log = logging.getLogger(__name__)

# ── Default args ───────────────────────────────────────────────────────────────

default_args = {
    "owner":                    "data-engineering",
    "retries":                  2,
    "retry_delay":              timedelta(minutes=3),
    "retry_exponential_backoff": True,
    "max_retry_delay":          timedelta(minutes=15),
    "email_on_failure":         False,
    "email_on_retry":           False,
    "depends_on_past":          False,
}


# ── Failure callback ───────────────────────────────────────────────────────────

def _on_failure(context: dict) -> None:
    """Log pipeline failures to anomaly_log for observability."""
    try:
        import sys
        sys.path.insert(0, "/opt/airflow")
        import psycopg2
        from config import WAREHOUSE

        conn    = psycopg2.connect(**WAREHOUSE.psycopg2_kwargs)
        task_id = context["task_instance"].task_id
        dag_id  = context["dag"].dag_id
        error   = str(context.get("exception", "unknown"))[:500]

        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO anomaly_log
                    (entity, anomaly_type, affected_key, description, severity)
                VALUES (%s, %s, %s, %s, 'high')
                """,
                ("airflow", "task_failure", f"{dag_id}.{task_id}", error),
            )
        conn.commit()
        conn.close()
    except Exception as exc:
        log.warning("Could not write failure to anomaly_log: %s", exc)


# ── Task callables ─────────────────────────────────────────────────────────────

def _produce_orders(**ctx) -> None:
    import sys; sys.path.insert(0, "/opt/airflow")
    from kafka_streaming.producer import KafkaEventProducer

    run_ts   = ctx["logical_date"].replace(tzinfo=timezone.utc)
    producer = KafkaEventProducer()
    result   = producer.produce_orders(run_ts=run_ts)
    ctx["ti"].xcom_push(key="orders_produced", value=result["produced"])
    log.info("Produced %d order events → %s", result["produced"], result["topic"])


def _produce_customers(**ctx) -> None:
    import sys; sys.path.insert(0, "/opt/airflow")
    from kafka_streaming.producer import KafkaEventProducer

    run_ts   = ctx["logical_date"].replace(tzinfo=timezone.utc)
    producer = KafkaEventProducer()
    result   = producer.produce_customers(run_ts=run_ts)
    ctx["ti"].xcom_push(key="customers_produced", value=result["produced"])
    log.info("Produced %d customer events → %s", result["produced"], result["topic"])


def _produce_products(**ctx) -> None:
    import sys; sys.path.insert(0, "/opt/airflow")
    from kafka_streaming.producer import KafkaEventProducer

    run_ts   = ctx["logical_date"].replace(tzinfo=timezone.utc)
    producer = KafkaEventProducer()
    result   = producer.produce_products(run_ts=run_ts)
    ctx["ti"].xcom_push(key="products_produced", value=result["produced"])
    log.info("Produced %d product events → %s", result["produced"], result["topic"])


def _consume_kafka(**ctx) -> None:
    """
    Batch-consume Kafka topics → write Bronze JSON to MinIO.
    Waits for all three entity topics to have messages before proceeding.
    """
    import sys; sys.path.insert(0, "/opt/airflow")
    from kafka_streaming.consumer import KafkaMinIOConsumer

    run_ts   = ctx["logical_date"].replace(tzinfo=timezone.utc)
    consumer = KafkaMinIOConsumer()
    results  = consumer.consume_all(run_ts=run_ts)

    ctx["ti"].xcom_push(key="consumed_summary", value={
        entity: r["records"] for entity, r in results.items()
    })
    total = sum(r["records"] for r in results.values())
    log.info("Consumed %d total events from Kafka → MinIO Bronze", total)


def _spark_transform(**ctx) -> None:
    """
    PySpark Gold transform: Silver Parquet → distributed joins/aggs → Gold Parquet.
    Replaces the Pandas-based etl/transform.py for this pipeline.
    """
    import sys; sys.path.insert(0, "/opt/airflow")
    from spark.spark_transform import run_spark_transform
    from spark.spark_session   import stop_spark

    run_ts = ctx["logical_date"].replace(tzinfo=timezone.utc)

    try:
        keys = run_spark_transform(run_ts=run_ts)
        ctx["ti"].xcom_push(key="spark_gold_keys", value=keys)
        log.info(
            "PySpark transform complete. %d Gold tables written.", len(keys)
        )
    finally:
        stop_spark()    # always release Spark resources


def _load_warehouse(**ctx) -> None:
    """Reuse existing load module — reads Gold Parquet → upserts into PostgreSQL."""
    import sys; sys.path.insert(0, "/opt/airflow")
    from etl.load_warehouse import run_load

    run_ts  = ctx["logical_date"].replace(tzinfo=timezone.utc)
    results = run_load(run_ts=run_ts)
    ctx["ti"].xcom_push(key="load_results", value=results)
    log.info("Warehouse load complete: %s", results)


# ── DAG definition ─────────────────────────────────────────────────────────────

with DAG(
    dag_id="kafka_spark_etl",
    description=(
        "Real-time pipeline: API → Kafka (stream) → MinIO Bronze → "
        "PySpark Gold → PostgreSQL"
    ),
    schedule_interval="*/15 * * * *",    # every 15 minutes
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,                   # prevent overlapping Spark sessions
    default_args=default_args,
    on_failure_callback=_on_failure,
    tags=["kafka", "pyspark", "streaming", "realtime", "production"],
    doc_md=__doc__,
) as dag:

    # ── 1. Produce to Kafka (parallel) ─────────────────────────────────────────
    produce_orders = PythonOperator(
        task_id="produce_orders",
        python_callable=_produce_orders,
        on_failure_callback=_on_failure,
        doc_md="Fetch /carts from FakeStore API → produce to orders-raw Kafka topic",
    )

    produce_customers = PythonOperator(
        task_id="produce_customers",
        python_callable=_produce_customers,
        on_failure_callback=_on_failure,
        doc_md="Fetch /users from FakeStore API → produce to customers-raw Kafka topic",
    )

    produce_products = PythonOperator(
        task_id="produce_products",
        python_callable=_produce_products,
        on_failure_callback=_on_failure,
        doc_md="Fetch /products from FakeStore API → produce to products-raw Kafka topic",
    )

    # ── 2. Consume from Kafka → MinIO Bronze ───────────────────────────────────
    consume_kafka = PythonOperator(
        task_id="consume_from_kafka",
        python_callable=_consume_kafka,
        on_failure_callback=_on_failure,
        doc_md="Batch-consume all entity Kafka topics → write Bronze JSON to MinIO raw/",
    )

    # ── 3. PySpark Gold transform ──────────────────────────────────────────────
    spark_transform = PythonOperator(
        task_id="spark_transform",
        python_callable=_spark_transform,
        on_failure_callback=_on_failure,
        execution_timeout=timedelta(minutes=20),  # Spark init + job time
        doc_md=(
            "Distributed Gold transform: Silver Parquet → "
            "dim_* + fact_sales + agg_* tables → MinIO Gold"
        ),
    )

    # ── 4. Load warehouse ──────────────────────────────────────────────────────
    load_warehouse = PythonOperator(
        task_id="load_warehouse",
        python_callable=_load_warehouse,
        trigger_rule=TriggerRule.ALL_SUCCESS,
        on_failure_callback=_on_failure,
        doc_md="Upsert Gold Parquet tables into PostgreSQL data warehouse",
    )

    # ── Pipeline dependencies ──────────────────────────────────────────────────
    [produce_orders, produce_customers, produce_products] >> consume_kafka
    consume_kafka >> spark_transform >> load_warehouse
