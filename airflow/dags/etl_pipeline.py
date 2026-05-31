# airflow/dags/etl_pipeline.py
"""
Airflow DAG — ETL Pipeline
═══════════════════════════
Full daily pipeline:

  ingest_orders ──┐
  ingest_customers─┼── clean_data ── transform ── validate ── load_warehouse
  ingest_products ─┘

Schedule: daily at 06:00 UTC
Retries:  2 per task, 5-minute backoff
Alerts:   on_failure_callback logs to anomaly_log

Task breakdown
──────────────
1. ingest_*       — Parallel: fetch each API entity → MinIO raw/
2. clean_data     — Silver: read raw JSON, clean, write Parquet to processed/
3. transform      — Gold: join + aggregate, write Parquet to processed/gold/
4. validate       — Quality checks on Gold tables; fails DAG on critical errors
5. load_warehouse — Upsert Gold Parquet into PostgreSQL
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

log = logging.getLogger(__name__)

# ── Default task arguments ─────────────────────────────────────────────────────
default_args = {
    "owner":             "data-engineering",
    "retries":           2,
    "retry_delay":       timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay":   timedelta(minutes=30),
    "email_on_failure":  False,
    "email_on_retry":    False,
    "depends_on_past":   False,
}


# ── Failure callback ───────────────────────────────────────────────────────────
def _on_failure(context: dict) -> None:
    """Log pipeline failures to the anomaly_log table for observability."""
    try:
        import sys
        sys.path.insert(0, "/opt/airflow")
        import psycopg2
        from config import WAREHOUSE

        conn = psycopg2.connect(**WAREHOUSE.psycopg2_kwargs)
        task_id   = context["task_instance"].task_id
        dag_id    = context["dag"].dag_id
        error     = str(context.get("exception", "unknown"))[:500]

        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO anomaly_log
                    (entity, anomaly_type, affected_key, description, severity)
                VALUES (%s, %s, %s, %s, 'high')
            """, ("airflow", "task_failure", f"{dag_id}.{task_id}", error))
        conn.commit()
        conn.close()
    except Exception as exc:
        log.warning("Could not write failure to anomaly_log: %s", exc)


# ── Task callables ─────────────────────────────────────────────────────────────

def _ingest_orders(**ctx):
    import sys; sys.path.insert(0, "/opt/airflow")
    from api_ingestion.fetch_orders import fetch_orders
    run_ts = ctx["logical_date"].replace(tzinfo=timezone.utc)
    result = fetch_orders(run_ts=run_ts)
    ctx["ti"].xcom_push(key="orders_key",   value=result["key"])
    ctx["ti"].xcom_push(key="orders_count", value=result["record_count"])
    log.info("Ingested %d orders → %s", result["record_count"], result["key"])


def _ingest_customers(**ctx):
    import sys; sys.path.insert(0, "/opt/airflow")
    from api_ingestion.fetch_customers import fetch_customers
    run_ts = ctx["logical_date"].replace(tzinfo=timezone.utc)
    result = fetch_customers(run_ts=run_ts)
    ctx["ti"].xcom_push(key="customers_key",   value=result["key"])
    ctx["ti"].xcom_push(key="customers_count", value=result["record_count"])
    log.info("Ingested %d customers → %s", result["record_count"], result["key"])


def _ingest_products(**ctx):
    import sys; sys.path.insert(0, "/opt/airflow")
    from api_ingestion.fetch_products import fetch_products
    run_ts = ctx["logical_date"].replace(tzinfo=timezone.utc)
    result = fetch_products(run_ts=run_ts)
    ctx["ti"].xcom_push(key="products_key",   value=result["key"])
    ctx["ti"].xcom_push(key="products_count", value=result["record_count"])
    log.info("Ingested %d products → %s", result["record_count"], result["key"])


def _clean(**ctx):
    import sys; sys.path.insert(0, "/opt/airflow")
    from etl.clean_data import run_clean
    run_ts = ctx["logical_date"].replace(tzinfo=timezone.utc)
    keys   = run_clean(run_ts=run_ts)
    ctx["ti"].xcom_push(key="clean_keys", value=keys)
    log.info("Clean complete: %s", list(keys.keys()))


def _transform(**ctx):
    import sys; sys.path.insert(0, "/opt/airflow")
    from etl.transform import run_transform
    run_ts = ctx["logical_date"].replace(tzinfo=timezone.utc)
    keys   = run_transform(run_ts=run_ts)
    ctx["ti"].xcom_push(key="gold_keys", value=keys)
    log.info("Transform complete: %s", list(keys.keys()))


def _validate(**ctx):
    import sys; sys.path.insert(0, "/opt/airflow")
    from etl.validate import run_validate
    run_ts = ctx["logical_date"].replace(tzinfo=timezone.utc)
    report = run_validate(run_ts=run_ts)
    ctx["ti"].xcom_push(key="quality_summary", value=report["summary"])
    log.info("Validation: %s", report["summary"])


def _load(**ctx):
    import sys; sys.path.insert(0, "/opt/airflow")
    from etl.load_warehouse import run_load
    run_ts  = ctx["logical_date"].replace(tzinfo=timezone.utc)
    results = run_load(run_ts=run_ts)
    ctx["ti"].xcom_push(key="load_results", value=results)
    log.info("Load complete: %s", results)


# ── DAG definition ─────────────────────────────────────────────────────────────
with DAG(
    dag_id="etl_pipeline",
    description="Daily ETL: API → Data Lake (Bronze/Silver/Gold) → PostgreSQL Warehouse",
    schedule_interval="0 6 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    on_failure_callback=_on_failure,
    tags=["etl", "data-lake", "warehouse", "production"],
    doc_md=__doc__,
) as dag:

    # ── Ingestion (parallel) ───────────────────────────────────────────────────
    ingest_orders = PythonOperator(
        task_id="ingest_orders",
        python_callable=_ingest_orders,
        on_failure_callback=_on_failure,
    )

    ingest_customers = PythonOperator(
        task_id="ingest_customers",
        python_callable=_ingest_customers,
        on_failure_callback=_on_failure,
    )

    ingest_products = PythonOperator(
        task_id="ingest_products",
        python_callable=_ingest_products,
        on_failure_callback=_on_failure,
    )

    # ── Silver ─────────────────────────────────────────────────────────────────
    clean = PythonOperator(
        task_id="clean_data",
        python_callable=_clean,
        on_failure_callback=_on_failure,
    )

    # ── Gold ───────────────────────────────────────────────────────────────────
    transform = PythonOperator(
        task_id="transform",
        python_callable=_transform,
        on_failure_callback=_on_failure,
    )

    # ── Validate ───────────────────────────────────────────────────────────────
    validate = PythonOperator(
        task_id="validate",
        python_callable=_validate,
        on_failure_callback=_on_failure,
    )

    # ── Load ───────────────────────────────────────────────────────────────────
    load = PythonOperator(
        task_id="load_warehouse",
        python_callable=_load,
        trigger_rule=TriggerRule.ALL_SUCCESS,
        on_failure_callback=_on_failure,
    )

    # ── Dependencies ───────────────────────────────────────────────────────────
    [ingest_orders, ingest_customers, ingest_products] >> clean >> transform >> validate >> load
