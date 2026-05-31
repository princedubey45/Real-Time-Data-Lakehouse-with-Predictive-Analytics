# airflow/dags/quality_check.py
"""
Airflow DAG — Quality Check & Anomaly Detection
═════════════════════════════════════════════════
Runs independently of the ETL pipeline, 30 minutes after it completes.
Can also be triggered manually at any time.

Task chain:
  run_quality_checks ── detect_anomalies ── run_forecasts ── notify_summary

Schedule: daily at 06:30 UTC (30 min after etl_pipeline)

Tasks
─────
run_quality_checks  → etl/validate.py — full Gold table quality suite
detect_anomalies    → anomaly_detection/anomaly.py — statistical outlier detection
run_forecasts       → forecasting/forecast.py — 30-day revenue forecast
notify_summary      → Logs summary to load_audit; in production this
                      would send a Slack / email notification
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.operators.python import PythonOperator

log = logging.getLogger(__name__)

default_args = {
    "owner":            "data-engineering",
    "retries":          1,
    "retry_delay":      timedelta(minutes=3),
    "email_on_failure": False,
}


# ── Task callables ─────────────────────────────────────────────────────────────

def _quality_checks(**ctx):
    import sys; sys.path.insert(0, "/opt/airflow")
    from etl.validate import run_validate
    run_ts = ctx["logical_date"].replace(tzinfo=timezone.utc)
    report = run_validate(run_ts=run_ts)
    ctx["ti"].xcom_push(key="quality_report", value=report["summary"])
    log.info("Quality summary: %s", report["summary"])
    return report["summary"]


def _detect_anomalies(**ctx):
    import sys; sys.path.insert(0, "/opt/airflow")
    from anomaly_detection.anomaly import run_anomaly_detection
    run_ts  = ctx["logical_date"].replace(tzinfo=timezone.utc)
    results = run_anomaly_detection(run_ts=run_ts)
    ctx["ti"].xcom_push(key="anomaly_count", value=results.get("total_anomalies", 0))
    log.info("Anomalies detected: %s", results.get("total_anomalies", 0))
    return results


def _run_forecasts(**ctx):
    import sys; sys.path.insert(0, "/opt/airflow")
    from forecasting.forecast import run_forecast
    run_ts  = ctx["logical_date"].replace(tzinfo=timezone.utc)
    results = run_forecast(run_ts=run_ts)
    ctx["ti"].xcom_push(key="forecast_horizon", value=results.get("horizon_days"))
    log.info("Forecast complete: %s", results)
    return results


def _notify_summary(**ctx):
    """
    Aggregate results from upstream tasks and write a summary audit row.
    In production: extend this to send Slack / email / PagerDuty alerts.
    """
    import sys; sys.path.insert(0, "/opt/airflow")
    import psycopg2
    from config import WAREHOUSE

    ti = ctx["ti"]
    quality  = ti.xcom_pull(task_ids="run_quality_checks",  key="quality_report") or {}
    anomalies = ti.xcom_pull(task_ids="detect_anomalies",   key="anomaly_count")  or 0
    forecast  = ti.xcom_pull(task_ids="run_forecasts",      key="forecast_horizon") or 0
    run_ts    = ctx["logical_date"].replace(tzinfo=timezone.utc)

    summary = (
        f"Quality: passed={quality.get('passed',0)}, "
        f"warned={quality.get('warned',0)}, failed={quality.get('failed',0)} | "
        f"Anomalies: {anomalies} | "
        f"Forecast horizon: {forecast} days"
    )
    log.info("Daily summary: %s", summary)

    # Write to audit log
    try:
        conn = psycopg2.connect(**WAREHOUSE.psycopg2_kwargs)
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO load_audit (table_name, rows_loaded, run_ts, status) "
                "VALUES (%s, %s, %s, %s)",
                ("quality_check_dag", anomalies, run_ts, "success"),
            )
        conn.commit()
        conn.close()
    except Exception as exc:
        log.warning("Could not write audit row: %s", exc)

    return summary


# ── DAG definition ─────────────────────────────────────────────────────────────
with DAG(
    dag_id="quality_check",
    description="Daily quality checks, anomaly detection, and forecasting",
    schedule_interval="30 6 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["quality", "anomaly", "forecast", "monitoring"],
    doc_md=__doc__,
) as dag:

    quality = PythonOperator(
        task_id="run_quality_checks",
        python_callable=_quality_checks,
    )

    anomalies = PythonOperator(
        task_id="detect_anomalies",
        python_callable=_detect_anomalies,
    )

    forecasts = PythonOperator(
        task_id="run_forecasts",
        python_callable=_run_forecasts,
    )

    notify = PythonOperator(
        task_id="notify_summary",
        python_callable=_notify_summary,
    )

    quality >> anomalies >> forecasts >> notify
