# anomaly_detection/anomaly.py
"""
Anomaly Detection
══════════════════
Runs statistical anomaly detection against the PostgreSQL warehouse
and writes flagged records to the anomaly_log table.

Detection methods
─────────────────
1. Z-Score   — flags individual records whose metric deviates more
               than N standard deviations from the population mean.
               Used for: order revenue, product prices, order quantities.

2. IQR Fence — flags records outside [Q1 - k*IQR, Q3 + k*IQR].
               More robust to heavy-tailed distributions.
               Used for: daily order counts, customer spend.

3. Day-over-Day — flags daily revenue that changed by more than X%
                  compared to the same day of the previous week.
                  Used for: revenue time series.

Each anomaly is written to the anomaly_log table with severity,
z-score, expected range, and a human-readable description.
"""

import logging
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import psycopg2
import psycopg2.extras

sys.path.insert(0, str(Path(__file__).parent.parent))
from config import WAREHOUSE, ANOMALY, LOG_LEVEL, LOG_FORMAT

logging.basicConfig(level=LOG_LEVEL, format=LOG_FORMAT)
log = logging.getLogger("anomaly")


# ── Data class ─────────────────────────────────────────────────────────────────

@dataclass
class AnomalyRecord:
    entity:         str
    anomaly_type:   str
    affected_key:   str
    metric_name:    str
    metric_value:   float
    expected_range: str
    z_score:        float | None
    severity:       str          # low | medium | high | critical
    description:    str


# ── DB helpers ─────────────────────────────────────────────────────────────────

def _pg():
    return psycopg2.connect(**WAREHOUSE.psycopg2_kwargs)


def _load_df(conn, sql: str, params: tuple = ()) -> pd.DataFrame:
    with conn.cursor() as cur:
        cur.execute(sql, params)
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()
    return pd.DataFrame(rows, columns=cols)


def _write_anomalies(conn, records: list[AnomalyRecord]) -> int:
    if not records:
        return 0
    sql = """
        INSERT INTO anomaly_log
            (entity, anomaly_type, affected_key, metric_name,
             metric_value, expected_range, z_score, severity, description)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """
    rows = [
        (r.entity, r.anomaly_type, r.affected_key, r.metric_name,
         r.metric_value, r.expected_range, r.z_score, r.severity, r.description)
        for r in records
    ]
    with conn.cursor() as cur:
        psycopg2.extras.execute_batch(cur, sql, rows, page_size=200)
    conn.commit()
    log.info("Wrote %d anomaly records to anomaly_log", len(records))
    return len(records)


# ── Statistical helpers ────────────────────────────────────────────────────────

def _zscore(series: pd.Series) -> pd.Series:
    """Return z-scores; NaN where std == 0."""
    std = series.std()
    if std == 0 or pd.isna(std):
        return pd.Series([0.0] * len(series), index=series.index)
    return (series - series.mean()) / std


def _iqr_bounds(series: pd.Series, k: float) -> tuple[float, float]:
    q1, q3 = series.quantile(0.25), series.quantile(0.75)
    iqr    = q3 - q1
    return q1 - k * iqr, q3 + k * iqr


def _severity(z: float) -> str:
    az = abs(z)
    if az >= 5:  return "critical"
    if az >= 4:  return "high"
    if az >= 3:  return "medium"
    return "low"


# ── Detection checks ───────────────────────────────────────────────────────────

def _detect_revenue_outliers(conn) -> list[AnomalyRecord]:
    """Flag orders whose revenue deviates > threshold standard deviations."""
    sql = """
        SELECT order_sk, SUM(revenue) AS order_revenue
        FROM   fact_sales
        GROUP  BY order_sk
        HAVING COUNT(*) > 0
    """
    df = _load_df(conn, sql)
    if len(df) < ANOMALY.min_sample_size:
        log.info("Revenue outlier check skipped — insufficient data (%d rows)", len(df))
        return []

    df["z"] = _zscore(df["order_revenue"])
    flagged  = df[df["z"].abs() > ANOMALY.zscore_threshold]
    mean_rev = df["order_revenue"].mean()
    std_rev  = df["order_revenue"].std()

    records = []
    for _, row in flagged.iterrows():
        records.append(AnomalyRecord(
            entity="fact_sales",
            anomaly_type="revenue_outlier",
            affected_key=str(int(row["order_sk"])),
            metric_name="order_revenue",
            metric_value=float(row["order_revenue"]),
            expected_range=f"[{mean_rev - ANOMALY.zscore_threshold*std_rev:.2f}, "
                           f"{mean_rev + ANOMALY.zscore_threshold*std_rev:.2f}]",
            z_score=round(float(row["z"]), 4),
            severity=_severity(float(row["z"])),
            description=(
                f"Order {int(row['order_sk'])} revenue ${row['order_revenue']:.2f} "
                f"is {abs(row['z']):.1f}σ from mean ${mean_rev:.2f}"
            ),
        ))
    log.info("Revenue outliers: %d flagged out of %d orders", len(records), len(df))
    return records


def _detect_quantity_outliers(conn) -> list[AnomalyRecord]:
    """Flag line items with unusually high quantities."""
    sql = "SELECT order_sk, product_sk, quantity FROM fact_sales"
    df  = _load_df(conn, sql)
    if len(df) < ANOMALY.min_sample_size:
        return []

    lb, ub = _iqr_bounds(df["quantity"].astype(float), ANOMALY.iqr_multiplier)
    flagged = df[df["quantity"] > ub]

    records = []
    for _, row in flagged.iterrows():
        records.append(AnomalyRecord(
            entity="fact_sales",
            anomaly_type="quantity_outlier",
            affected_key=f"order={int(row['order_sk'])},product={int(row['product_sk'])}",
            metric_name="quantity",
            metric_value=float(row["quantity"]),
            expected_range=f"[{max(1, lb):.0f}, {ub:.0f}]",
            z_score=None,
            severity="medium" if row["quantity"] < ub * 2 else "high",
            description=(
                f"Order {int(row['order_sk'])} line item for product "
                f"{int(row['product_sk'])} has quantity {int(row['quantity'])} "
                f"(IQR upper fence: {ub:.1f})"
            ),
        ))
    log.info("Quantity outliers: %d flagged", len(records))
    return records


def _detect_price_anomalies(conn) -> list[AnomalyRecord]:
    """Flag products whose unit_price in fact_sales diverges from dim_product.price."""
    sql = """
        SELECT
            fs.product_sk,
            p.title,
            p.price        AS catalogue_price,
            AVG(fs.unit_price) AS avg_sold_price,
            STDDEV(fs.unit_price) AS std_sold_price
        FROM  fact_sales fs
        JOIN  dim_product p USING (product_sk)
        GROUP BY fs.product_sk, p.title, p.price
        HAVING COUNT(*) >= 3
    """
    df = _load_df(conn, sql)
    if df.empty:
        return []

    # Flag if average sold price differs > 10% from catalogue price
    df["pct_diff"] = abs(df["avg_sold_price"] - df["catalogue_price"]) / df["catalogue_price"].replace(0, 1)
    flagged = df[df["pct_diff"] > 0.10]

    records = []
    for _, row in flagged.iterrows():
        sev = "high" if row["pct_diff"] > 0.30 else "medium"
        records.append(AnomalyRecord(
            entity="dim_product",
            anomaly_type="price_discrepancy",
            affected_key=f"product={int(row['product_sk'])}",
            metric_name="price_pct_diff",
            metric_value=round(float(row["pct_diff"]) * 100, 2),
            expected_range="[0%, 10%]",
            z_score=None,
            severity=sev,
            description=(
                f"Product '{row['title']}' (id={int(row['product_sk'])}) "
                f"avg sold price ${row['avg_sold_price']:.2f} differs "
                f"{row['pct_diff']*100:.1f}% from catalogue ${row['catalogue_price']:.2f}"
            ),
        ))
    log.info("Price anomalies: %d flagged", len(records))
    return records


def _detect_daily_revenue_drop(conn) -> list[AnomalyRecord]:
    """
    Compare each day's revenue to the rolling 7-day average.
    Flag days that are > 40% below the rolling average.
    """
    sql = """
        SELECT date_sk, gross_revenue
        FROM   agg_daily_sales
        WHERE  gross_revenue > 0
        ORDER  BY date_sk
    """
    df = _load_df(conn, sql)
    if len(df) < 14:
        log.info("Daily revenue drop check skipped — insufficient history")
        return []

    df["rolling_7"]  = df["gross_revenue"].rolling(7, min_periods=3).mean().shift(1)
    df["pct_change"] = (df["gross_revenue"] - df["rolling_7"]) / df["rolling_7"].replace(0, np.nan)
    flagged = df[df["pct_change"] < -0.40].dropna(subset=["pct_change"])

    records = []
    for _, row in flagged.iterrows():
        records.append(AnomalyRecord(
            entity="agg_daily_sales",
            anomaly_type="revenue_drop",
            affected_key=str(int(row["date_sk"])),
            metric_name="gross_revenue",
            metric_value=float(row["gross_revenue"]),
            expected_range=f">=60% of rolling avg ${row['rolling_7']:.2f}",
            z_score=None,
            severity="high" if row["pct_change"] < -0.60 else "medium",
            description=(
                f"Date {int(row['date_sk'])}: revenue ${row['gross_revenue']:.2f} "
                f"is {abs(row['pct_change'])*100:.1f}% below "
                f"7-day rolling average ${row['rolling_7']:.2f}"
            ),
        ))
    log.info("Daily revenue drops: %d flagged", len(records))
    return records


def _detect_new_customers_spike(conn) -> list[AnomalyRecord]:
    """
    Detect if any customer placed an unusually high number of orders today
    compared to the platform average (possible bot / fraud signal).
    """
    sql = """
        SELECT customer_sk, total_orders, total_spend, ltv_tier
        FROM   agg_customer_ltv
    """
    df = _load_df(conn, sql)
    if len(df) < ANOMALY.min_sample_size:
        return []

    df["z_orders"] = _zscore(df["total_orders"].astype(float))
    df["z_spend"]  = _zscore(df["total_spend"].astype(float))
    flagged = df[(df["z_orders"].abs() > ANOMALY.zscore_threshold) |
                 (df["z_spend"].abs()  > ANOMALY.zscore_threshold)]

    records = []
    for _, row in flagged.iterrows():
        z = max(abs(row["z_orders"]), abs(row["z_spend"]))
        records.append(AnomalyRecord(
            entity="agg_customer_ltv",
            anomaly_type="customer_spend_spike",
            affected_key=f"customer={int(row['customer_sk'])}",
            metric_name="total_spend",
            metric_value=float(row["total_spend"]),
            expected_range=f"z-score within ±{ANOMALY.zscore_threshold}",
            z_score=round(float(z), 4),
            severity=_severity(z),
            description=(
                f"Customer {int(row['customer_sk'])} has {int(row['total_orders'])} orders "
                f"(z={row['z_orders']:.2f}) and ${row['total_spend']:.2f} spend "
                f"(z={row['z_spend']:.2f}) — potential fraud / bot"
            ),
        ))
    log.info("Customer spend spikes: %d flagged", len(records))
    return records


# ── Public callable ────────────────────────────────────────────────────────────

def run_anomaly_detection(run_ts: datetime | None = None) -> dict[str, Any]:
    """
    Run all anomaly detection checks and write results to anomaly_log.
    Returns summary dict.
    """
    run_ts = run_ts or datetime.now(timezone.utc)
    conn   = _pg()

    all_records: list[AnomalyRecord] = []
    checks = [
        ("revenue_outliers",      _detect_revenue_outliers),
        ("quantity_outliers",     _detect_quantity_outliers),
        ("price_anomalies",       _detect_price_anomalies),
        ("daily_revenue_drop",    _detect_daily_revenue_drop),
        ("customer_spend_spikes", _detect_new_customers_spike),
    ]

    check_summary: dict[str, int] = {}
    for check_name, fn in checks:
        try:
            results = fn(conn)
            all_records.extend(results)
            check_summary[check_name] = len(results)
        except Exception as exc:
            log.error("Anomaly check '%s' failed: %s", check_name, exc)
            check_summary[check_name] = -1   # -1 signals check failure

    total_written = _write_anomalies(conn, all_records)
    conn.close()

    # Severity breakdown
    severity_counts: dict[str, int] = {"low": 0, "medium": 0, "high": 0, "critical": 0}
    for r in all_records:
        severity_counts[r.severity] = severity_counts.get(r.severity, 0) + 1

    summary = {
        "run_ts":          run_ts.isoformat(),
        "total_anomalies": total_written,
        "by_check":        check_summary,
        "by_severity":     severity_counts,
    }
    log.info("Anomaly detection complete: %s", summary)
    return summary


if __name__ == "__main__":
    import pprint
    result = run_anomaly_detection()
    pprint.pprint(result)
