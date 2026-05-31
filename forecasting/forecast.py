# forecasting/forecast.py
"""
Sales Forecasting
══════════════════
Generates a 30-day rolling revenue forecast using the
Holt-Winters Exponential Smoothing model (additive trend + weekly seasonality).

If fewer than 2 seasonal cycles (14 days) of data are available, falls
back to a simple linear extrapolation.

Forecast outputs
────────────────
  • Predicted daily revenue (point estimate)
  • Lower / upper 95% confidence bounds
  • Stored in forecast_results table (one row per forecast date)
  • Also saved as Parquet to MinIO under processed/forecasts/

How to interpret results
────────────────────────
  • horizon_day=1  → tomorrow's predicted revenue
  • horizon_day=30 → predicted revenue 30 days from now
  • Confidence bounds widen with horizon (uncertainty increases)
"""

import io
import logging
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import psycopg2
import psycopg2.extras

sys.path.insert(0, str(Path(__file__).parent.parent))
from config import WAREHOUSE, FORECAST, LAKE, LOG_LEVEL, LOG_FORMAT

logging.basicConfig(level=LOG_LEVEL, format=LOG_FORMAT)
log = logging.getLogger("forecasting")


# ── DB helpers ─────────────────────────────────────────────────────────────────

def _pg():
    return psycopg2.connect(**WAREHOUSE.psycopg2_kwargs)


def _load_history(conn) -> pd.DataFrame:
    """Load daily revenue history from the warehouse."""
    sql = """
        SELECT
            d.full_date::DATE          AS ds,
            COALESCE(a.net_revenue, 0) AS y
        FROM   dim_date d
        LEFT JOIN agg_daily_sales a ON a.date_sk = d.date_sk
        WHERE  d.full_date <= CURRENT_DATE
        ORDER  BY d.full_date
    """
    with conn.cursor() as cur:
        cur.execute(sql)
        rows = cur.fetchall()
    df = pd.DataFrame(rows, columns=["ds", "y"])
    df["ds"] = pd.to_datetime(df["ds"])
    df["y"]  = df["y"].astype(float)
    log.info("Loaded %d days of revenue history", len(df))
    return df


def _write_forecasts(conn, rows: list[dict], run_ts: datetime) -> int:
    sql = """
        INSERT INTO forecast_results
            (forecast_run_ts, model_name, entity, forecast_date,
             predicted_value, lower_bound, upper_bound, confidence, horizon_day)
        VALUES
            (%(run_ts)s, %(model)s, %(entity)s, %(date)s,
             %(pred)s, %(lower)s, %(upper)s, %(ci)s, %(horizon)s)
        ON CONFLICT (forecast_run_ts, entity, forecast_date)
        DO UPDATE SET
            predicted_value = EXCLUDED.predicted_value,
            lower_bound     = EXCLUDED.lower_bound,
            upper_bound     = EXCLUDED.upper_bound
    """
    with conn.cursor() as cur:
        psycopg2.extras.execute_batch(cur, sql, rows, page_size=100)
    conn.commit()
    log.info("Saved %d forecast rows to forecast_results", len(rows))
    return len(rows)


# ── Forecasting models ─────────────────────────────────────────────────────────

def _holt_winters_forecast(
    history: pd.Series,
    horizon: int,
    season_len: int,
    confidence: float,
) -> pd.DataFrame:
    """
    Additive Holt-Winters exponential smoothing.
    Returns DataFrame with columns: pred, lower, upper.
    Falls back to simple linear trend if statsmodels is unavailable.
    """
    try:
        from statsmodels.tsa.holtwinters import ExponentialSmoothing

        if len(history) < season_len * 2:
            raise ValueError("Insufficient data for seasonal model")

        model  = ExponentialSmoothing(
            history,
            trend="add",
            seasonal="add",
            seasonal_periods=season_len,
            initialization_method="estimated",
        )
        fitted = model.fit(optimized=True, use_brute=True)
        pred   = fitted.forecast(horizon)

        # Compute confidence intervals from simulation
        simulations = fitted.simulate(horizon, repetitions=200, error="add")
        z = 1.96 if confidence >= 0.95 else 1.645  # 95% or 90%
        std = simulations.std(axis=1)

        return pd.DataFrame({
            "pred":  pred.values,
            "lower": np.maximum(0, pred.values - z * std.values),
            "upper": pred.values + z * std.values,
        })

    except Exception as exc:
        log.warning("Holt-Winters failed (%s) — falling back to linear trend", exc)
        return _linear_fallback(history, horizon, confidence)


def _linear_fallback(
    history: pd.Series,
    horizon: int,
    confidence: float,
) -> pd.DataFrame:
    """
    Simple linear regression fallback when Holt-Winters is not applicable.
    """
    n     = len(history)
    x     = np.arange(n)
    y     = history.values
    slope, intercept = np.polyfit(x, y, 1)

    preds = np.array([slope * (n + i) + intercept for i in range(horizon)])
    preds = np.maximum(0, preds)

    # Widen PI proportional to horizon
    residuals = y - (slope * x + intercept)
    std       = residuals.std()
    z         = 1.96 if confidence >= 0.95 else 1.645

    widths = np.array([z * std * (1 + i / n) for i in range(horizon)])
    return pd.DataFrame({
        "pred":  preds,
        "lower": np.maximum(0, preds - widths),
        "upper": preds + widths,
    })


# ── MinIO output ───────────────────────────────────────────────────────────────

def _save_parquet(forecast_df: pd.DataFrame, run_ts: datetime) -> str:
    """Save forecast to MinIO as Parquet for audit trail."""
    try:
        import boto3
        from botocore.client import Config

        client = boto3.client(
            "s3",
            endpoint_url=LAKE.endpoint,
            aws_access_key_id=LAKE.access_key,
            aws_secret_access_key=LAKE.secret_key,
            config=Config(signature_version="s3v4"),
            region_name="us-east-1",
        )
        ts_str = run_ts.strftime("%Y%m%dT%H%M%SZ")
        key    = f"processed/forecasts/daily_revenue_{ts_str}.parquet"

        buf = io.BytesIO()
        forecast_df.to_parquet(buf, index=False, engine="pyarrow")
        buf.seek(0)
        client.put_object(
            Bucket=LAKE.bucket, Key=key,
            Body=buf.getvalue(), ContentType="application/octet-stream",
        )
        log.info("Forecast Parquet → s3://%s/%s", LAKE.bucket, key)
        return key
    except Exception as exc:
        log.warning("Could not save forecast Parquet: %s", exc)
        return ""


# ── Public callable ────────────────────────────────────────────────────────────

def run_forecast(run_ts: datetime | None = None) -> dict[str, Any]:
    """
    Load revenue history, fit Holt-Winters, write forecast to DB + MinIO.
    Returns summary dict.
    """
    run_ts  = run_ts or datetime.now(timezone.utc)
    horizon = FORECAST.horizon_days
    season  = FORECAST.seasonality
    ci      = FORECAST.confidence

    conn    = _pg()
    history = _load_history(conn)

    if history.empty or history["y"].sum() == 0:
        log.warning("No revenue history available — forecast skipped")
        conn.close()
        return {"status": "skipped", "reason": "no_history"}

    # Use only non-zero tail (drop leading zeros)
    non_zero = history[history["y"] > 0]
    series   = non_zero.set_index("ds")["y"]

    log.info("Fitting %s model on %d days of history, horizon=%d days",
             FORECAST.model, len(series), horizon)

    fcast_df = _holt_winters_forecast(series, horizon, season, ci)

    # Build date index for forecast window
    last_date = history["ds"].max()
    dates     = [last_date + timedelta(days=i + 1) for i in range(horizon)]

    fcast_df["forecast_date"] = dates
    fcast_df["horizon_day"]   = range(1, horizon + 1)
    fcast_df["pred"]   = fcast_df["pred"].round(4)
    fcast_df["lower"]  = fcast_df["lower"].round(4)
    fcast_df["upper"]  = fcast_df["upper"].round(4)

    # Write to DB
    db_rows = [
        {
            "run_ts":  run_ts,
            "model":   FORECAST.model,
            "entity":  "daily_revenue",
            "date":    row["forecast_date"].date().isoformat(),
            "pred":    row["pred"],
            "lower":   row["lower"],
            "upper":   row["upper"],
            "ci":      ci,
            "horizon": row["horizon_day"],
        }
        for _, row in fcast_df.iterrows()
    ]
    rows_written = _write_forecasts(conn, db_rows, run_ts)
    conn.close()

    # Save Parquet
    parquet_key = _save_parquet(fcast_df, run_ts)

    # Log next-week summary
    week_pred = fcast_df[fcast_df["horizon_day"] <= 7]["pred"]
    log.info(
        "Forecast summary (next 7 days): mean=$%.2f, min=$%.2f, max=$%.2f",
        week_pred.mean(), week_pred.min(), week_pred.max(),
    )

    return {
        "status":         "success",
        "model":          FORECAST.model,
        "horizon_days":   horizon,
        "rows_written":   rows_written,
        "parquet_key":    parquet_key,
        "next_7d_avg":    round(float(week_pred.mean()), 2),
        "next_7d_min":    round(float(week_pred.min()), 2),
        "next_7d_max":    round(float(week_pred.max()), 2),
        "history_days":   len(series),
    }


if __name__ == "__main__":
    import pprint
    result = run_forecast()
    pprint.pprint(result)
