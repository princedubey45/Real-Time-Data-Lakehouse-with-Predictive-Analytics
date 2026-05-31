# etl/load_warehouse.py
"""
ETL — Load Warehouse
═════════════════════
Final ETL step: reads all Gold Parquet tables from MinIO and
upserts them into the PostgreSQL data warehouse.

Load strategy per table
───────────────────────
dim_customer      → UPSERT on customer_sk  (SCD Type 1 overwrite)
dim_product       → UPSERT on product_sk
dim_date          → UPSERT on date_sk  (idempotent — same dates always same data)
fact_sales        → UPSERT on (order_sk, product_sk) composite key
agg_daily_sales   → UPSERT on date_sk  (replace yesterday's partial with final)
agg_product_perf  → UPSERT on product_sk
agg_customer_ltv  → UPSERT on customer_sk

All loads are batched at 500 rows per execute_batch call.
The entire load is wrapped in a transaction; any failure rolls back.
A load_audit row is written on completion for observability.
"""

import io
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import boto3
import pandas as pd
import psycopg2
import psycopg2.extras
from botocore.client import Config

sys.path.insert(0, str(Path(__file__).parent.parent))
from config import LAKE, WAREHOUSE, LOG_LEVEL, LOG_FORMAT

logging.basicConfig(level=LOG_LEVEL, format=LOG_FORMAT)
log = logging.getLogger("etl.load")

GOLD_PREFIX = f"{LAKE.processed_prefix}/gold"


# ── MinIO ──────────────────────────────────────────────────────────────────────

def _s3():
    return boto3.client(
        "s3",
        endpoint_url=LAKE.endpoint,
        aws_access_key_id=LAKE.access_key,
        aws_secret_access_key=LAKE.secret_key,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )


def _latest_gold(client, table: str, date_str: str) -> pd.DataFrame:
    prefix = f"{GOLD_PREFIX}/{table}/date={date_str}/"
    resp   = client.list_objects_v2(Bucket=LAKE.bucket, Prefix=prefix)
    objs   = resp.get("Contents", [])
    if not objs:
        raise FileNotFoundError(f"No Gold Parquet at {LAKE.bucket}/{prefix}")
    key = sorted(objs, key=lambda o: o["LastModified"], reverse=True)[0]["Key"]
    obj = client.get_object(Bucket=LAKE.bucket, Key=key)
    df  = pd.read_parquet(io.BytesIO(obj["Body"].read()), engine="pyarrow")
    log.info("  Loaded Gold %-22s  %d rows", table, len(df))
    return df


# ── PostgreSQL helpers ─────────────────────────────────────────────────────────

def _pg():
    return psycopg2.connect(**WAREHOUSE.psycopg2_kwargs)


def _upsert(conn, pg_table: str, df: pd.DataFrame, conflict_cols: list[str],
            batch_size: int = 500) -> int:
    """
    INSERT … ON CONFLICT (conflict_cols) DO UPDATE SET all other columns.
    Returns number of rows processed.
    """
    if df.empty:
        log.warning("  Empty DataFrame for %s — skipped", pg_table)
        return 0

    # Sanitise column names (Pandas may use reserved words)
    df = df.copy()

    cols        = list(df.columns)
    update_cols = [c for c in cols if c not in conflict_cols]
    ph          = ", ".join(["%s"] * len(cols))
    conflict_str = ", ".join(conflict_cols)
    update_str   = ", ".join(f'"{c}" = EXCLUDED."{c}"' for c in update_cols)
    col_str      = ", ".join(f'"{c}"' for c in cols)

    sql = (
        f'INSERT INTO {pg_table} ({col_str}) VALUES ({ph}) '
        f'ON CONFLICT ({conflict_str}) DO UPDATE SET {update_str};'
    )

    # Convert pandas NA / NaT to None for psycopg2
    rows = [
        tuple(None if pd.isna(v) else v for v in row)
        for row in df.itertuples(index=False, name=None)
    ]

    with conn.cursor() as cur:
        psycopg2.extras.execute_batch(cur, sql, rows, page_size=batch_size)

    log.info("  Upserted %d rows → %s", len(rows), pg_table)
    return len(rows)


def _write_audit(conn, table: str, rows: int, run_ts: datetime,
                 status: str = "success", error: str | None = None) -> None:
    sql = """
        INSERT INTO load_audit
            (table_name, rows_loaded, run_ts, status, error_message)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT DO NOTHING;
    """
    with conn.cursor() as cur:
        cur.execute(sql, (table, rows, run_ts, status, error))
    conn.commit()


# ── Load plan ──────────────────────────────────────────────────────────────────
#   gold_table_name  →  (postgres_table_name, [conflict_columns])

LOAD_PLAN: list[tuple[str, str, list[str]]] = [
    ("dim_customer",     "dim_customer",     ["customer_sk"]),
    ("dim_product",      "dim_product",      ["product_sk"]),
    ("dim_date",         "dim_date",         ["date_sk"]),
    ("fact_sales",       "fact_sales",       ["order_sk", "product_sk"]),
    ("agg_daily_sales",  "agg_daily_sales",  ["date_sk"]),
    ("agg_product_perf", "agg_product_perf", ["product_sk"]),
    ("agg_customer_ltv", "agg_customer_ltv", ["customer_sk"]),
]


# ── Public callable ────────────────────────────────────────────────────────────

def run_load(run_ts: datetime | None = None) -> dict[str, int]:
    """
    Load all Gold tables into PostgreSQL.
    Returns mapping: table_name → rows upserted.
    """
    run_ts   = run_ts or datetime.now(timezone.utc)
    date_str = run_ts.strftime("%Y-%m-%d")

    s3_client = _s3()
    conn      = _pg()
    results: dict[str, int] = {}

    try:
        for gold_table, pg_table, conflict_cols in LOAD_PLAN:
            log.info("Loading: %s → %s", gold_table, pg_table)
            try:
                df   = _latest_gold(s3_client, gold_table, date_str)
                rows = _upsert(conn, pg_table, df, conflict_cols)
                conn.commit()
                results[pg_table] = rows
                _write_audit(conn, pg_table, rows, run_ts, status="success")
            except FileNotFoundError as exc:
                log.warning("  Skipping %s — not found: %s", gold_table, exc)
                results[pg_table] = 0
            except Exception as exc:
                conn.rollback()
                log.error("  FAILED %s: %s", pg_table, exc)
                _write_audit(conn, pg_table, 0, run_ts, status="failed", error=str(exc))
                raise
    finally:
        conn.close()

    total = sum(results.values())
    log.info("Warehouse load complete. Total rows upserted: %d", total)
    return results


if __name__ == "__main__":
    result = run_load()
    print("\nLoad results:")
    for table, count in result.items():
        print(f"  {table:25s}: {count:>6} rows")
    print(f"  {'TOTAL':25s}: {sum(result.values()):>6} rows")
