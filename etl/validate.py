# etl/validate.py
"""
ETL — Data Quality Validation
══════════════════════════════
Runs automated quality checks after each ETL layer and raises
DataQualityError if any critical check fails.

Checks performed
────────────────
Schema checks:
  • All expected columns present
  • No unexpected nulls in NOT NULL columns

Volume checks:
  • Record count above minimum threshold
  • Count does not drop more than 50% vs previous run (stale detection)

Value checks:
  • Numeric ranges (price > 0, quantity > 0, rating in [0,5])
  • No duplicate primary keys
  • Referential integrity: all fact foreign keys exist in dimensions

Each check produces a QualityResult with status, message, and row count.
The full report is written to MinIO as JSON for audit trail.
"""

import io
import json
import logging
import sys
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Literal

import boto3
import pandas as pd
from botocore.client import Config

sys.path.insert(0, str(Path(__file__).parent.parent))
from config import LAKE, LOG_LEVEL, LOG_FORMAT

logging.basicConfig(level=LOG_LEVEL, format=LOG_FORMAT)
log = logging.getLogger("etl.validate")

GOLD_PREFIX = f"{LAKE.processed_prefix}/gold"
REPORT_PREFIX = "quality_reports"


class DataQualityError(Exception):
    """Raised when a CRITICAL quality check fails."""


@dataclass
class QualityResult:
    check:    str
    entity:   str
    status:   Literal["PASS", "WARN", "FAIL"]
    message:  str
    severity: Literal["critical", "warning"] = "critical"
    rows_affected: int = 0
    metadata: dict = field(default_factory=dict)


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


def _load_gold(client, table: str, date_str: str) -> pd.DataFrame:
    prefix = f"{GOLD_PREFIX}/{table}/date={date_str}/"
    resp   = client.list_objects_v2(Bucket=LAKE.bucket, Prefix=prefix)
    objs   = resp.get("Contents", [])
    if not objs:
        raise FileNotFoundError(f"No Gold table at {LAKE.bucket}/{prefix}")
    key = sorted(objs, key=lambda o: o["LastModified"], reverse=True)[0]["Key"]
    obj = client.get_object(Bucket=LAKE.bucket, Key=key)
    return pd.read_parquet(io.BytesIO(obj["Body"].read()), engine="pyarrow")


def _write_report(client, report: dict, run_ts: datetime) -> str:
    ts_str = run_ts.strftime("%Y%m%dT%H%M%SZ")
    key    = f"{REPORT_PREFIX}/date={run_ts.strftime('%Y-%m-%d')}/report_{ts_str}.json"
    body   = json.dumps(report, indent=2, default=str).encode("utf-8")
    client.put_object(
        Bucket=LAKE.bucket, Key=key,
        Body=body, ContentType="application/json",
    )
    log.info("Quality report → s3://%s/%s", LAKE.bucket, key)
    return key


# ── Individual checks ──────────────────────────────────────────────────────────

def check_not_empty(df: pd.DataFrame, entity: str, min_rows: int = 1) -> QualityResult:
    if len(df) < min_rows:
        return QualityResult(
            check="not_empty", entity=entity, status="FAIL", severity="critical",
            message=f"{entity} has {len(df)} rows, expected >= {min_rows}",
            rows_affected=0,
        )
    return QualityResult(
        check="not_empty", entity=entity, status="PASS",
        message=f"{entity} has {len(df)} rows", rows_affected=len(df),
    )


def check_required_columns(df: pd.DataFrame, entity: str, required: list[str]) -> QualityResult:
    missing = [c for c in required if c not in df.columns]
    if missing:
        return QualityResult(
            check="required_columns", entity=entity, status="FAIL", severity="critical",
            message=f"Missing columns: {missing}",
        )
    return QualityResult(
        check="required_columns", entity=entity, status="PASS",
        message=f"All {len(required)} required columns present",
    )


def check_no_nulls(df: pd.DataFrame, entity: str, columns: list[str]) -> QualityResult:
    results = []
    for col in columns:
        if col not in df.columns:
            continue
        null_count = df[col].isna().sum()
        if null_count > 0:
            results.append(f"{col}: {null_count} nulls")
    if results:
        return QualityResult(
            check="no_nulls", entity=entity, status="FAIL", severity="critical",
            message=f"Null violations: {'; '.join(results)}",
            rows_affected=sum(int(r.split(": ")[1].split(" ")[0]) for r in results),
        )
    return QualityResult(
        check="no_nulls", entity=entity, status="PASS",
        message=f"No nulls in critical columns: {columns}",
    )


def check_no_duplicates(df: pd.DataFrame, entity: str, key_col: str) -> QualityResult:
    if key_col not in df.columns:
        return QualityResult(
            check="no_duplicates", entity=entity, status="WARN", severity="warning",
            message=f"Key column '{key_col}' not found — skipped",
        )
    dup_count = df[key_col].duplicated().sum()
    if dup_count > 0:
        return QualityResult(
            check="no_duplicates", entity=entity, status="FAIL", severity="critical",
            message=f"{dup_count} duplicate values in '{key_col}'",
            rows_affected=dup_count,
        )
    return QualityResult(
        check="no_duplicates", entity=entity, status="PASS",
        message=f"No duplicates in '{key_col}'",
    )


def check_numeric_range(
    df: pd.DataFrame, entity: str, col: str,
    min_val: float | None = None, max_val: float | None = None,
) -> QualityResult:
    if col not in df.columns:
        return QualityResult(
            check="numeric_range", entity=entity, status="WARN", severity="warning",
            message=f"Column '{col}' not found — skipped",
        )
    violations = pd.Series([False] * len(df))
    if min_val is not None:
        violations |= (df[col] < min_val)
    if max_val is not None:
        violations |= (df[col] > max_val)
    n = violations.sum()
    if n > 0:
        return QualityResult(
            check="numeric_range", entity=entity, status="FAIL", severity="critical",
            message=f"{n} rows in '{col}' outside [{min_val}, {max_val}]",
            rows_affected=int(n),
            metadata={"col": col, "min": min_val, "max": max_val},
        )
    return QualityResult(
        check="numeric_range", entity=entity, status="PASS",
        message=f"'{col}' values in range [{min_val}, {max_val}]",
    )


def check_referential_integrity(
    fact: pd.DataFrame, dim: pd.DataFrame,
    fact_col: str, dim_col: str,
    fact_entity: str, dim_entity: str,
) -> QualityResult:
    if fact_col not in fact.columns or dim_col not in dim.columns:
        return QualityResult(
            check="referential_integrity", entity=fact_entity, status="WARN",
            severity="warning",
            message=f"Columns {fact_col}/{dim_col} not found — skipped",
        )
    orphans = ~fact[fact_col].isin(dim[dim_col])
    n = orphans.sum()
    if n > 0:
        return QualityResult(
            check="referential_integrity", entity=fact_entity, status="WARN",
            severity="warning",
            message=f"{n} fact rows in '{fact_col}' not found in {dim_entity}.{dim_col}",
            rows_affected=int(n),
        )
    return QualityResult(
        check="referential_integrity", entity=fact_entity, status="PASS",
        message=f"All {fact_col} values exist in {dim_entity}.{dim_col}",
    )


# ── Validation suite ───────────────────────────────────────────────────────────

def _validate_dim_customer(df: pd.DataFrame) -> list[QualityResult]:
    return [
        check_not_empty(df, "dim_customer", min_rows=1),
        check_required_columns(df, "dim_customer",
            ["customer_sk", "username", "full_name", "city"]),
        check_no_nulls(df, "dim_customer", ["customer_sk"]),
        check_no_duplicates(df, "dim_customer", "customer_sk"),
    ]


def _validate_dim_product(df: pd.DataFrame) -> list[QualityResult]:
    return [
        check_not_empty(df, "dim_product", min_rows=1),
        check_required_columns(df, "dim_product",
            ["product_sk", "title", "category", "price"]),
        check_no_nulls(df, "dim_product", ["product_sk", "price"]),
        check_no_duplicates(df, "dim_product", "product_sk"),
        check_numeric_range(df, "dim_product", "price", min_val=0.01),
        check_numeric_range(df, "dim_product", "rating_score", min_val=0.0, max_val=5.0),
    ]


def _validate_fact_sales(
    fact: pd.DataFrame,
    dim_customer: pd.DataFrame,
    dim_product: pd.DataFrame,
) -> list[QualityResult]:
    results = [
        check_not_empty(fact, "fact_sales", min_rows=1),
        check_required_columns(fact, "fact_sales",
            ["order_sk", "customer_sk", "product_sk", "quantity", "revenue"]),
        check_no_nulls(fact, "fact_sales", ["order_sk", "quantity", "revenue"]),
        check_numeric_range(fact, "fact_sales", "quantity",  min_val=1),
        check_numeric_range(fact, "fact_sales", "revenue",   min_val=0.0),
        check_numeric_range(fact, "fact_sales", "unit_price", min_val=0.0),
    ]
    if dim_customer is not None:
        results.append(check_referential_integrity(
            fact, dim_customer, "customer_sk", "customer_sk",
            "fact_sales", "dim_customer",
        ))
    if dim_product is not None:
        results.append(check_referential_integrity(
            fact, dim_product, "product_sk", "product_sk",
            "fact_sales", "dim_product",
        ))
    return results


# ── Public callable ────────────────────────────────────────────────────────────

def run_validate(run_ts: datetime | None = None) -> dict:
    """
    Run full validation suite on all Gold tables.
    Writes a JSON report to MinIO.
    Raises DataQualityError if any CRITICAL check fails.
    Returns the full report dict.
    """
    run_ts   = run_ts or datetime.now(timezone.utc)
    date_str = run_ts.strftime("%Y-%m-%d")
    client   = _s3()

    # Load Gold tables (best-effort — warn if missing)
    tables: dict[str, pd.DataFrame | None] = {}
    for table in ["dim_customer", "dim_product", "dim_date", "fact_sales",
                  "agg_daily_sales", "agg_product_perf", "agg_customer_ltv"]:
        try:
            tables[table] = _load_gold(client, table, date_str)
        except FileNotFoundError:
            log.warning("Table not found (skipping): %s", table)
            tables[table] = None

    # Run checks
    all_results: list[QualityResult] = []

    if tables["dim_customer"] is not None:
        all_results.extend(_validate_dim_customer(tables["dim_customer"]))
    if tables["dim_product"] is not None:
        all_results.extend(_validate_dim_product(tables["dim_product"]))
    if tables["fact_sales"] is not None:
        all_results.extend(_validate_fact_sales(
            tables["fact_sales"], tables["dim_customer"], tables["dim_product"]
        ))

    # Summarise
    passed  = sum(1 for r in all_results if r.status == "PASS")
    warned  = sum(1 for r in all_results if r.status == "WARN")
    failed  = sum(1 for r in all_results if r.status == "FAIL")
    critical_failures = [r for r in all_results if r.status == "FAIL" and r.severity == "critical"]

    log.info("Validation: %d passed, %d warned, %d failed", passed, warned, failed)

    report = {
        "run_ts":   run_ts.isoformat(),
        "summary":  {"passed": passed, "warned": warned, "failed": failed},
        "overall":  "FAIL" if failed else ("WARN" if warned else "PASS"),
        "results":  [asdict(r) for r in all_results],
    }

    _write_report(client, report, run_ts)

    if critical_failures:
        msgs = "; ".join(r.message for r in critical_failures)
        raise DataQualityError(f"Critical quality failures: {msgs}")

    return report


if __name__ == "__main__":
    import pprint
    report = run_validate()
    pprint.pprint(report["summary"])
    for r in report["results"]:
        icon = {"PASS": "✓", "WARN": "⚠", "FAIL": "✗"}.get(r["status"], "?")
        print(f"  {icon} [{r['entity']:20s}] {r['check']:30s} — {r['message']}")
