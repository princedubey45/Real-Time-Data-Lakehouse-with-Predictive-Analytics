# spark/spark_session.py
"""
Shared SparkSession factory for the Enterprise Data Platform.

Centralises all Spark + S3A (MinIO) configuration so every PySpark
job gets an identically configured session without duplicating settings.

Usage::

    from spark.spark_session import get_spark_session, stop_spark

    spark = get_spark_session()
    df = spark.read.parquet("s3a://enterprise-lake/processed/orders/...")
    spark.stop()
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path

# Ensure root project is on path (handles both Docker and local runs)
sys.path.insert(0, str(Path(__file__).parent.parent))

from config import SPARK, LOG_LEVEL, LOG_FORMAT

logging.basicConfig(level=LOG_LEVEL, format=LOG_FORMAT)
log = logging.getLogger("spark.session")

_session = None   # module-level singleton


def get_spark_session(app_name: str | None = None):
    """
    Return a singleton SparkSession configured for MinIO/S3A access.

    Parameters
    ----------
    app_name : str, optional
        Override the app name from config.

    Returns
    -------
    pyspark.sql.SparkSession
    """
    global _session

    if _session is not None and not _session._sc._jvm.SparkContext.getOrCreate().isStopped:
        return _session

    try:
        from pyspark.sql import SparkSession
    except ImportError as exc:
        raise RuntimeError(
            "PySpark not installed. Run: pip install pyspark==3.5.1"
        ) from exc

    name = app_name or SPARK.app_name
    log.info("Initialising SparkSession: %s  master=%s", name, SPARK.master_url)

    builder = SparkSession.builder.appName(name)

    # Apply all Spark + S3A config from SPARK singleton
    for key, val in SPARK.spark_conf.items():
        builder = builder.config(key, val)

    # Extra S3A settings not in spark_conf to keep it clean
    builder = (
        builder
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.attempts.maximum", "3")
        # Required jars for S3A — bundled in bitnami/spark image
        # For local dev, pyspark downloads these via ivy on first run
        .config(
            "spark.jars.packages",
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.262",
        )
    )

    _session = builder.getOrCreate()
    _session.sparkContext.setLogLevel(SPARK.log_level)

    log.info(
        "SparkSession ready  version=%s  master=%s",
        _session.version,
        _session.sparkContext.master,
    )
    return _session


def stop_spark() -> None:
    """Stop the active SparkSession and reset the singleton."""
    global _session
    if _session is not None:
        log.info("Stopping SparkSession")
        _session.stop()
        _session = None
