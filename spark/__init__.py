# spark/__init__.py
"""
spark — Distributed transform layer using Apache PySpark.

Reads Silver Parquet from MinIO via S3A, applies distributed Gold
transforms using Spark DataFrames and Spark SQL, then writes Gold
Parquet back to MinIO.

Public API
──────────
    from spark.spark_session  import get_spark_session, stop_spark
    from spark.spark_transform import run_spark_transform
    from spark.schemas         import SILVER_SCHEMAS
"""

__all__ = ["get_spark_session", "stop_spark", "run_spark_transform", "SILVER_SCHEMAS"]
