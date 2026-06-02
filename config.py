# config.py
"""
Central configuration for the Enterprise Data Platform.
All modules import from here — never hardcode credentials elsewhere.
Environment variables take precedence over defaults.
"""

import os
from dataclasses import dataclass, field
from pathlib import Path

ROOT_DIR = Path(__file__).parent


# ── API ────────────────────────────────────────────────────────────────────────

@dataclass
class APIConfig:
    base_url:    str = os.getenv("API_BASE_URL", "https://fakestoreapi.com")
    timeout_sec: int = int(os.getenv("API_TIMEOUT", "30"))
    max_retries: int = int(os.getenv("API_MAX_RETRIES", "3"))
    retry_backoff: float = 2.0   # exponential backoff multiplier

    endpoints: dict = field(default_factory=lambda: {
        "orders":    "/carts",
        "customers": "/users",
        "products":  "/products",
    })


# ── MinIO / S3 (Data Lake) ─────────────────────────────────────────────────────

@dataclass
class LakeConfig:
    endpoint:   str = os.getenv("MINIO_ENDPOINT",   "http://localhost:9000")
    access_key: str = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
    secret_key: str = os.getenv("MINIO_SECRET_KEY", "minioadmin")
    bucket:     str = os.getenv("MINIO_BUCKET",     "enterprise-lake")

    # Layer prefixes
    raw_prefix:       str = "raw"
    processed_prefix: str = "processed"
    archive_prefix:   str = "archive"

    # Local mirror paths (used by the archive job for cold storage)
    local_raw:       Path = ROOT_DIR / "data_lake" / "raw"
    local_processed: Path = ROOT_DIR / "data_lake" / "processed"
    local_archive:   Path = ROOT_DIR / "data_lake" / "archive"


# ── PostgreSQL Warehouse ───────────────────────────────────────────────────────

@dataclass
class WarehouseConfig:
    host:     str = os.getenv("POSTGRES_HOST",     "localhost")
    port:     int = int(os.getenv("POSTGRES_PORT", "5432"))
    database: str = os.getenv("POSTGRES_DB",       "data_warehouse")
    user:     str = os.getenv("POSTGRES_USER",     "warehouse")
    password: str = os.getenv("POSTGRES_PASSWORD", "warehouse")

    @property
    def conn_str(self) -> str:
        return (
            f"postgresql://{self.user}:{self.password}"
            f"@{self.host}:{self.port}/{self.database}"
        )

    @property
    def psycopg2_kwargs(self) -> dict:
        return dict(
            host=self.host, port=self.port,
            dbname=self.database,
            user=self.user, password=self.password,
        )


# ── Kafka (Streaming Layer) ────────────────────────────────────────────────────

@dataclass
class KafkaConfig:
    # Bootstrap servers — comma-separated for multi-broker setups
    bootstrap_servers: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")

    # Topics
    orders_topic:    str = os.getenv("KAFKA_ORDERS_TOPIC",    "orders-raw")
    customers_topic: str = os.getenv("KAFKA_CUSTOMERS_TOPIC", "customers-raw")
    products_topic:  str = os.getenv("KAFKA_PRODUCTS_TOPIC",  "products-raw")
    events_topic:    str = os.getenv("KAFKA_EVENTS_TOPIC",    "pipeline-events")

    # Consumer
    consumer_group:      str = os.getenv("KAFKA_CONSUMER_GROUP",      "enterprise-etl")
    consumer_timeout_ms: int = int(os.getenv("KAFKA_CONSUMER_TIMEOUT_MS", "10000"))
    auto_offset_reset:   str = os.getenv("KAFKA_AUTO_OFFSET_RESET",   "earliest")

    # Producer
    acks:               str = os.getenv("KAFKA_ACKS",               "all")  # strongest guarantee
    retries:            int = int(os.getenv("KAFKA_RETRIES",        "3"))
    compression_type:   str = os.getenv("KAFKA_COMPRESSION",        "snappy")

    @property
    def producer_config(self) -> dict:
        return {
            "bootstrap.servers": self.bootstrap_servers,
            "acks":              self.acks,
            "retries":           self.retries,
            "compression.type":  self.compression_type,
            "linger.ms":         10,       # batch up to 10ms for throughput
            "batch.size":        16384,
        }

    @property
    def consumer_config(self) -> dict:
        return {
            "bootstrap.servers":  self.bootstrap_servers,
            "group.id":           self.consumer_group,
            "auto.offset.reset":  self.auto_offset_reset,
            "enable.auto.commit": True,
        }

    @property
    def all_topics(self) -> list[str]:
        return [self.orders_topic, self.customers_topic, self.products_topic]


# ── Apache Spark (Distributed Transform) ──────────────────────────────────────

@dataclass
class SparkConfig:
    # Use "local[*]" for single-node (laptop/CI), "spark://spark-master:7077" for cluster
    master_url:  str = os.getenv("SPARK_MASTER_URL",  "local[*]")
    app_name:    str = os.getenv("SPARK_APP_NAME",    "enterprise-data-platform")
    log_level:   str = os.getenv("SPARK_LOG_LEVEL",   "WARN")

    # S3A (MinIO) config used by Spark to read/write Parquet
    s3a_endpoint:   str = os.getenv("MINIO_ENDPOINT",   "http://localhost:9000")
    s3a_access_key: str = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
    s3a_secret_key: str = os.getenv("MINIO_SECRET_KEY", "minioadmin")
    s3a_path_style: str = "true"   # required for MinIO (non-AWS S3)

    # Memory / executor tuning (sane defaults for a dev laptop)
    driver_memory:   str = os.getenv("SPARK_DRIVER_MEMORY",   "1g")
    executor_memory: str = os.getenv("SPARK_EXECUTOR_MEMORY", "1g")
    executor_cores:  int = int(os.getenv("SPARK_EXECUTOR_CORES", "2"))

    @property
    def spark_conf(self) -> dict:
        """Return a flat dict suitable for SparkConf.setAll() / SparkSession builder."""
        return {
            "spark.master":                    self.master_url,
            "spark.app.name":                  self.app_name,
            # S3A connector for MinIO
            "spark.hadoop.fs.s3a.endpoint":                self.s3a_endpoint,
            "spark.hadoop.fs.s3a.access.key":              self.s3a_access_key,
            "spark.hadoop.fs.s3a.secret.key":              self.s3a_secret_key,
            "spark.hadoop.fs.s3a.path.style.access":       self.s3a_path_style,
            "spark.hadoop.fs.s3a.impl":                    "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.hadoop.fs.s3a.aws.credentials.provider":"org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
            # Performance
            "spark.driver.memory":             self.driver_memory,
            "spark.executor.memory":           self.executor_memory,
            "spark.executor.cores":            str(self.executor_cores),
            "spark.sql.adaptive.enabled":      "true",
            "spark.sql.adaptive.coalescePartitions.enabled": "true",
            # Parquet optimisations
            "spark.sql.parquet.compression.codec": "snappy",
            "spark.sql.parquet.mergeSchema":        "false",
        }


# ── Anomaly Detection ──────────────────────────────────────────────────────────

@dataclass
class AnomalyConfig:
    # Z-score threshold above which an order amount is flagged
    zscore_threshold:   float = float(os.getenv("ANOMALY_ZSCORE",   "3.0"))
    # IQR multiplier for fence-based detection
    iqr_multiplier:     float = float(os.getenv("ANOMALY_IQR",      "1.5"))
    # Minimum records needed before anomaly detection runs
    min_sample_size:    int   = int(os.getenv("ANOMALY_MIN_SAMPLE", "30"))


# ── Forecasting ────────────────────────────────────────────────────────────────

@dataclass
class ForecastConfig:
    horizon_days:   int   = int(os.getenv("FORECAST_HORIZON",   "30"))
    seasonality:    int   = int(os.getenv("FORECAST_SEASON",    "7"))   # weekly
    confidence:     float = float(os.getenv("FORECAST_CI",      "0.95"))
    model:          str   = os.getenv("FORECAST_MODEL",          "holt_winters")


# ── Logging ────────────────────────────────────────────────────────────────────

LOG_LEVEL  = os.getenv("LOG_LEVEL",  "INFO")
LOG_FORMAT = "%(asctime)s [%(levelname)s] %(name)s — %(message)s"


# ── Singleton instances (import these in other modules) ────────────────────────

API       = APIConfig()
LAKE      = LakeConfig()
WAREHOUSE = WarehouseConfig()
KAFKA     = KafkaConfig()
SPARK     = SparkConfig()
ANOMALY   = AnomalyConfig()
FORECAST  = ForecastConfig()
