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
ANOMALY   = AnomalyConfig()
FORECAST  = ForecastConfig()
