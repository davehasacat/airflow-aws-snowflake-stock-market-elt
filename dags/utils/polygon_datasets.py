# dags/utils/polygon_datasets.py
"""
📦 polygon_datasets.py

Centralized dataset registry for Polygon.io-related data assets.
These Airflow Datasets act as dependency markers for cross-DAG triggering,
data lineage tracking, and observability in the Airflow UI.

Each Dataset represents a key asset updated or consumed by one or more DAGs:
  • S3 manifests (produced by ingest DAGs)
  • Snowflake RAW tables (produced by load DAGs; consumed by dbt)
"""

from __future__ import annotations
import os
from airflow.datasets import Dataset

# ──────────────────────────────────────────────────────────────────────────────
# Env-aware configuration (kept lightweight; mirrors DAG defaults)
# ──────────────────────────────────────────────────────────────────────────────
# Bucket used by all ingest DAGs
BUCKET_NAME = os.getenv("BUCKET_NAME", "stock-market-elt")

# Snowflake identifiers (purely for Dataset URIs; loaders still use Connection extras)
SNOWFLAKE_DB = os.getenv("SNOWFLAKE_DB", "STOCKS_ELT_DB")
SNOWFLAKE_RAW_SCHEMA = os.getenv("SNOWFLAKE_RAW_SCHEMA", "RAW")

# RAW table names (match loader defaults)
SNOWFLAKE_STOCKS_TABLE = os.getenv("SNOWFLAKE_STOCKS_TABLE", "source_polygon_stocks_raw")
SNOWFLAKE_OPTIONS_TABLE = os.getenv("SNOWFLAKE_OPTIONS_TABLE", "source_polygon_options_raw")

# Pointer manifest keys (match ingest daily DAGs)
STOCKS_LATEST_POINTER_KEY = os.getenv(
    "STOCKS_MANIFEST_KEY",
    "raw/manifests/polygon_stocks_manifest_latest.txt",
)
OPTIONS_LATEST_POINTER_KEY = os.getenv(
    "OPTIONS_LATEST_POINTER_KEY",
    "raw/manifests/polygon_options_manifest_latest.txt",
)

# ──────────────────────────────────────────────────────────────────────────────
# 🪣 S3 Datasets (Manifests)
# ──────────────────────────────────────────────────────────────────────────────
# These are *pointer files* whose content is:
#   POINTER=raw/manifests/<asset>/<YYYY-MM-DD>/manifest.txt
# Daily ingest DAGs atomically update the pointer and then emit the Dataset.
S3_STOCKS_MANIFEST_DATASET = Dataset(
    f"s3://{BUCKET_NAME}/{STOCKS_LATEST_POINTER_KEY}"
)

S3_OPTIONS_MANIFEST_DATASET = Dataset(
    f"s3://{BUCKET_NAME}/{OPTIONS_LATEST_POINTER_KEY}"
)

# ──────────────────────────────────────────────────────────────────────────────
# ❄️ Snowflake Datasets (RAW Tables)
# ──────────────────────────────────────────────────────────────────────────────
# Emitted by the LOAD DAGs after a successful COPY INTO (append-only RAW).
# dbt jobs can subscribe to these to kick off incremental models.
SNOWFLAKE_STOCKS_RAW_DATASET = Dataset(
    f"snowflake://{SNOWFLAKE_DB}.{SNOWFLAKE_RAW_SCHEMA}.{SNOWFLAKE_STOCKS_TABLE}"
)

SNOWFLAKE_OPTIONS_RAW_DATASET = Dataset(
    f"snowflake://{SNOWFLAKE_DB}.{SNOWFLAKE_RAW_SCHEMA}.{SNOWFLAKE_OPTIONS_TABLE}"
)
