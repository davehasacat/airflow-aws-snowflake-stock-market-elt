"""
📦 polygon_datasets.py

Centralized dataset registry for Polygon.io-related data assets.
These Airflow Datasets act as dependency markers for cross-DAG triggering,
data lineage tracking, and observability in the Airflow UI.

Each Dataset represents a key asset updated or consumed by one or more DAGs:
  - S3 manifests (produced by ingest DAGs)
  - Snowflake raw tables (loaded via load DAGs)
"""

import os
from airflow.datasets import Dataset

# We *optionally* peek at Snowflake connection extras to build accurate table URIs.
# If the connection/extras are not available at import time, we fall back to sensible defaults.
try:
    from airflow.hooks.base import BaseHook  # safe import; guarded in try/except
except Exception:  # pragma: no cover
    BaseHook = None  # type: ignore


# ──────────────────────────────────────────────────────────────────────────────
# Helper: resolve Snowflake context from conn extras (with safe fallbacks)
# ──────────────────────────────────────────────────────────────────────────────
def _sf_ctx():
    conn_id = os.getenv("SNOWFLAKE_CONN_ID", "snowflake_default")
    db = "STOCKS_ELT_DB"
    raw_schema = "RAW"
    stocks_tbl = "SOURCE_POLYGON_STOCKS_RAW"
    options_tbl = "SOURCE_POLYGON_OPTIONS_RAW"

    if BaseHook is not None:
        try:
            conn = BaseHook.get_connection(conn_id)
            x = (conn.extra_dejson or {})
            db = x.get("database", db)
            raw_schema = x.get("raw_schema", x.get("schema", raw_schema))
            stocks_tbl = x.get("stocks_table", stocks_tbl)
            options_tbl = x.get("options_table", options_tbl)
        except Exception:
            pass

    # Build canonical, case-preserving identifiers for Dataset URIs
    return {
        "DB": db,
        "RAW": raw_schema,
        "STOCKS_TBL": stocks_tbl,
        "OPTIONS_TBL": options_tbl,
    }


_sf = _sf_ctx()

# ──────────────────────────────────────────────────────────────────────────────
# 🪣 S3 Datasets (Manifests)
# ──────────────────────────────────────────────────────────────────────────────
BUCKET_NAME = os.getenv("BUCKET_NAME", "stock-market-elt")

# Latest-pointer files (POINTER=<per-day manifest key>)
STOCKS_LATEST_POINTER_KEY  = os.getenv(
    "STOCKS_LATEST_POINTER_KEY", "raw/manifests/polygon_stocks_manifest_latest.txt"
)
OPTIONS_LATEST_POINTER_KEY = os.getenv(
    "OPTIONS_LATEST_POINTER_KEY", "raw/manifests/polygon_options_manifest_latest.txt"
)

# Dataset representing the *latest stocks manifest* in S3.
# Produced by: polygon_stocks_ingest_daily
# Consumed by: polygon_stocks_load_stream (Dataset-driven trigger)
S3_STOCKS_MANIFEST_DATASET = Dataset(
    f"s3://{BUCKET_NAME}/{STOCKS_LATEST_POINTER_KEY}"
)

# Dataset representing the *latest options manifest* in S3.
# Produced by: polygon_options_ingest_daily
# Consumed by: polygon_options_load_stream (Dataset-driven trigger)
S3_OPTIONS_MANIFEST_DATASET = Dataset(
    f"s3://{BUCKET_NAME}/{OPTIONS_LATEST_POINTER_KEY}"
)

# ──────────────────────────────────────────────────────────────────────────────
# ❄️ Snowflake Datasets (Raw Tables)
# ──────────────────────────────────────────────────────────────────────────────
# These mark completion of raw loads and can be depended on by dbt build DAGs.

# Stocks raw bars table
SNOWFLAKE_STOCKS_RAW_DATASET = Dataset(
    f"snowflake://{_sf['DB']}/{_sf['RAW']}/{_sf['STOCKS_TBL']}"
)

# Options raw bars table
SNOWFLAKE_OPTIONS_RAW_DATASET = Dataset(
    f"snowflake://{_sf['DB']}/{_sf['RAW']}/{_sf['OPTIONS_TBL']}"
)
