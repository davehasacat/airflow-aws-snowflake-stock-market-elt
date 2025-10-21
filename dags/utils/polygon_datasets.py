"""
📦 polygon_datasets.py

Centralized dataset registry for Polygon.io-related data assets.
These Airflow Datasets act as dependency markers for cross-DAG triggering,
data lineage tracking, and observability in the Airflow UI.

Each Dataset represents a key asset updated or consumed by one or more DAGs:
  - S3 manifests (produced by ingest DAGs)
  - Snowflake raw tables (loaded via load DAGs)
"""

from airflow.datasets import Dataset

# ──────────────────────────────────────────────────────────────────────────────
# 🪣 S3 Datasets (Manifests)
# ──────────────────────────────────────────────────────────────────────────────

# Dataset representing the *latest stock manifest* in S3.
# This manifest file lists which trading-day files were ingested for stocks.
# Produced by: polygon_stocks_ingest_* DAGs
# Consumed by: downstream loaders or dbt build DAGs for freshness detection.
S3_STOCKS_MANIFEST_DATASET = Dataset(
    "s3://test/manifests/polygon_stocks_manifest_latest.txt"
)

# Dataset representing the *latest options manifest* in S3 (bars).
# Produced by: polygon_options_ingest_* DAGs
# Consumed by: downstream loaders and dbt models.
S3_OPTIONS_MANIFEST_DATASET = Dataset(
    "s3://test/manifests/polygon_options_manifest_latest.txt"
)

# ──────────────────────────────────────────────────────────────────────────────
# ❄️ Snowflake Datasets (Raw Tables)
# ──────────────────────────────────────────────────────────────────────────────

# Dataset representing the *raw stock bars table* in Snowflake.
# Produced by: stock load DAG after successful S3-to-Snowflake load.
# Consumed by: dbt models in the `staging` layer (e.g., stg_polygon__stocks_bars).
SNOWFLAKE_STOCKS_RAW_DATASET = Dataset(
    "snowflake://stocks_elt_db/public/source_polygon_stock_bars_daily"
)

# Dataset representing the *raw options bars table* in Snowflake.
# Produced by: options load DAG after successful S3-to-Snowflake load.
# Consumed by: dbt models in the `staging` layer (e.g., stg_polygon__options_bars).
SNOWFLAKE_OPTIONS_RAW_DATASET = Dataset(
    "snowflake://stocks_elt_db/public/source_polygon_options_bars_daily"
)
