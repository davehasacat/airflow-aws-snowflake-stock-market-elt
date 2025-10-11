import os
from airflow.datasets import Dataset

# Get the S3 bucket name from the environment variables to ensure consistency
BUCKET_NAME = os.getenv("BUCKET_NAME", "your-s3-bucket-name")

# --- STOCKS DATASETS ---

# Dataset for the S3 manifest file created by the stocks ingest DAG
S3_STOCKS_MANIFEST_DATASET = Dataset(f"s3://{BUCKET_NAME}/manifests/manifest_latest.txt")

# Dataset for the raw stocks table in Snowflake, updated by the stocks load DAG
SNOWFLAKE_STOCKS_RAW_DATASET = Dataset("snowflake://stocks_elt_db/public/source_polygon_stock_bars_daily")


# --- OPTIONS DATASETS ---

# Dataset for the S3 manifest file created by the options ingest DAG
S3_OPTIONS_MANIFEST_DATASET = Dataset(f"s3://{BUCKET_NAME}/manifests/polygon_options_manifest_latest.txt")

# Dataset for the raw options table in Snowflake, updated by the options load DAG
SNOWFLAKE_OPTIONS_RAW_DATASET = Dataset("snowflake://stocks_elt_db/public/source_polygon_options_bars_daily")
