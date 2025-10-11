from airflow.datasets import Dataset

# Dataset for the manifest file in S3 created by the stock ingest DAG
S3_STOCKS_MANIFEST_DATASET = Dataset("s3://test/manifests/polygon_stocks_manifest_latest.txt")

# Dataset for the manifest file in S3 created by the options ingest DAG
S3_OPTIONS_MANIFEST_DATASET = Dataset("s3://test/manifests/polygon_options_manifest_latest.txt")

# Dataset for the raw stocks table updated by the load DAG
SNOWFLAKE_STOCKS_RAW_DATASET = Dataset("snowflake://stocks_elt_db/public/source_polygon_stock_bars_daily")
