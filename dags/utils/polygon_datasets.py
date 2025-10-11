from airflow.datasets import Dataset

# Dataset for the manifest file in S3 created by the stock ingest DAG
S3_STOCKS_MANIFEST_DATASET = Dataset("s3://test/manifests/manifest_latest.txt")

# Dataset for the manifest file in S3 created by the options ingest DAG
S3_OPTIONS_MANIFEST_DATASET = Dataset("s3://test/manifests/polygon_options_manifest_latest.txt")
