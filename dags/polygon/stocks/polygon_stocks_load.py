from __future__ import annotations
import pendulum
from datetime import timedelta
import os

from airflow.decorators import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.exceptions import AirflowSkipException
from airflow.hooks.base import BaseHook  # read extras from conn

from dags.utils.polygon_datasets import S3_STOCKS_MANIFEST_DATASET, SNOWFLAKE_STOCKS_RAW_DATASET


@dag(
    dag_id="polygon_stocks_load",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    schedule=[S3_STOCKS_MANIFEST_DATASET],
    catchup=False,
    tags=["load", "polygon", "snowflake"],
    dagrun_timeout=timedelta(hours=2),
)
def polygon_stocks_load_dag():
    # Use default AWS credentials chain (no aws_conn_id)
    SNOWFLAKE_CONN_ID = "snowflake_default"
    BUCKET_NAME = os.getenv("BUCKET_NAME")  # expected: stock-market-elt

    # --- Resolve Snowflake context from the Airflow connection (Secrets-backed) ---
    conn = BaseHook.get_connection(SNOWFLAKE_CONN_ID)
    x = conn.extra_dejson or {}

    # Required
    SF_DB = x.get("database")
    SF_SCHEMA = x.get("schema")
    if not SF_DB or not SF_SCHEMA:
        raise ValueError(
            "Snowflake connection extras must include 'database' and 'schema'. "
            "Edit secret 'airflow/connections/snowflake_default' to include these in extra."
        )

    # Optional overrides from extras; otherwise use sane defaults
    STAGE_NAME = x.get("stage", "s3_stage")  # logical stage name in the same DB/schema
    TABLE_NAME = x.get("stocks_table", "source_polygon_stock_bars_daily")

    FULLY_QUALIFIED_TABLE_NAME = f"{SF_DB}.{SF_SCHEMA}.{TABLE_NAME}"
    FULLY_QUALIFIED_STAGE_NAME = f"@{SF_DB}.{SF_SCHEMA}.{STAGE_NAME}"

    @task
    def create_snowflake_table():
        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {FULLY_QUALIFIED_TABLE_NAME} (
            ticker TEXT,
            trade_date DATE,
            open NUMERIC(19, 4),
            high NUMERIC(19, 4),
            low NUMERIC(19, 4),
            close NUMERIC(19, 4),
            volume BIGINT,
            vwap NUMERIC(19, 4),
            transactions BIGINT,
            inserted_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP()
        );
        """
        hook.run(create_table_sql)

    @task
    def get_s3_keys_from_manifest() -> list[str]:
        """
        Fetch manifest and convert absolute keys (starting with 'raw/') to
        stage-relative paths for @.../raw/ (i.e., strip leading 'raw/').
        """
        s3 = S3Hook()
        manifest_key = "raw/manifests/manifest_latest.txt"
        if not s3.check_for_key(manifest_key, bucket_name=BUCKET_NAME):
            raise FileNotFoundError(f"Manifest file not found at s3://{BUCKET_NAME}/{manifest_key}")

        manifest_content = s3.read_key(key=manifest_key, bucket_name=BUCKET_NAME) or ""
        raw_keys = [k.strip() for k in manifest_content.splitlines() if k.strip()]

        if not raw_keys:
            raise AirflowSkipException("Manifest is empty. No new files to process.")

        # Convert to stage-relative paths (stage URL is s3://.../raw/)
        rel_keys = [k[4:] if k.startswith("raw/") else k for k in raw_keys]

        # Optional: ensure we're only loading stocks files
        rel_keys = [k for k in rel_keys if k.startswith("stocks/")]

        if not rel_keys:
            raise AirflowSkipException("No eligible stocks files found in manifest after normalization.")
        return rel_keys

    @task(outlets=[SNOWFLAKE_STOCKS_RAW_DATASET])
    def load_data_to_snowflake(stage_relative_keys: list[str]):
        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        files_clause = ", ".join(f"'{k}'" for k in stage_relative_keys)

        copy_sql = f"""
        COPY INTO {FULLY_QUALIFIED_TABLE_NAME}
          (ticker, trade_date, volume, vwap, open, close, high, low, transactions)
        FROM (
            SELECT
                $1:ticker::TEXT,
                TO_DATE(REGEXP_SUBSTR(METADATA$FILENAME, '([0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}})')),
                $1:results[0]:v::BIGINT,
                $1:results[0]:vw::FLOAT,
                $1:results[0]:o::FLOAT,
                $1:results[0]:c::FLOAT,
                $1:results[0]:h::FLOAT,
                $1:results[0]:l::FLOAT,
                $1:results[0]:n::BIGINT
            FROM {FULLY_QUALIFIED_STAGE_NAME}
        )
        FILES = ({files_clause})
        FILE_FORMAT = (TYPE = 'JSON')
        ON_ERROR = 'ABORT_STATEMENT'
        FORCE = FALSE;
        """
        hook.run(copy_sql)
        print(f"Loaded {len(stage_relative_keys)} files into {FULLY_QUALIFIED_TABLE_NAME}.")

    table_created = create_snowflake_table()
    rel_keys = get_s3_keys_from_manifest()
    load_op = load_data_to_snowflake(rel_keys)
    table_created >> load_op


polygon_stocks_load_dag()
