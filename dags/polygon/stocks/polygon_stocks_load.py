from __future__ import annotations
import pendulum
import os
from datetime import timedelta

from airflow.decorators import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.exceptions import AirflowSkipException

from dags.utils.polygon_datasets import S3_MANIFEST_DATASET
from airflow.datasets import Dataset
SNOWFLAKE_DWH_RAW_DATASET = Dataset("snowflake://stocks_elt_db/public/source_polygon_stock_bars_daily")


@dag(
    dag_id="polygon_stocks_load",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    schedule=[S3_MANIFEST_DATASET],
    catchup=False,
    tags=["load", "polygon", "snowflake"],
    dagrun_timeout=timedelta(hours=2),
)
def polygon_stocks_load_dag():
    S3_CONN_ID = "aws_default"
    SNOWFLAKE_CONN_ID = "snowflake_default"
    BUCKET_NAME = os.getenv("BUCKET_NAME")
    
    SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE")
    SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA")
    SNOWFLAKE_TABLE = "source_polygon_stock_bars_daily"
    FULLY_QUALIFIED_TABLE_NAME = f"{SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{SNOWFLAKE_TABLE}"
    FULLY_QUALIFIED_STAGE_NAME = f"@{SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.s3_stage"

    @task
    def create_snowflake_table():
        snowflake_hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {FULLY_QUALIFIED_TABLE_NAME} (
            ticker TEXT, trade_date DATE, open NUMERIC(19, 4), high NUMERIC(19, 4),
            low NUMERIC(19, 4), close NUMERIC(19, 4), volume BIGINT, vwap NUMERIC(19, 4),
            transactions BIGINT, inserted_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP()
        );
        """
        snowflake_hook.run(create_table_sql)

    @task
    def get_s3_keys_from_manifest() -> list[str]:
        s3_hook = S3Hook(aws_conn_id=S3_CONN_ID)
        manifest_key = "manifests/manifest_latest.txt"
        if not s3_hook.check_for_key(manifest_key, bucket_name=BUCKET_NAME):
            raise FileNotFoundError(f"Manifest file not found: {manifest_key}")
        manifest_content = s3_hook.read_key(key=manifest_key, bucket_name=BUCKET_NAME)
        s3_keys = [key for key in manifest_content.strip().splitlines() if key]
        if not s3_keys:
            raise AirflowSkipException("Manifest is empty. No new files to process.")
        return s3_keys

    @task(outlets=[SNOWFLAKE_DWH_RAW_DATASET])
    def load_data_to_snowflake(s3_keys: list[str]):
        snowflake_hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        copy_sql = f"""
        COPY INTO {FULLY_QUALIFIED_TABLE_NAME} (ticker, trade_date, volume, vwap, open, close, high, low, transactions)
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
        FILES = ({', '.join(f"'{key}'" for key in s3_keys)})
        FILE_FORMAT = (TYPE = 'JSON');
        """
        snowflake_hook.run(copy_sql)
        print(f"Successfully loaded data from {len(s3_keys)} files into {FULLY_QUALIFIED_TABLE_NAME}.")

    table_created = create_snowflake_table()
    s3_keys = get_s3_keys_from_manifest()
    
    load_op = load_data_to_snowflake(s3_keys)
    table_created >> load_op

polygon_stocks_load_dag()
