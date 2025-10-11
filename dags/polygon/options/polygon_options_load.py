from __future__ import annotations
import os
from datetime import timedelta
from typing import List

import pendulum
from airflow.decorators import dag, task
from airflow.exceptions import AirflowSkipException
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from dags.utils.polygon_datasets import (
    S3_OPTIONS_MANIFEST_DATASET,
    SNOWFLAKE_OPTIONS_RAW_DATASET,
)

# -----------------------------
# Config
# -----------------------------
S3_CONN_ID = os.getenv("S3_CONN_ID", "aws_default")  # align with ingest
SNOWFLAKE_CONN_ID = os.getenv("SNOWFLAKE_CONN_ID", "snowflake_default")
BUCKET_NAME = os.getenv("BUCKET_NAME", "test")

SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA")
SNOWFLAKE_TABLE = os.getenv("SNOWFLAKE_OPTIONS_TABLE", "source_polygon_options_bars_daily")
FULLY_QUALIFIED_TABLE_NAME = f"{SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{SNOWFLAKE_TABLE}"

# Use fully-qualified stage created in your guide
SNOWFLAKE_STAGE = os.getenv("SNOWFLAKE_STAGE", "STOCKS_ELT_DB.PUBLIC.s3_stage")

# Batch size for FILES=() COPY (avoid overly-long SQL statements)
COPY_BATCH_SIZE = int(os.getenv("SNOWFLAKE_COPY_BATCH_SIZE", "1000"))


@dag(
    dag_id="polygon_options_load",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule=[S3_OPTIONS_MANIFEST_DATASET],  # triggered by manifest dataset update
    catchup=False,
    tags=["load", "polygon", "options", "snowflake"],
    dagrun_timeout=timedelta(hours=4),
)
def polygon_options_load_dag():
    """
    Loads raw JSON options data from S3 (written by ingest DAG) into Snowflake using COPY INTO.
    Triggered by the options manifest dataset.
    """

    @task
    def create_snowflake_table():
        """Creates the target options table in Snowflake if it doesn't exist."""
        ddl = f"""
        CREATE TABLE IF NOT EXISTS {FULLY_QUALIFIED_TABLE_NAME} (
            option_symbol TEXT,
            trade_date DATE,
            underlying_ticker TEXT,
            expiration_date DATE,
            strike_price NUMERIC(19, 4),
            option_type VARCHAR(4),
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
        SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID).run(ddl)

    @task
    def get_s3_keys_from_manifest() -> List[str]:
        """
        Reads the options manifest file from S3 to get the list of JSON files produced by the ingest DAG.
        """
        s3 = S3Hook(aws_conn_id=S3_CONN_ID)
        manifest_key = "manifests/polygon_options_manifest_latest.txt"
        if not s3.check_for_key(manifest_key, bucket_name=BUCKET_NAME):
            raise AirflowSkipException(f"Manifest not found at s3://{BUCKET_NAME}/{manifest_key}")

        manifest_content = s3.read_key(key=manifest_key, bucket_name=BUCKET_NAME)
        s3_keys = [k.strip() for k in manifest_content.splitlines() if k.strip()]

        if not s3_keys:
            raise AirflowSkipException("Manifest is empty. No new files to process.")

        return s3_keys

    @task(outlets=[SNOWFLAKE_OPTIONS_RAW_DATASET])
    def load_data_to_snowflake(s3_keys: List[str]):
        """
        COPY INTO the target table from the Snowflake stage, limiting FILES per statement for stability.
        Assumes the stage points at the root of the bucket used by the ingest DAG.
        """
        if not s3_keys:
            raise AirflowSkipException("No S3 keys provided to loader.")

        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)

        # Build the SELECT that maps Polygon JSON to columns.
        # Note: FILES=() will restrict the stage scan to the batch we pass.
        base_copy_sql = f"""
        COPY INTO {FULLY_QUALIFIED_TABLE_NAME} (
            option_symbol, trade_date, underlying_ticker, expiration_date, strike_price, option_type,
            open, high, low, close, volume, vwap, transactions
        )
        FROM (
            SELECT
                $1:ticker::TEXT                                                     AS option_symbol,
                TO_DATE(REGEXP_SUBSTR(METADATA$FILENAME, '([0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}})')) AS trade_date,
                REGEXP_SUBSTR($1:ticker, '^O:([A-Z\\.]+)', 1, 1, 'e', 1)            AS underlying_ticker,
                TO_DATE(REGEXP_SUBSTR($1:ticker, '(\\d{{6}})', 1, 1, 'e'), 'YYMMDD') AS expiration_date,
                (REGEXP_SUBSTR($1:ticker, '(\\d{{8}}$)', 1, 1, 'e'))::NUMBER / 1000  AS strike_price,
                IFF(REGEXP_SUBSTR($1:ticker, '([CP])', 1, 1, 'e') = 'C', 'call', 'put') AS option_type,
                $1:results[0]:o::FLOAT AS open,
                $1:results[0]:h::FLOAT AS high,
                $1:results[0]:l::FLOAT AS low,
                $1:results[0]:c::FLOAT AS close,
                $1:results[0]:v::BIGINT AS volume,
                $1:results[0]:vw::FLOAT AS vwap,
                $1:results[0]:n::BIGINT AS transactions
            FROM @{SNOWFLAKE_STAGE}
        )
        FILE_FORMAT = (TYPE = 'JSON')
        """

        # Chunk the FILES list so statements stay reasonable in size
        total = len(s3_keys)
        loaded = 0
        for i in range(0, total, COPY_BATCH_SIZE):
            batch = s3_keys[i : i + COPY_BATCH_SIZE]
            files_clause = ", ".join(f"'{k}'" for k in batch)

            copy_sql = base_copy_sql + f"\nFILES = ({files_clause});"
            hook.run(copy_sql)
            loaded += len(batch)

        print(f"Successfully loaded {loaded} files into {FULLY_QUALIFIED_TABLE_NAME}.")

    table_created = create_snowflake_table()
    s3_keys = get_s3_keys_from_manifest()
    load_op = load_data_to_snowflake(s3_keys)

    table_created >> load_op


polygon_options_load_dag()
