from __future__ import annotations
import os
from datetime import timedelta
from typing import List

import pendulum
from airflow.decorators import dag, task
from airflow.exceptions import AirflowSkipException
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.hooks.base import BaseHook  # resolve Snowflake extras

from dags.utils.polygon_datasets import (
    S3_OPTIONS_MANIFEST_DATASET,
    SNOWFLAKE_OPTIONS_RAW_DATASET,
)

# -----------------------------
# Config
# -----------------------------
SNOWFLAKE_CONN_ID = os.getenv("SNOWFLAKE_CONN_ID", "snowflake_default")
BUCKET_NAME = os.getenv("BUCKET_NAME", "test")
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
    Loads raw JSON options data from S3 (written by ingest DAG) into Snowflake:
      1) COPY raw JSON into a VARIANT staging table (no transformation in COPY)
      2) INSERT-SELECT parse into target table
      3) TRUNCATE the staging table

    S3 paths produced by ingest:
      - s3://<bucket>/raw/options/<symbol>/<YYYY-MM-DD>.json
      - s3://<bucket>/raw/manifests/polygon_options_manifest_latest.txt
    """

    # --- Resolve Snowflake context from connection extras ---
    conn = BaseHook.get_connection(SNOWFLAKE_CONN_ID)
    x = conn.extra_dejson or {}

    SF_DB = x.get("database")
    SF_SCHEMA = x.get("schema")
    if not SF_DB or not SF_SCHEMA:
        raise ValueError(
            "Snowflake connection extras must include 'database' and 'schema'. "
            "Edit secret 'airflow/connections/snowflake_default' to include these in extra."
        )

    # Optional overrides
    STAGE_NAME = x.get("stage", "s3_stage")  # same stage used for stocks/options
    OPTIONS_TABLE = x.get("options_table", "source_polygon_options_bars_daily")
    OPTIONS_STAGE_TABLE = x.get("options_stage_table", "stg_polygon_options_raw")

    FQ_TABLE = f"{SF_DB}.{SF_SCHEMA}.{OPTIONS_TABLE}"
    FQ_STAGE_TABLE = f"{SF_DB}.{SF_SCHEMA}.{OPTIONS_STAGE_TABLE}"
    FQ_STAGE = f"@{SF_DB}.{SF_SCHEMA}.{STAGE_NAME}"

    @task
    def create_tables():
        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)

        ddl_target = f"""
        CREATE TABLE IF NOT EXISTS {FQ_TABLE} (
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

        ddl_stage = f"""
        CREATE TABLE IF NOT EXISTS {FQ_STAGE_TABLE} ( rec VARIANT );
        """

        hook.run(ddl_target)
        hook.run(ddl_stage)

    @task
    def get_s3_keys_from_manifest() -> List[str]:
        """
        Read manifest and convert absolute S3 keys (starting with 'raw/') to
        stage-relative paths for a stage rooted at s3://.../raw/.
        """
        s3 = S3Hook()
        manifest_key = "raw/manifests/polygon_options_manifest_latest.txt"
        if not s3.check_for_key(manifest_key, bucket_name=BUCKET_NAME):
            raise AirflowSkipException(f"Manifest not found at s3://{BUCKET_NAME}/{manifest_key}")

        manifest_content = s3.read_key(key=manifest_key, bucket_name=BUCKET_NAME) or ""
        raw_keys = [k.strip() for k in manifest_content.splitlines() if k.strip()]
        if not raw_keys:
            raise AirflowSkipException("Manifest is empty. No new files to process.")

        # Convert to stage-relative (strip leading 'raw/')
        rel_keys = [k[4:] if k.startswith("raw/") else k for k in raw_keys]

        # Keep only options files (avoid accidental stocks loads)
        rel_keys = [k for k in rel_keys if k.startswith("options/")]

        if not rel_keys:
            raise AirflowSkipException("No eligible options files found in manifest after normalization.")
        return rel_keys

    @task
    def copy_raw_into_staging(stage_relative_keys: List[str]) -> int:
        if not stage_relative_keys:
            raise AirflowSkipException("No S3 keys provided to loader.")

        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)

        # Clean slate per run (optional but tidy)
        hook.run(f"TRUNCATE TABLE {FQ_STAGE_TABLE};")

        total = len(stage_relative_keys)
        loaded = 0
        for i in range(0, total, COPY_BATCH_SIZE):
            batch = stage_relative_keys[i : i + COPY_BATCH_SIZE]
            files_clause = ", ".join(f"'{k}'" for k in batch)

            copy_sql = f"""
            COPY INTO {FQ_STAGE_TABLE} (rec)
            FROM {FQ_STAGE}
            FILES = ({files_clause})
            FILE_FORMAT = (TYPE = 'JSON')
            ON_ERROR = 'ABORT_STATEMENT'
            FORCE = FALSE;
            """
            hook.run(copy_sql)
            loaded += len(batch)

        return loaded

    @task(outlets=[SNOWFLAKE_OPTIONS_RAW_DATASET])
    def insert_from_staging_to_target(_rows_loaded_to_stage: int) -> int:
        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)

        insert_sql = f"""
        INSERT INTO {FQ_TABLE} (
            option_symbol, trade_date, underlying_ticker, expiration_date, strike_price, option_type,
            open, high, low, close, volume, vwap, transactions
        )
        SELECT
            sym                                                                 AS option_symbol,
            TO_DATE(TO_TIMESTAMP_NTZ( (rec:results[0]:t)::NUMBER / 1000 ))      AS trade_date,
            REGEXP_SUBSTR(sym, '^O:([A-Z0-9\\.\\-]+)\\d{{6}}[CP]\\d{{8}}', 1, 1, 'c', 1)  AS underlying_ticker,
            TO_DATE(REGEXP_SUBSTR(sym, '^O:[A-Z0-9\\.\\-]+(\\d{{6}})[CP]\\d{{8}}', 1, 1, 'c', 1), 'YYMMDD') AS expiration_date,
            TRY_TO_NUMBER(REGEXP_SUBSTR(sym, '(\\d{{8}})$', 1, 1, 'c', 1)) / 1000 AS strike_price,
            IFF(REGEXP_SUBSTR(sym, '^O:[A-Z0-9\\.\\-]+\\d{{6}}([CP])\\d{{8}}', 1, 1, 'c', 1) = 'C', 'call', 'put') AS option_type,
            rec:results[0]:o::FLOAT AS open,
            rec:results[0]:h::FLOAT AS high,
            rec:results[0]:l::FLOAT AS low,
            rec:results[0]:c::FLOAT AS close,
            rec:results[0]:v::BIGINT AS volume,
            rec:results[0]:vw::FLOAT AS vwap,
            rec:results[0]:n::BIGINT AS transactions
        FROM (
            SELECT rec, TO_VARCHAR(rec:ticker) AS sym
            FROM {FQ_STAGE_TABLE}
        );
        """
        hook.run(insert_sql)

        cnt = hook.get_first(f"SELECT COUNT(*) FROM {FQ_STAGE_TABLE}")[0] or 0
        return int(cnt)

    @task
    def cleanup_staging(_inserted: int) -> None:
        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        hook.run(f"TRUNCATE TABLE {FQ_STAGE_TABLE};")

    # Flow
    _ = create_tables()
    rel_keys = get_s3_keys_from_manifest()
    rows_to_stage = copy_raw_into_staging(rel_keys)
    inserted = insert_from_staging_to_target(rows_to_stage)
    cleanup_staging(inserted)

polygon_options_load_dag()
