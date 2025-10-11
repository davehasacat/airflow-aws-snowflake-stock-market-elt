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

# Use fully-qualified stage created in your setup guide
SNOWFLAKE_STAGE = os.getenv("SNOWFLAKE_STAGE", "STOCKS_ELT_DB.PUBLIC.s3_stage")

# Raw staging table (variant)
SNOWFLAKE_STAGE_TABLE = os.getenv("SNOWFLAKE_OPTIONS_STAGE_TABLE", "stg_polygon_options_raw")
FULLY_QUALIFIED_STAGE_TABLE = f"{SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{SNOWFLAKE_STAGE_TABLE}"

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
    Loads raw JSON options data from S3 (written by ingest DAG) into Snowflake:
      1) COPY raw JSON into a VARIANT staging table (no transformation in COPY)
      2) INSERT-SELECT parse into target table
      3) TRUNCATE the staging table
    """

    @task
    def create_tables():
        """Create the target and staging tables if they don't exist."""
        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)

        ddl_target = f"""
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

        ddl_stage = f"""
        CREATE TABLE IF NOT EXISTS {FULLY_QUALIFIED_STAGE_TABLE} (
            rec VARIANT
        );
        """

        hook.run(ddl_target)
        hook.run(ddl_stage)

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

    @task
    def copy_raw_into_staging(s3_keys: List[str]) -> int:
        """
        COPY raw JSON into the VARIANT staging table.
        We avoid COPY transformations entirely here (no SELECT), which is robust.
        """
        if not s3_keys:
            raise AirflowSkipException("No S3 keys provided to loader.")

        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)

        # Ensure staging table is empty for this run (optional but clean)
        hook.run(f"TRUNCATE TABLE {FULLY_QUALIFIED_STAGE_TABLE};")

        total = len(s3_keys)
        loaded = 0
        for i in range(0, total, COPY_BATCH_SIZE):
            batch = s3_keys[i : i + COPY_BATCH_SIZE]
            files_clause = ", ".join(f"'{k}'" for k in batch)

            copy_sql = f"""
            COPY INTO {FULLY_QUALIFIED_STAGE_TABLE} (rec)
            FROM @{SNOWFLAKE_STAGE}
            FILES = ({files_clause})
            FILE_FORMAT = (TYPE = 'JSON');
            """
            hook.run(copy_sql)
            loaded += len(batch)

        return loaded

    @task(outlets=[SNOWFLAKE_OPTIONS_RAW_DATASET])
    def insert_from_staging_to_target(_rows_loaded_to_stage: int) -> int:
        """
        Parse from staging and insert into the target table.
        - trade_date is derived from rec:results[0]:t (ms epoch)
        - underlying/expiration/type/strike parsed from the symbol text
        """
        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)

        insert_sql = f"""
        INSERT INTO {FULLY_QUALIFIED_TABLE_NAME} (
            option_symbol, trade_date, underlying_ticker, expiration_date, strike_price, option_type,
            open, high, low, close, volume, vwap, transactions
        )
        SELECT
            sym                                                                 AS option_symbol,
            TO_DATE(TO_TIMESTAMP_NTZ(TRY_TO_NUMBER(rec:results[0]:t)::NUMBER / 1000))  AS trade_date,

            /* UNDERLYING immediately after 'O:' */
            REGEXP_SUBSTR(sym, '^O:([A-Z0-9\\.\\-]+)\\d{{6}}[CP]\\d{{8}}', 1, 1, 'c', 1)        AS underlying_ticker,

            /* EXPIRATION: YYMMDD right after underlying */
            TO_DATE(
              REGEXP_SUBSTR(sym, '^O:[A-Z0-9\\.\\-]+(\\d{{6}})[CP]\\d{{8}}', 1, 1, 'c', 1),
              'YYMMDD'
            )                                                                               AS expiration_date,

            /* STRIKE: last 8 digits, then /1000 */
            TRY_TO_NUMBER(REGEXP_SUBSTR(sym, '(\\d{{8}})$', 1, 1, 'c', 1)) / 1000            AS strike_price,

            /* TYPE: single C/P right after expiration */
            IFF(
              REGEXP_SUBSTR(sym, '^O:[A-Z0-9\\.\\-]+\\d{{6}}([CP])\\d{{8}}', 1, 1, 'c', 1) = 'C',
              'call',
              'put'
            )                                                                               AS option_type,

            rec:results[0]:o::FLOAT AS open,
            rec:results[0]:h::FLOAT AS high,
            rec:results[0]:l::FLOAT AS low,
            rec:results[0]:c::FLOAT AS close,
            rec:results[0]:v::BIGINT AS volume,
            rec:results[0]:vw::FLOAT AS vwap,
            rec:results[0]:n::BIGINT AS transactions
        FROM (
            SELECT
                rec,
                TO_VARCHAR(rec:ticker) AS sym
            FROM {FULLY_QUALIFIED_STAGE_TABLE}
        );
        """

        hook.run(insert_sql)

        # Optionally return how many staging rows we processed
        cnt = hook.get_first(f"SELECT COUNT(*) FROM {FULLY_QUALIFIED_STAGE_TABLE}")[0] or 0
        return int(cnt)

    @task
    def cleanup_staging(_inserted: int) -> None:
        """Truncate staging to keep it clean between runs."""
        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        hook.run(f"TRUNCATE TABLE {FULLY_QUALIFIED_STAGE_TABLE};")

    # ───────────────────────────────────────────────────────────────────────────
    # Flow
    # ───────────────────────────────────────────────────────────────────────────
    _ = create_tables()
    s3_keys = get_s3_keys_from_manifest()
    rows_to_stage = copy_raw_into_staging(s3_keys)
    inserted = insert_from_staging_to_target(rows_to_stage)
    cleanup_staging(inserted)


polygon_options_load_dag()
