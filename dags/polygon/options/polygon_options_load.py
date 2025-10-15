# dags/polygon/options/polygon_options_load.py
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
    Load raw Polygon options bars from S3 → Snowflake.
    Raw-in/raw-out, plus selected projections of results[0] fields.

    Flow:
      1) CREATE SCHEMA/TABLES IF NOT EXISTS
      2) Read latest manifest
      3) COPY raw JSON/.json.gz files from external stage into VARIANT staging table
      4) INSERT into landing table:
           - option_symbol (from rec:ticker)
           - trade_date (from results[0].t)
           - bar_ts (TIMESTAMP from results[0].t)
           - open, high, low, close, volume, vwap, transactions (from results[0])
           - raw_rec (entire JSON)
      5) TRUNCATE staging
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

    # External stage & table names (can be overridden via connection extras)
    STAGE_NAME = x.get("stage", "s3_stage")  # external stage rooted at s3://<bucket>/raw/
    OPTIONS_TABLE = x.get("options_table", "source_polygon_options_raw")  # landing/raw table
    OPTIONS_STAGE_TABLE = x.get("options_stage_table", "polygon_options_raw_staging")  # VARIANT staging table

    FQ_TABLE = f"{SF_DB}.{SF_SCHEMA}.{OPTIONS_TABLE}"
    FQ_STAGE_TABLE = f"{SF_DB}.{SF_SCHEMA}.{OPTIONS_STAGE_TABLE}"
    FQ_STAGE = f"@{SF_DB}.{SF_SCHEMA}.{STAGE_NAME}"

    # -----------------------------
    # Tasks
    # -----------------------------
    @task
    def create_tables() -> None:
        """
        Ensure schema & tables exist:
          - CREATE SCHEMA IF NOT EXISTS <DB>.<SCHEMA>
          - CREATE TABLE IF NOT EXISTS <landing> with raw + projected result fields
          - CREATE TABLE IF NOT EXISTS <staging> (rec VARIANT)
        """
        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)

        # 1) Schema (in case it was dropped)
        hook.run(f"CREATE SCHEMA IF NOT EXISTS {SF_DB}.{SF_SCHEMA};")

        # 2) Landing (raw) table with projected columns
        ddl_target = f"""
        CREATE TABLE IF NOT EXISTS {FQ_TABLE} (
            option_symbol   TEXT,
            trade_date      DATE,
            bar_ts          TIMESTAMP_NTZ,         -- from results[0].t (ms → seconds)
            open            FLOAT,
            high            FLOAT,
            low             FLOAT,
            close           FLOAT,
            volume          BIGINT,
            vwap            FLOAT,
            transactions    BIGINT,
            raw_rec         VARIANT,               -- full JSON payload
            inserted_at     TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP()
        );
        """
        hook.run(ddl_target)

        # 3) Staging table
        ddl_stage = f"CREATE TABLE IF NOT EXISTS {FQ_STAGE_TABLE} ( rec VARIANT );"
        hook.run(ddl_stage)

    @task
    def get_s3_keys_from_manifest() -> List[str]:
        """
        Read manifest and convert absolute S3 keys (starting with 'raw/') to
        stage-relative paths for a stage rooted at s3://.../raw/.
        Filter to options files only.
        """
        s3 = S3Hook()  # rely on default AWS creds chain
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
        """
        COPY the specified files from the external stage into the VARIANT staging table.
        Handles .json and .json.gz (COMPRESSION=AUTO).
        """
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
            FILE_FORMAT = (TYPE = 'JSON')  -- gzip auto-detected
            ON_ERROR = 'ABORT_STATEMENT'
            FORCE = FALSE;
            """
            hook.run(copy_sql)
            loaded += len(batch)

        return loaded

    @task(outlets=[SNOWFLAKE_OPTIONS_RAW_DATASET])
    def insert_from_staging_to_target(_rows_loaded_to_stage: int) -> int:
        """
        Insert raw payloads from staging into landing table (no regex or symbol parsing).
        Projections are from rec:results[0]; extract VARIANT -> STRING -> TRY_TO_NUMBER,
        and specify scale to preserve decimals before casting to FLOAT.
        """
        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)

        insert_sql = f"""
        INSERT INTO {FQ_TABLE} (
            option_symbol, trade_date, bar_ts,
            open, high, low, close, volume, vwap, transactions,
            raw_rec
        )
        SELECT
            TO_VARCHAR(rec:ticker) AS option_symbol,

            /* t is ms since epoch; extract as string -> to number -> timestamp */
            TO_DATE(
              TO_TIMESTAMP_NTZ( TRY_TO_NUMBER(rec:results[0]:t::STRING) / 1000 )
            )                                                                 AS trade_date,
            TO_TIMESTAMP_NTZ( TRY_TO_NUMBER(rec:results[0]:t::STRING) / 1000 ) AS bar_ts,

            /* Preserve decimal precision for numeric fields */
            TRY_TO_NUMBER(rec:results[0]:o::STRING, 38, 12)::FLOAT  AS open,
            TRY_TO_NUMBER(rec:results[0]:h::STRING, 38, 12)::FLOAT  AS high,
            TRY_TO_NUMBER(rec:results[0]:l::STRING, 38, 12)::FLOAT  AS low,
            TRY_TO_NUMBER(rec:results[0]:c::STRING, 38, 12)::FLOAT  AS close,
            TRY_TO_NUMBER(rec:results[0]:v::STRING)::BIGINT         AS volume,
            TRY_TO_NUMBER(rec:results[0]:vw::STRING, 38, 12)::FLOAT AS vwap,
            TRY_TO_NUMBER(rec:results[0]:n::STRING)::BIGINT         AS transactions,

            rec AS raw_rec
        FROM {FQ_STAGE_TABLE}
        WHERE rec:ticker IS NOT NULL
          AND ARRAY_SIZE(rec:results) > 0
          AND rec:results[0]:t IS NOT NULL;
        """
        hook.run(insert_sql)

        cnt = hook.get_first(f"SELECT COUNT(*) FROM {FQ_STAGE_TABLE}")[0] or 0
        return int(cnt)

    @task
    def cleanup_staging(_inserted: int) -> None:
        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        hook.run(f"TRUNCATE TABLE {FQ_STAGE_TABLE};")

    # -----------------------------
    # Wiring (explicit dependencies)
    # -----------------------------
    t_create = create_tables()
    rel_keys = get_s3_keys_from_manifest()
    rows_to_stage = copy_raw_into_staging(rel_keys)
    inserted = insert_from_staging_to_target(rows_to_stage)
    cleanup = cleanup_staging(inserted)

    # Ensure create happens before the rest
    t_create >> rel_keys
    t_create >> rows_to_stage
    t_create >> inserted
    t_create >> cleanup

polygon_options_load_dag()
