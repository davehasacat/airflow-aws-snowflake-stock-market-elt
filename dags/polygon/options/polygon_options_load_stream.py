# dags/polygon/options/polygon_options_load_stream.py
# =============================================================================
# Polygon Options → Snowflake (Load Stream)
# -----------------------------------------------------------------------------
# Stream loader subscribed to the *POINTER* manifest dataset produced by the
# daily ingest DAG. It dereferences the pointer to an immutable per-day manifest,
# loads the listed JSON(.gz) files from S3 external stage into a typed RAW table,
# and emits a Snowflake dataset on success for downstream dbt jobs.
#
# Enterprise/modern behavior:
#   • Subscribes to S3_OPTIONS_MANIFEST_DATASET (POINTER file)
#   • Pointer format: a single line →  "POINTER=raw/manifests/options/YYYY-MM-DD/manifest.txt"
#   • Transformed COPY SELECT projects JSON to typed columns + lineage
#   • Idempotent via Snowflake load history (FORCE=FALSE) and optional prefilter
#   • Small, deterministic COPY batches to stabilize runtime
#   • Optional probe validation step (VALIDATION_MODE on a temp table)
#   • All S3 object paths are stage-relative (strip 'raw/' prefix)
#   • Emits SNOWFLAKE_OPTIONS_RAW_DATASET for dbt incremental models
#   • Pools to control concurrency: load_pool (configurable)
# =============================================================================

from __future__ import annotations
import os
from datetime import timedelta
from typing import List

import pendulum

from airflow.decorators import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.exceptions import AirflowSkipException
from airflow.hooks.base import BaseHook

from dags.utils.polygon_datasets import (
    S3_OPTIONS_MANIFEST_DATASET,
    SNOWFLAKE_OPTIONS_RAW_DATASET,
)

# ────────────────────────────────────────────────────────────────────────────────
# Config (env-first; sensible defaults)
# ────────────────────────────────────────────────────────────────────────────────
BUCKET_NAME = os.getenv("BUCKET_NAME")  # expected: stock-market-elt
SNOWFLAKE_CONN_ID = os.getenv("SNOWFLAKE_CONN_ID", "snowflake_default")

# Manifest pointer location (dataset must point here). Loader reads the file content.
LATEST_POINTER_KEY = os.getenv(
    "OPTIONS_LATEST_POINTER_KEY", "raw/manifests/polygon_options_manifest_latest.txt"
)

# COPY tuning
BATCH_SIZE = int(os.getenv("SNOWFLAKE_COPY_BATCH_SIZE", "500"))      # FILES list size per COPY
ON_ERROR = os.getenv("SNOWFLAKE_COPY_ON_ERROR", "ABORT_STATEMENT")   # or 'CONTINUE'
FORCE = os.getenv("SNOWFLAKE_COPY_FORCE", "FALSE").upper()           # TRUE to bypass load history
DO_VALIDATE = os.getenv("SNOWFLAKE_COPY_VALIDATE", "FALSE").upper() == "TRUE"
PREFILTER_LOADED = os.getenv("SNOWFLAKE_PREFILTER_LOADED", "TRUE").upper() == "TRUE"
COPY_HISTORY_LOOKBACK_HOURS = int(os.getenv("SNOWFLAKE_COPY_HISTORY_LOOKBACK_HOURS", "168"))  # 7d

# Optional: non-transform probe validation (fast-fail for JSON/compression/access)
DO_PROBE_VALIDATE = os.getenv("SNOWFLAKE_PROBE_VALIDATE", "FALSE").upper() == "TRUE"

# Pools
LOAD_POOL = os.getenv("LOAD_POOL", "load_pool")                  # general load orchestration
SNOWFLAKE_POOL = os.getenv("SNOWFLAKE_POOL", "snowflake_pool")   # Snowflake-heavy steps

default_args = {
    "owner": "data-platform",
    "retries": 2,
    "retry_delay": pendulum.duration(minutes=5),
    "pool": LOAD_POOL,
}

# ────────────────────────────────────────────────────────────────────────────────
# DAG
# ────────────────────────────────────────────────────────────────────────────────
@dag(
    dag_id="polygon_options_load_stream",
    description="Load stream for Polygon options: subscribe to POINTER manifest and COPY to Snowflake RAW.",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    schedule=[S3_OPTIONS_MANIFEST_DATASET],   # fires when the POINTER file is updated
    catchup=False,
    tags=["load", "polygon", "snowflake", "options", "stream"],
    dagrun_timeout=timedelta(hours=2),
    max_active_runs=1,
    default_args=default_args,
)
def polygon_options_load_stream_dag():

    # ────────────────────────────────────────────────────────────────────────────
    # Resolve Snowflake context (from Secrets-backed connection extras)
    # ────────────────────────────────────────────────────────────────────────────
    conn = BaseHook.get_connection(SNOWFLAKE_CONN_ID)
    x = conn.extra_dejson or {}

    SF_DB = x.get("database")
    RAW_SCHEMA = x.get("raw_schema", "RAW")                           # landing table schema
    STAGE_SCHEMA = x.get("stage_schema", x.get("schema", "STAGES"))   # external stage schema

    if not SF_DB:
        raise ValueError("Snowflake connection extras must include 'database'.")

    STAGE_NAME = x.get("stage", "s3_stage")                            # external stage name
    TABLE_NAME = x.get("options_table", "source_polygon_options_raw")  # landing table name

    # Table goes to RAW schema
    FQ_TABLE = f"{SF_DB}.{RAW_SCHEMA}.{TABLE_NAME}"
    # Stage stays in STAGES (or whatever you set in extras)
    FQ_STAGE = f"@{SF_DB}.{STAGE_SCHEMA}.{STAGE_NAME}"
    FQ_STAGE_NO_AT = f"{SF_DB}.{STAGE_SCHEMA}.{STAGE_NAME}"

    if not BUCKET_NAME:
        raise RuntimeError("BUCKET_NAME env var is required (e.g., 'stock-market-elt').")

    # ────────────────────────────────────────────────────────────────────────────
    # Tasks
    # ────────────────────────────────────────────────────────────────────────────
    @task(pool=SNOWFLAKE_POOL)
    def create_snowflake_table():
        """Create/ensure the typed landing table (lineage-friendly for dbt incremental)."""
        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        sql = f"""
        CREATE TABLE IF NOT EXISTS {FQ_TABLE} (
            -- Business columns
            option_symbol TEXT,
            polygon_trade_date DATE,
            polygon_bar_ts TIMESTAMP_NTZ,
            "open" NUMERIC(19, 4),
            high NUMERIC(19, 4),
            low NUMERIC(19, 4),
            "close" NUMERIC(19, 4),
            volume BIGINT,
            vwap NUMERIC(19, 4),
            transactions BIGINT,

            -- Lineage & audit (for dbt incremental)
            source_file TEXT,
            source_row_number BIGINT,
            inserted_at TIMESTAMP_NTZ DEFAULT (CURRENT_TIMESTAMP()::TIMESTAMP_NTZ)
        );
        """
        hook.run(sql)

    @task(pool=SNOWFLAKE_POOL)
    def check_stage_exists():
        """Ensure the external stage exists and is accessible for the Airflow role."""
        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        try:
            hook.run(f"DESC STAGE {FQ_STAGE_NO_AT}")
        except Exception as e:
            raise RuntimeError(
                f"Stage {FQ_STAGE_NO_AT} not found or not accessible. "
                f"Verify existence and privileges. Original error: {e}"
            )

    @task
    def read_pointer_and_manifest() -> List[str]:
        """
        1) Read the POINTER file content from S3 (Dataset producer writes this).
        2) Dereference to the immutable per-day manifest.
        3) Return ordered list of *stage-relative* object keys for COPY.
        """
        s3 = S3Hook()

        # 1) Read latest pointer file
        if not s3.check_for_key(LATEST_POINTER_KEY, bucket_name=BUCKET_NAME):
            raise AirflowSkipException(f"Latest pointer not found: s3://{BUCKET_NAME}/{LATEST_POINTER_KEY}")
        pointer = (s3.read_key(key=LATEST_POINTER_KEY, bucket_name=BUCKET_NAME) or "").strip()
        if not pointer.startswith("POINTER="):
            raise RuntimeError(f"Pointer file is malformed: {pointer[:120]}")
        manifest_key = pointer.split("=", 1)[1].strip()
        if not manifest_key:
            raise RuntimeError("Pointer file has empty target manifest path.")

        # 2) Read the immutable per-day manifest
        if not s3.check_for_key(manifest_key, bucket_name=BUCKET_NAME):
            raise AirflowSkipException(f"Per-day manifest not found: s3://{BUCKET_NAME}/{manifest_key}")
        manifest_body = s3.read_key(key=manifest_key, bucket_name=BUCKET_NAME) or ""
        lines = [ln.strip() for ln in manifest_body.splitlines() if ln.strip()]
        if not lines:
            raise AirflowSkipException(f"Per-day manifest is empty: s3://{BUCKET_NAME}/{manifest_key}")

        # 3) Normalize to stage-relative keys: strip leading 'raw/' and keep options/*
        rel = [(k[4:] if k.startswith("raw/") else k) for k in lines]
        rel = [k for k in rel if k.startswith("options/")]

        # De-dupe while preserving order
        seen = set()
        deduped: list[str] = []
        for k in rel:
            if k not in seen:
                seen.add(k)
                deduped.append(k)

        if not deduped:
            raise AirflowSkipException("No eligible options files in manifest after filtering.")

        print(f"Pointer → manifest: {manifest_key}")
        print(f"Files eligible (options/*): {len(deduped)}")
        return deduped

    @task(pool=SNOWFLAKE_POOL)
    def prefilter_already_loaded(keys: List[str]) -> List[str]:
        """
        Optional: prefilter keys that Snowflake already loaded into {FQ_TABLE}
        (based on information_schema.copy_history). Defaults on; disable with
        SNOWFLAKE_PREFILTER_LOADED=FALSE.
        """
        if not PREFILTER_LOADED or not keys:
            return keys

        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        lookback = COPY_HISTORY_LOOKBACK_HOURS
        sql = f"""
        with hist as (
          select FILE_NAME
          from table(information_schema.copy_history(
            table_name => '{FQ_TABLE}',
            start_time => dateadd('hour', -{lookback}, current_timestamp())
          ))
        )
        select FILE_NAME from hist;
        """
        rows = hook.get_records(sql) or []
        already = {r[0] for r in rows if r and r[0]}
        if not already:
            return keys

        remaining = [k for k in keys if k not in already]
        print(f"Prefiltered {len(keys) - len(remaining)} already-loaded files (lookback {lookback}h).")
        return remaining

    @task
    def chunk_keys(keys: List[str]) -> List[List[str]]:
        """Split keys into FILES=() batches to keep COPY statements predictable."""
        if not keys:
            raise AirflowSkipException("No files to load after prefiltering.")
        return [keys[i : i + BATCH_SIZE] for i in range(0, len(keys), BATCH_SIZE)]

    def _copy_sql(files: List[str]) -> str:
        """
        Compose COPY SQL using transformed SELECT.
        - projects JSON fields into typed columns
        - includes lineage via METADATA$FILENAME / METADATA$FILE_ROW_NUMBER
        - supports .json and .json.gz
        - relies on load history for idempotency (FORCE={FORCE})
        """
        files_clause = ", ".join(f"'{k}'" for k in files)
        return f"""
        COPY INTO {FQ_TABLE}
          (option_symbol, polygon_trade_date, polygon_bar_ts, volume, vwap, "open", "close", high, low, transactions, source_file, source_row_number)
        FROM (
            SELECT
                $1:ticker::TEXT                                                      AS option_symbol,
                /* Event ms → TIMESTAMP_NTZ → DATE (keep raw ts below) */
                TO_DATE(
                  TO_TIMESTAMP_NTZ(TRY_TO_NUMBER($1:results[0]:t::STRING) / 1000)
                )                                                                     AS polygon_trade_date,
                TO_TIMESTAMP_NTZ(TRY_TO_NUMBER($1:results[0]:t::STRING) / 1000)       AS polygon_bar_ts,

                TRY_TO_NUMBER($1:results[0]:v::STRING)::BIGINT                        AS volume,
                TRY_TO_NUMBER($1:results[0]:vw::STRING, 38, 12)::NUMERIC(19,4)        AS vwap,
                TRY_TO_NUMBER($1:results[0]:o::STRING, 38, 12)::NUMERIC(19,4)         AS "open",
                TRY_TO_NUMBER($1:results[0]:c::STRING, 38, 12)::NUMERIC(19,4)         AS "close",
                TRY_TO_NUMBER($1:results[0]:h::STRING, 38, 12)::NUMERIC(19,4)         AS high,
                TRY_TO_NUMBER($1:results[0]:l::STRING, 38, 12)::NUMERIC(19,4)         AS low,
                TRY_TO_NUMBER($1:results[0]:n::STRING)::BIGINT                        AS transactions,

                METADATA$FILENAME::TEXT                                               AS source_file,
                METADATA$FILE_ROW_NUMBER::BIGINT                                       AS source_row_number
            FROM {FQ_STAGE}
        )
        FILES = ({files_clause})
        FILE_FORMAT = (TYPE = 'JSON')
        ON_ERROR = '{ON_ERROR}'
        FORCE = {FORCE};
        """

    @task(retries=1, pool=SNOWFLAKE_POOL)
    def validate_batch_in_snowflake(batch: List[str]) -> int:
        """
        Validation step (transformed COPY) is a no-op: Snowflake doesn't support VALIDATION_MODE here.
        We keep this for interface symmetry and logging.
        """
        if not DO_VALIDATE or not batch:
            return 0
        print("Skipping transformed COPY validation: VALIDATION_MODE not supported with SELECT loads.")
        return 0

    @task(retries=1, pool=SNOWFLAKE_POOL)
    def probe_validate_batch(batch: List[str]) -> int:
        """
        Optional non-transform JSON validation: dry-run COPY that returns errors.
        Checks JSON/compression/access before the transformed load.
        Enabled when SNOWFLAKE_PROBE_VALIDATE=TRUE.
        """
        if not DO_PROBE_VALIDATE or not batch:
            return 0

        files_clause = ", ".join(f"'{k}'" for k in batch)
        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)

        # Ensure temp table exists (target for COPY; data won't be loaded due to VALIDATION_MODE)
        hook.run("CREATE TEMP TABLE IF NOT EXISTS TMP_JSON_VALIDATION (v VARIANT);")

        validate_sql = f"""
            COPY INTO TMP_JSON_VALIDATION
            FROM {FQ_STAGE}
            FILES = ({files_clause})
            FILE_FORMAT = (TYPE = 'JSON')
            VALIDATION_MODE = 'RETURN_ERRORS';
        """
        rows = hook.get_records(validate_sql) or []

        if rows:
            # Each row describes an error (row/file/error). Raise with first error for brevity.
            raise RuntimeError(f"Probe validation failed for {len(rows)} row(s); first error: {rows[0]}")
        print(f"Probe validated batch of {len(batch)} files (no errors).")
        return len(batch)

    @task(outlets=[SNOWFLAKE_OPTIONS_RAW_DATASET], retries=2, pool=SNOWFLAKE_POOL)
    def copy_batch_to_snowflake(batch: List[str]) -> int:
        """
        COPY a single batch using FILES=().
        - FORCE=FALSE + Snowflake load history → skip already ingested files.
        - ON_ERROR configurable (ABORT_STATEMENT or CONTINUE).
        """
        if not batch:
            return 0
        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        hook.run(_copy_sql(batch))
        print(f"Loaded batch of {len(batch)} files into {FQ_TABLE}.")
        return len(batch)

    @task
    def sum_loaded(counts: List[int]) -> int:
        """Visibility: how many files were targeted across COPY batches."""
        total = sum(counts or [])
        print(f"Total files targeted for load: {total}")
        return total

    # ────────────────────────────────────────────────────────────────────────────
    # Flow (dependency order + optional probe)
    # ────────────────────────────────────────────────────────────────────────────
    tbl = create_snowflake_table()
    stage_ok = check_stage_exists()
    manifest_keys = read_pointer_and_manifest()
    remaining = prefilter_already_loaded(manifest_keys)
    batches = chunk_keys(remaining)

    _ = validate_batch_in_snowflake.expand(batch=batches)  # no-op for transformed loads
    _ = probe_validate_batch.expand(batch=batches)         # gated by DO_PROBE_VALIDATE
    loaded_counts = copy_batch_to_snowflake.expand(batch=batches)
    _ = sum_loaded(loaded_counts)

    tbl >> stage_ok >> manifest_keys >> remaining >> batches >> loaded_counts

# Instantiate the DAG
polygon_options_load_stream_dag()
