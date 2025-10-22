# dags/polygon/stocks/polygon_stocks_load.py
# =============================================================================
# Polygon Stocks → Snowflake (Loader) — lineage-aware & dbt-incremental friendly
# -----------------------------------------------------------------------------
# Loads raw JSON(.gz) produced by the stocks ingest DAG from S3 (external stage)
# into a typed landing table (RAW schema). Supports flat and pointer manifests.
# Adds lineage columns for dbt incremental models.
# =============================================================================

from __future__ import annotations
import os
from datetime import timedelta

import pendulum

from airflow.decorators import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.exceptions import AirflowSkipException
from airflow.hooks.base import BaseHook

from dags.utils.polygon_datasets import (
    S3_STOCKS_MANIFEST_DATASET,
    SNOWFLAKE_STOCKS_RAW_DATASET,
)

# ────────────────────────────────────────────────────────────────────────────────
# Config (env-first; sensible defaults)
# ────────────────────────────────────────────────────────────────────────────────
MANIFEST_KEY = os.getenv("STOCKS_MANIFEST_KEY", "raw/manifests/manifest_latest.txt")
BUCKET_NAME = os.getenv("BUCKET_NAME")  # expected: stock-market-elt
SNOWFLAKE_CONN_ID = os.getenv("SNOWFLAKE_CONN_ID", "snowflake_default")

# COPY tuning
BATCH_SIZE = int(os.getenv("SNOWFLAKE_COPY_BATCH_SIZE", "500"))      # FILES list size per COPY
ON_ERROR = os.getenv("SNOWFLAKE_COPY_ON_ERROR", "ABORT_STATEMENT")   # or 'CONTINUE'
FORCE = os.getenv("SNOWFLAKE_COPY_FORCE", "FALSE").upper()           # TRUE to bypass load history
DO_VALIDATE = os.getenv("SNOWFLAKE_COPY_VALIDATE", "FALSE").upper() == "TRUE"
PREFILTER_LOADED = os.getenv("SNOWFLAKE_PREFILTER_LOADED", "TRUE").upper() == "TRUE"
COPY_HISTORY_LOOKBACK_HOURS = int(os.getenv("SNOWFLAKE_COPY_HISTORY_LOOKBACK_HOURS", "168"))  # 7d


@dag(
    dag_id="polygon_stocks_load",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    schedule=[S3_STOCKS_MANIFEST_DATASET],   # fires when stocks manifest updates
    catchup=False,
    tags=["load", "polygon", "snowflake", "stocks"],
    dagrun_timeout=timedelta(hours=2),
    max_active_runs=1,
)
def polygon_stocks_load_dag():

    # ────────────────────────────────────────────────────────────────────────────
    # Resolve Snowflake context (from Secrets-backed connection extras)
    # ────────────────────────────────────────────────────────────────────────────
    conn = BaseHook.get_connection(SNOWFLAKE_CONN_ID)
    x = (conn.extra_dejson or {})

    # Required
    SF_DB = x.get("database")
    if not SF_DB:
        raise ValueError(
            "Snowflake connection extras must include 'database' in secret "
            "'airflow/connections/snowflake_default' (extras)."
        )

    # Table target (RAW by default)
    TABLE_SCHEMA = x.get("table_schema", x.get("raw_schema", "RAW"))
    TABLE_NAME = x.get("stocks_table", "source_polygon_stocks_raw")

    # Stage location (PUBLIC.S3_STAGE by default per setup script)
    STAGE_DB = x.get("stage_db", SF_DB)
    STAGE_SCHEMA = x.get("stage_schema", "PUBLIC")
    STAGE_NAME = x.get("stage", "S3_STAGE")

    FQ_TABLE = f"{SF_DB}.{TABLE_SCHEMA}.{TABLE_NAME}"
    FQ_STAGE = f"@{STAGE_DB}.{STAGE_SCHEMA}.{STAGE_NAME}"        # for COPY
    FQ_STAGE_NO_AT = f"{STAGE_DB}.{STAGE_SCHEMA}.{STAGE_NAME}"    # for DESC/SHOW

    if not BUCKET_NAME:
        raise RuntimeError("BUCKET_NAME env var is required (e.g., 'stock-market-elt').")

    # ────────────────────────────────────────────────────────────────────────────
    # Tasks
    # ────────────────────────────────────────────────────────────────────────────
    @task
    def create_snowflake_table():
        """Create/ensure the typed landing table (lineage-friendly for dbt incremental)."""
        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        sql = f"""
        CREATE TABLE IF NOT EXISTS {FQ_TABLE} (
            -- Business columns
            ticker TEXT,
            polygon_trade_date DATE,
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

    @task
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
    def get_stage_relative_keys_from_manifest() -> list[str]:
        """
        Read the S3 manifest and normalize keys into stage-relative paths.
        Supports:
          a) Flat manifest — one 'raw/stocks/.../*.json[.gz]' key per line.
          b) Pointer manifest — first line 'POINTER=<s3_key>' → flat manifest.
        Filters to 'stocks/', dedupes while preserving order.
        """
        s3 = S3Hook()

        def _read_lines(key: str) -> list[str]:
            if not s3.check_for_key(key, bucket_name=BUCKET_NAME):
                raise FileNotFoundError(f"Manifest not found: s3://{BUCKET_NAME}/{key}")
            content = s3.read_key(key=key, bucket_name=BUCKET_NAME) or ""
            return [ln.strip() for ln in content.splitlines() if ln.strip()]

        lines = _read_lines(MANIFEST_KEY)
        if not lines:
            raise AirflowSkipException("Manifest is empty; nothing to load.")

        first = lines[0]
        if first.startswith("POINTER="):
            pointed_key = first.split("=", 1)[1].strip()
            if not pointed_key:
                raise ValueError(f"Pointer manifest has empty target in {MANIFEST_KEY}")
            lines = _read_lines(pointed_key)
            if not lines:
                raise AirflowSkipException(f"Pointer target is empty: {pointed_key}")

        # Normalize to stage-relative: strip 'raw/' prefix, filter to stocks/
        rel = [(k[4:] if k.startswith("raw/") else k) for k in lines]
        rel = [k for k in rel if k.startswith("stocks/")]

        # De-dupe while preserving order
        seen = set()
        deduped: list[str] = []
        for k in rel:
            if k not in seen:
                seen.add(k)
                deduped.append(k)

        if not deduped:
            raise AirflowSkipException("No eligible stocks files in manifest after filtering.")

        print(f"Manifest files (stocks/*): {len(deduped)} (from {len(lines)} raw entries)")
        return deduped

    @task
    def prefilter_already_loaded(keys: list[str]) -> list[str]:
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
    def chunk_keys(keys: list[str]) -> list[list[str]]:
        """Split keys into FILES=() batches to keep COPY statements predictable."""
        if not keys:
            raise AirflowSkipException("No files to load after prefiltering.")
        return [keys[i : i + BATCH_SIZE] for i in range(0, len(keys), BATCH_SIZE)]

    def _copy_sql(files_clause: str, validation_only: bool = False) -> str:
        """
        Compose COPY or VALIDATION SQL. We:
          - project JSON fields into typed columns
          - include lineage columns via METADATA$FILENAME / METADATA$FILE_ROW_NUMBER
          - allow both .json and .json.gz (COMPRESSION inferred from extension)
          - rely on load history for idempotency (FORCE={FORCE})
        """
        validate = " VALIDATION_MODE = 'RETURN_ERRORS'" if validation_only else ""
        return f"""
        COPY INTO {FQ_TABLE}
          (ticker, polygon_trade_date, volume, vwap, "open", "close", high, low, transactions, source_file, source_row_number)
        FROM (
            SELECT
                $1:ticker::TEXT                                                      AS ticker,

                /* Event ms → TIMESTAMP_NTZ → DATE */
                TO_DATE(
                  TO_TIMESTAMP_NTZ( TRY_TO_NUMBER($1:results[0]:t::STRING) / 1000 )
                )                                                                     AS polygon_trade_date,

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
        FORCE = {FORCE}
        {validate};
        """

    @task(retries=2)
    def validate_batch_in_snowflake(batch: list[str]) -> int:
        """Optional dry-run validation. Returns the number of files checked."""
        if not DO_VALIDATE or not batch:
            return 0
        files_clause = ", ".join(f"'{k}'" for k in batch)
        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        hook.run(_copy_sql(files_clause, validation_only=True))
        print(f"Validated batch of {len(batch)} files for {FQ_TABLE}.")
        return len(batch)

    @task(outlets=[SNOWFLAKE_STOCKS_RAW_DATASET], retries=2)
    def copy_batch_to_snowflake(batch: list[str]) -> int:
        """
        COPY a single batch using FILES=().
        - FORCE=FALSE + Snowflake load history → skip already ingested files.
        - ON_ERROR configurable (ABORT_STATEMENT or CONTINUE).
        """
        if not batch:
            return 0
        files_clause = ", ".join(f"'{k}'" for k in batch)
        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        hook.run(_copy_sql(files_clause, validation_only=False))
        print(f"Loaded batch of {len(batch)} files into {FQ_TABLE}.")
        return len(batch)

    @task
    def sum_loaded(counts: list[int]) -> int:
        """Visibility: how many files were targeted across COPY batches."""
        total = sum(counts or [])
        print(f"Total files targeted for load: {total}")
        return total

    # ────────────────────────────────────────────────────────────────────────────
    # Flow
    # ────────────────────────────────────────────────────────────────────────────
    tbl = create_snowflake_table()
    stage_ok = check_stage_exists()
    rel_keys = get_stage_relative_keys_from_manifest()
    remaining = prefilter_already_loaded(rel_keys)
    batches = chunk_keys(remaining)

    validated_counts = validate_batch_in_snowflake.expand(batch=batches)
    loaded_counts = copy_batch_to_snowflake.expand(batch=batches)
    total = sum_loaded(loaded_counts)

    tbl >> stage_ok >> rel_keys >> remaining >> batches >> validated_counts >> loaded_counts >> total


# Instantiate the DAG
polygon_stocks_load_dag()
