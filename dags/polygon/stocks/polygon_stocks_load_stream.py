# dags/polygon/stocks/polygon_stocks_load_stream.py
# =============================================================================
# Polygon Stocks → Snowflake (Stream Loader) — dataset-triggered (latest pointer)
# -----------------------------------------------------------------------------
# Purpose:
#   Load *ready* stocks data as soon as the daily ingest DAG updates the
#   "latest" POINTER file. No date math, no S3 prefix scans.
#
# Data contract:
#   • Latest pointer (mutable): raw/manifests/polygon_stocks_manifest_latest.txt
#       - File body: "POINTER=raw/manifests/stocks/YYYY-MM-DD/manifest.txt\n"
#   • Per-day manifest (immutable): raw/manifests/stocks/YYYY-MM-DD/manifest.txt
#       - Body is a newline-delimited list of S3 keys under:
#           raw/stocks/year=YYYY/month=MM/day=DD/ticker=<T>.json.gz
#
# Snowflake:
#   • Destination table (landing): RAW.SOURCE_POLYGON_STOCKS_RAW   (config via conn extras)
#   • External stage (read-only):  STAGES.S3_STAGE                  (config via conn extras)
#
# Enterprise features:
#   • Triggers on Dataset (data-aware scheduling)
#   • Idempotent loads (FORCE=FALSE + load history)
#   • Optional prefilter vs information_schema.copy_history
#   • Secrets-first Snowflake config via connection extras
#   • Hive-style path awareness; stage-relative FILES=() usage
#   • Dedicated pool for loaders (set via LOAD_POOL_NAME or create 'load_pool')
# =============================================================================

from __future__ import annotations
import os
from typing import List, Optional
from datetime import timedelta

import pendulum
from airflow.decorators import dag, task
from airflow.exceptions import AirflowSkipException
from airflow.hooks.base import BaseHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from dags.utils.polygon_datasets import (
    S3_STOCKS_MANIFEST_DATASET,
    SNOWFLAKE_STOCKS_RAW_DATASET,
)

# ────────────────────────────────────────────────────────────────────────────────
# Config (env-first; all IaC-friendly)
# ────────────────────────────────────────────────────────────────────────────────
BUCKET_NAME = os.getenv("BUCKET_NAME")  # expected: stock-market-elt
SNOWFLAKE_CONN_ID = os.getenv("SNOWFLAKE_CONN_ID", "snowflake_default")
LATEST_POINTER_KEY = os.getenv(
    "STOCKS_LATEST_POINTER_KEY",
    "raw/manifests/polygon_stocks_manifest_latest.txt",
)

# COPY tuning
BATCH_SIZE = int(os.getenv("SNOWFLAKE_COPY_BATCH_SIZE", "500"))      # FILES list size per COPY
ON_ERROR = os.getenv("SNOWFLAKE_COPY_ON_ERROR", "ABORT_STATEMENT")   # or 'CONTINUE'
FORCE = os.getenv("SNOWFLAKE_COPY_FORCE", "FALSE").upper()           # TRUE to bypass load history
PREFILTER_LOADED = os.getenv("SNOWFLAKE_PREFILTER_LOADED", "TRUE").upper() == "TRUE"
COPY_HISTORY_LOOKBACK_HOURS = int(os.getenv("SNOWFLAKE_COPY_HISTORY_LOOKBACK_HOURS", "168"))  # 7d
DO_PROBE_VALIDATE = os.getenv("SNOWFLAKE_PROBE_VALIDATE", "FALSE").upper() == "TRUE"

# Airflow defaults for warehouse safety
DEFAULT_ARGS = {
    "owner": "data-platform",
    "retries": 2,
    "retry_delay": pendulum.duration(minutes=5),
    "pool": os.getenv("LOAD_POOL_NAME", "load_pool"),  # separate from API pool
}


@dag(
    dag_id="polygon_stocks_load_stream",
    description="Dataset-triggered loader for daily Polygon stocks (reads latest POINTER → per-day manifest).",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    schedule=[S3_STOCKS_MANIFEST_DATASET],   # ← data-aware trigger from the ingest_daily DAG
    catchup=False,                            # stream only; backfills handled by backfill loader
    default_args=DEFAULT_ARGS,
    dagrun_timeout=timedelta(hours=2),
    max_active_runs=2,                        # allow overlap if needed; loaders are idempotent
    tags=["load", "polygon", "stocks", "snowflake", "stream"],
)
def polygon_stocks_load_stream_dag():

    # ────────────────────────────────────────────────────────────────────────────
    # Resolve Snowflake context (via Secrets-backed connection extras)
    # ────────────────────────────────────────────────────────────────────────────
    @task
    def resolve_snowflake_ctx() -> dict:
        """
        Pull DB/schema/stage/table names from connection extras to keep code IaC-friendly.
        Expected extras (example):
          {
            "database": "STOCKS_ELT_DB",
            "schema": "STAGES",
            "raw_schema": "RAW",
            "stage_schema": "STAGES",
            "stage": "S3_STAGE",
            "stocks_table": "SOURCE_POLYGON_STOCKS_RAW"
          }
        """
        conn = BaseHook.get_connection(SNOWFLAKE_CONN_ID)
        x = conn.extra_dejson or {}
        sf_db = x.get("database")
        if not sf_db:
            raise ValueError("Snowflake connection extras must include 'database'.")

        raw_schema = x.get("raw_schema", "RAW")
        stage_schema = x.get("stage_schema", x.get("schema", "STAGES"))
        stage_name = x.get("stage", "S3_STAGE")
        table_name = x.get("stocks_table", "SOURCE_POLYGON_STOCKS_RAW")

        return {
            "FQ_TABLE": f"{sf_db}.{raw_schema}.{table_name}",
            "FQ_STAGE": f"@{sf_db}.{stage_schema}.{stage_name}",
            "FQ_STAGE_NO_AT": f"{sf_db}.{stage_schema}.{stage_name}",
        }

    # ────────────────────────────────────────────────────────────────────────────
    # Snowflake preflight
    # ────────────────────────────────────────────────────────────────────────────
    @task
    def ensure_table(ctx: dict) -> None:
        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        hook.run(f"""
            CREATE TABLE IF NOT EXISTS {ctx["FQ_TABLE"]} (
                -- Business columns
                stock_symbol TEXT,
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
        """)

    @task
    def check_stage(ctx: dict) -> None:
        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        try:
            hook.run(f"DESC STAGE {ctx['FQ_STAGE_NO_AT']}")
        except Exception as e:
            raise RuntimeError(
                f"Stage {ctx['FQ_STAGE_NO_AT']} not found or not accessible. "
                f"Verify existence and privileges. Original error: {e}"
            )

    # ────────────────────────────────────────────────────────────────────────────
    # Manifest resolution (pointer → per-day manifest → keys)
    # ────────────────────────────────────────────────────────────────────────────
    @task
    def read_latest_pointer() -> str:
        if not BUCKET_NAME:
            raise RuntimeError("BUCKET_NAME env var is required (e.g., 'stock-market-elt').")
        s3 = S3Hook()
        if not s3.check_for_key(LATEST_POINTER_KEY, bucket_name=BUCKET_NAME):
            raise AirflowSkipException(f"Latest pointer not found: s3://{BUCKET_NAME}/{LATEST_POINTER_KEY}")
        body = s3.read_key(key=LATEST_POINTER_KEY, bucket_name=BUCKET_NAME) or ""
        body = body.strip()
        if not body:
            raise AirflowSkipException("Latest pointer is empty; skipping.")
        if not body.startswith("POINTER="):
            # Backward-compat: treat file body as the actual manifest content
            return "__INLINE__:" + body
        return body.split("=", 1)[1].strip()  # returns per-day manifest key

    @task
    def read_per_day_manifest(pointer_or_inline: str) -> List[str]:
        """
        Returns a de-duplicated list of **stage-relative** keys limited to stocks/*.
        - If input starts with "__INLINE__:", it contains newline-delimited keys directly.
        - Else input is the S3 key of the per-day manifest to read.
        """
        s3 = S3Hook()
        if pointer_or_inline.startswith("__INLINE__:"):
            content = pointer_or_inline.replace("__INLINE__:", "", 1)
        else:
            manifest_key = pointer_or_inline
            if not s3.check_for_key(manifest_key, bucket_name=BUCKET_NAME):
                raise AirflowSkipException(f"Per-day manifest not found: s3://{BUCKET_NAME}/{manifest_key}")
            content = s3.read_key(key=manifest_key, bucket_name=BUCKET_NAME) or ""

        # Normalize to stage-relative: strip leading 'raw/' and filter to stocks/*
        raw_keys = [ln.strip() for ln in content.splitlines() if ln.strip()]
        rel = [(k[4:] if k.startswith("raw/") else k) for k in raw_keys]
        rel = [k for k in rel if k.startswith("stocks/")]

        # de-dupe preserve order
        seen, out = set(), []
        for k in rel:
            if k not in seen:
                seen.add(k)
                out.append(k)

        if not out:
            raise AirflowSkipException("Resolved manifest has 0 eligible stock files; skipping.")
        return out

    # ────────────────────────────────────────────────────────────────────────────
    # Optional: prefilter keys that are already loaded recently
    # ────────────────────────────────────────────────────────────────────────────
    @task
    def prefilter_loaded(ctx: dict, keys: List[str]) -> List[str]:
        if not PREFILTER_LOADED or not keys:
            return keys
        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        rows = hook.get_records(f"""
            with hist as (
              select FILE_NAME
              from table(information_schema.copy_history(
                table_name => '{ctx["FQ_TABLE"]}',
                start_time => dateadd('hour', -{COPY_HISTORY_LOOKBACK_HOURS}, current_timestamp())
              ))
            )
            select FILE_NAME from hist;
        """) or []
        already = {r[0] for r in rows if r and r[0]}
        if not already:
            return keys
        remaining = [k for k in keys if k not in already]
        removed = len(keys) - len(remaining)
        print(f"Prefiltered {removed} already-loaded files (lookback {COPY_HISTORY_LOOKBACK_HOURS}h).")
        return remaining

    @task
    def chunk_keys(keys: List[str]) -> List[List[str]]:
        if not keys:
            raise AirflowSkipException("No files to load after prefiltering.")
        return [keys[i:i+BATCH_SIZE] for i in range(0, len(keys), BATCH_SIZE)]

    # ────────────────────────────────────────────────────────────────────────────
    # COPY INTO with transformed SELECT (typed projection + lineage columns)
    # ────────────────────────────────────────────────────────────────────────────
    def _copy_sql(ctx: dict, files: List[str]) -> str:
        files_clause = ", ".join(f"'{k}'" for k in files)
        return f"""
        COPY INTO {ctx["FQ_TABLE"]}
          (stock_symbol, polygon_trade_date, polygon_bar_ts, volume, vwap, "open", "close", high, low, transactions, source_file, source_row_number)
        FROM (
            SELECT
                $1:ticker::TEXT                                                        AS stock_symbol,
                TO_DATE(TO_TIMESTAMP_NTZ(TRY_TO_NUMBER($1:results[0]:t::STRING)/1000)) AS polygon_trade_date,
                TO_TIMESTAMP_NTZ(TRY_TO_NUMBER($1:results[0]:t::STRING)/1000)           AS polygon_bar_ts,
                TRY_TO_NUMBER($1:results[0]:v::STRING)::BIGINT                          AS volume,
                TRY_TO_NUMBER($1:results[0]:vw::STRING, 38, 12)::NUMERIC(19,4)          AS vwap,
                TRY_TO_NUMBER($1:results[0]:o::STRING, 38, 12)::NUMERIC(19,4)           AS "open",
                TRY_TO_NUMBER($1:results[0]:c::STRING, 38, 12)::NUMERIC(19,4)           AS "close",
                TRY_TO_NUMBER($1:results[0]:h::STRING, 38, 12)::NUMERIC(19,4)           AS high,
                TRY_TO_NUMBER($1:results[0]:l::STRING, 38, 12)::NUMERIC(19,4)           AS low,
                TRY_TO_NUMBER($1:results[0]:n::STRING)::BIGINT                          AS transactions,
                METADATA$FILENAME::TEXT                                                 AS source_file,
                METADATA$FILE_ROW_NUMBER::BIGINT                                         AS source_row_number
            FROM {ctx["FQ_STAGE"]}
        )
        FILES = ({files_clause})
        FILE_FORMAT = (TYPE = 'JSON')
        ON_ERROR = '{ON_ERROR}'
        FORCE = {FORCE};
        """

    @task(retries=1)
    def probe_validate(ctx: dict, batch: List[str]) -> int:
        """Optional dry-run COPY to check JSON/compression/access (no transform)."""
        if not DO_PROBE_VALIDATE or not batch:
            return 0
        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        hook.run("CREATE TEMP TABLE IF NOT EXISTS TMP_JSON_VALIDATION (v VARIANT);")
        files_clause = ", ".join(f"'{k}'" for k in batch)
        rows = hook.get_records(f"""
            COPY INTO TMP_JSON_VALIDATION
            FROM {ctx["FQ_STAGE"]}
            FILES = ({files_clause})
            FILE_FORMAT = (TYPE = 'JSON')
            VALIDATION_MODE = 'RETURN_ERRORS';
        """) or []
        if rows:
            raise RuntimeError(f"Probe validation failed for {len(rows)} row(s); first error: {rows[0]}")
        print(f"Probe validated batch of {len(batch)} files (no errors).")
        return len(batch)

    @task(outlets=[SNOWFLAKE_STOCKS_RAW_DATASET], retries=2)
    def copy_batch(ctx: dict, batch: List[str]) -> int:
        if not batch:
            return 0
        SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID).run(_copy_sql(ctx, batch))
        print(f"Loaded batch of {len(batch)} files into {ctx['FQ_TABLE']}.")
        return len(batch)

    @task
    def summarize(counts: List[int]) -> int:
        total = sum(counts or [])
        print(f"Total files targeted for load: {total}")
        return total

    # ────────────────────────────────────────────────────────────────────────────
    # Flow (dynamic mapping; no parse-time len())
    # ────────────────────────────────────────────────────────────────────────────
    ctx = resolve_snowflake_ctx()
    _tbl = ensure_table(ctx)
    _stg = check_stage(ctx)

    ptr = read_latest_pointer()
    keys = read_per_day_manifest(ptr)
    remaining = prefilter_loaded(ctx, keys)
    batches = chunk_keys(remaining)

    _probe = probe_validate.partial(ctx=ctx).expand(batch=batches)  # gated by DO_PROBE_VALIDATE
    loaded = copy_batch.partial(ctx=ctx).expand(batch=batches)
    _total = summarize(loaded)

    _tbl >> _stg >> ptr >> keys >> remaining >> batches >> loaded >> _total


polygon_stocks_load_stream_dag()
