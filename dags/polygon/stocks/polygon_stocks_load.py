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
# Config
# ────────────────────────────────────────────────────────────────────────────────
MANIFEST_KEY = os.getenv("STOCKS_MANIFEST_KEY", "raw/manifests/manifest_latest.txt")
BUCKET_NAME = os.getenv("BUCKET_NAME")  # expected: stock-market-elt
SNOWFLAKE_CONN_ID = os.getenv("SNOWFLAKE_CONN_ID", "snowflake_default")
BATCH_SIZE = int(os.getenv("SNOWFLAKE_COPY_BATCH_SIZE", "500"))  # FILES list batch size
ON_ERROR = os.getenv("SNOWFLAKE_COPY_ON_ERROR", "ABORT_STATEMENT")  # or 'CONTINUE'
FORCE = os.getenv("SNOWFLAKE_COPY_FORCE", "FALSE").upper()  # 'TRUE' to force reloads


@dag(
    dag_id="polygon_stocks_load",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    schedule=[S3_STOCKS_MANIFEST_DATASET],
    catchup=False,
    tags=["load", "polygon", "snowflake", "stocks"],
    dagrun_timeout=timedelta(hours=2),
    max_active_runs=1,
)
def polygon_stocks_load_dag():

    # ────────────────────────────────────────────────────────────────────────────
    # Resolve Snowflake context (from connection extras)
    # ────────────────────────────────────────────────────────────────────────────
    conn = BaseHook.get_connection(SNOWFLAKE_CONN_ID)
    x = conn.extra_dejson or {}

    SF_DB = x.get("database")
    SF_SCHEMA = x.get("schema")
    if not SF_DB or not SF_SCHEMA:
        raise ValueError(
            "Snowflake connection extras must include 'database' and 'schema'. "
            "Edit secret 'airflow/connections/snowflake_default' extras accordingly."
        )
    STAGE_NAME = x.get("stage", "s3_stage")  # external stage already configured
    TABLE_NAME = x.get("stocks_table", "source_polygon_stocks_raw")

    FQ_TABLE = f"{SF_DB}.{SF_SCHEMA}.{TABLE_NAME}"
    FQ_STAGE = f"@{SF_DB}.{SF_SCHEMA}.{STAGE_NAME}"

    if not BUCKET_NAME:
        raise RuntimeError("BUCKET_NAME env var is required (e.g., 'stock-market-elt').")

    # ────────────────────────────────────────────────────────────────────────────
    # Tasks
    # ────────────────────────────────────────────────────────────────────────────
    @task
    def create_snowflake_table():
        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        sql = f"""
        CREATE TABLE IF NOT EXISTS {FQ_TABLE} (
            ticker TEXT,
            trade_date DATE,
            open NUMERIC(19, 4),
            high NUMERIC(19, 4),
            low NUMERIC(19, 4),
            close NUMERIC(19, 4),
            volume BIGINT,
            vwap NUMERIC(19, 4),
            transactions BIGINT,
            inserted_at TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP()
        );
        """
        hook.run(sql)

    @task
    def check_stage_exists():
        """Nice-to-have guard: make sure stage name matches what Snowflake has."""
        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        try:
            hook.run(f"DESC STAGE {FQ_STAGE}")
        except Exception as e:
            raise RuntimeError(
                f"Stage {FQ_STAGE} not found or not accessible. "
                f"Make sure your Snowflake external stage exists and privileges are set. Original error: {e}"
            )

    @task
    def get_stage_relative_keys_from_manifest() -> list[str]:
        """
        Read the S3 manifest and normalize keys into stage-relative paths.
        Handles BOTH:
          a) Flat manifest: each line is 'raw/stocks/.../*.json'
          b) Pointer manifest: first non-empty line 'POINTER=<s3_key>' (same bucket)
             which points to a flat manifest created by the writer (e.g., backfill).
        Only includes stocks files, de-duped, order preserved.
        """
        s3 = S3Hook()

        def _read_lines(key: str) -> list[str]:
            if not s3.check_for_key(key, bucket_name=BUCKET_NAME):
                raise FileNotFoundError(f"Manifest not found: s3://{BUCKET_NAME}/{key}")
            content = s3.read_key(key=key, bucket_name=BUCKET_NAME) or ""
            return [ln.strip() for ln in content.splitlines() if ln.strip()]

        lines = _read_lines(MANIFEST_KEY)
        # Pointer mode? First non-empty line like "POINTER=raw/manifests/stocks/manifest_2025-10-15T18-22-09.txt"
        first = lines[0] if lines else ""
        if first.startswith("POINTER="):
            pointed_key = first.split("=", 1)[1].strip()
            if not pointed_key:
                raise ValueError(f"Pointer manifest has empty target in {MANIFEST_KEY}")
            lines = _read_lines(pointed_key)

        if not lines:
            raise AirflowSkipException("Manifest is empty; nothing to load.")

        # Normalize to stage-relative: strip the leading 'raw/' prefix, filter to stocks/
        rel = [(k[4:] if k.startswith("raw/") else k) for k in lines]
        rel = [k for k in rel if k.startswith("stocks/")]

        # De-dupe while preserving order
        seen = set()
        deduped = []
        for k in rel:
            if k not in seen:
                seen.add(k)
                deduped.append(k)

        if not deduped:
            raise AirflowSkipException("No eligible stocks files in manifest after filtering.")

        print(f"Manifest files (stocks/*): {len(deduped)} (from {len(lines)} raw entries)")
        return deduped

    @task
    def chunk_keys(keys: list[str]) -> list[list[str]]:
        """Split keys into batches for the Snowflake FILES clause."""
        return [keys[i : i + BATCH_SIZE] for i in range(0, len(keys), BATCH_SIZE)]

    @task(outlets=[SNOWFLAKE_STOCKS_RAW_DATASET], retries=2)
    def copy_batch_to_snowflake(batch: list[str]) -> int:
        """
        COPY a single batch using FILES=(). FORCE=FALSE leverages load history:
        previously ingested files (same stage path) are skipped automatically.
        """
        if not batch:
            return 0
        files_clause = ", ".join(f"'{k}'" for k in batch)

        copy_sql = f"""
        COPY INTO {FQ_TABLE}
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
            FROM {FQ_STAGE}
        )
        FILES = ({files_clause})
        FILE_FORMAT = (TYPE = 'JSON')
        ON_ERROR = '{ON_ERROR}'
        FORCE = {FORCE};
        """
        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        hook.run(copy_sql)
        print(f"Loaded batch of {len(batch)} files into {FQ_TABLE}.")
        return len(batch)

    @task
    def sum_loaded(counts: list[int]) -> int:
        total = sum(counts or [])
        print(f"Total files targeted for load: {total}")
        return total

    # Flow
    tbl = create_snowflake_table()
    stage_ok = check_stage_exists()
    rel_keys = get_stage_relative_keys_from_manifest()
    batches = chunk_keys(rel_keys)
    loaded_counts = copy_batch_to_snowflake.expand(batch=batches)
    total = sum_loaded(loaded_counts)

    tbl >> stage_ok >> batches >> loaded_counts >> total


polygon_stocks_load_dag()
