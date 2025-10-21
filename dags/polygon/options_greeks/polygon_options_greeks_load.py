# =============================================================================
# Polygon Options → Snowflake (Loader; Greeks — PARSED)
# -----------------------------------------------------------------------------
# 1) COPY raw JSON(.gz) from S3 external stage → STAGING table (VARIANT)
# 2) INSERT parsed rows → TYPED landing table using LATERAL FLATTEN
#    (idempotent via anti-join on (source_file, source_row_number))
#
# Datasets:
#   - schedule=[S3_OPTIONS_GREEKS_MANIFEST_DATASET]
#   - INSERT task outlets=[SNOWFLAKE_OPTIONS_GREEKS_RAW_DATASET]
#
# Default manifest pointer:
#   raw/manifests/polygon_options_greeks_manifest_latest.txt
# =============================================================================

from __future__ import annotations
import os
from datetime import timedelta

import pendulum

from airflow.decorators import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.exceptions import AirflowSkipException, AirflowFailException
from airflow.hooks.base import BaseHook

from dags.utils.polygon_datasets import (
    S3_OPTIONS_GREEKS_MANIFEST_DATASET,
    SNOWFLAKE_OPTIONS_GREEKS_RAW_DATASET,
)

# ────────────────────────────────────────────────────────────────────────────────
# Config
# ────────────────────────────────────────────────────────────────────────────────
MANIFEST_KEY = os.getenv(
    "OPTIONS_GREEKS_MANIFEST_KEY",
    "raw/manifests/polygon_options_greeks_manifest_latest.txt"
)
BUCKET_NAME = os.getenv("BUCKET_NAME")  # expected: stock-market-elt
SNOWFLAKE_CONN_ID = os.getenv("SNOWFLAKE_CONN_ID", "snowflake_default")

BATCH_SIZE = int(os.getenv("SNOWFLAKE_COPY_BATCH_SIZE", "400"))
ON_ERROR = os.getenv("SNOWFLAKE_COPY_ON_ERROR", "ABORT_STATEMENT")
FORCE = os.getenv("SNOWFLAKE_COPY_FORCE", "FALSE").upper()
DO_VALIDATE = os.getenv("SNOWFLAKE_COPY_VALIDATE", "FALSE").upper() == "TRUE"
PREFILTER_LOADED = os.getenv("SNOWFLAKE_PREFILTER_LOADED", "TRUE").upper() == "TRUE"
COPY_HISTORY_LOOKBACK_HOURS = int(os.getenv("SNOWFLAKE_COPY_HISTORY_LOOKBACK_HOURS", "168"))  # 7d


@dag(
    dag_id="polygon_options_greeks_load",
    start_date=pendulum.datetime(2025, 10, 1, tz="UTC"),
    schedule=[S3_OPTIONS_GREEKS_MANIFEST_DATASET],
    catchup=False,
    tags=["load", "polygon", "snowflake", "options", "greeks", "parsed"],
    dagrun_timeout=timedelta(hours=2),
    max_active_runs=1,
)
def polygon_options_greeks_load_dag():

    # Resolve Snowflake context
    conn = BaseHook.get_connection(SNOWFLAKE_CONN_ID)
    x = conn.extra_dejson or {}

    SF_DB = x.get("database")
    SF_SCHEMA = x.get("schema")
    if not SF_DB or not SF_SCHEMA:
        raise AirflowFailException(
            "Snowflake connection extras must include 'database' and 'schema'. "
            "Edit secret 'airflow/connections/snowflake_default' extras accordingly."
        )

    STAGE_NAME = x.get("stage", "s3_stage")
    TYPED_TABLE = x.get("options_greeks_table", "source_polygon_options_greeks_raw")
    STAGING_TABLE = x.get("options_greeks_stage_table", "source_polygon_options_greeks_stage")

    FQ_STAGE = f"@{SF_DB}.{SF_SCHEMA}.{STAGE_NAME}"
    FQ_STAGE_NO_AT = f"{SF_DB}.{SF_SCHEMA}.{STAGE_NAME}"
    FQ_STAGE_TBL = f"{SF_DB}.{SF_SCHEMA}.{STAGING_TABLE}"
    FQ_TYPED_TBL = f"{SF_DB}.{SF_SCHEMA}.{TYPED_TABLE}"

    if not BUCKET_NAME:
        raise AirflowFailException("BUCKET_NAME env var is required (e.g., 'stock-market-elt').")

    # ────────────────────────────────────────────────────────────────────────────
    # Tasks
    # ────────────────────────────────────────────────────────────────────────────
    @task
    def create_snowflake_tables():
        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        sql = f"""
        CREATE TABLE IF NOT EXISTS {FQ_STAGE_TBL} (
            raw VARIANT,
            source_file TEXT,
            load_ts TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
        );

        CREATE TABLE IF NOT EXISTS {FQ_TYPED_TBL} (
            underlying_ticker TEXT,
            contract_symbol   TEXT,
            as_of_ts          TIMESTAMP_NTZ,
            trade_date        DATE,
            delta             NUMERIC(19, 6),
            gamma             NUMERIC(19, 6),
            theta             NUMERIC(19, 6),
            vega              NUMERIC(19, 6),
            implied_vol       NUMERIC(19, 6),
            open_interest     BIGINT,
            bid_price         NUMERIC(19, 6),
            ask_price         NUMERIC(19, 6),
            source_file       TEXT,
            source_row_number BIGINT,
            inserted_at       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
        );
        """
        hook.run(sql)

    @task
    def check_stage_exists():
        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        try:
            hook.run(f"DESC STAGE {FQ_STAGE_NO_AT}")
        except Exception as e:
            raise AirflowFailException(
                f"Stage {FQ_STAGE_NO_AT} not found or not accessible. "
                f"Verify existence and privileges. Original error: {e}"
            )

    @task
    def get_stage_relative_keys_from_manifest() -> list[str]:
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
                raise AirflowFailException(f"Pointer manifest has empty target in {MANIFEST_KEY}")
            lines = _read_lines(pointed_key)
            if not lines:
                raise AirflowSkipException(f"Pointer target is empty: {pointed_key}")

        rel = [(k[4:] if k.startswith("raw/") else k) for k in lines]
        rel = [k for k in rel if k.startswith("options_greeks/")]

        seen = set()
        deduped: list[str] = []
        for k in rel:
            if k not in seen:
                seen.add(k)
                deduped.append(k)

        if not deduped:
            raise AirflowSkipException("No eligible options_greeks files in manifest after filtering.")
        print(f"Manifest files (options_greeks/*): {len(deduped)} (from {len(lines)} raw entries)")
        return deduped

    @task
    def prefilter_already_copied_to_stage(keys: list[str]) -> list[str]:
        if not PREFILTER_LOADED or not keys:
            return keys

        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        lookback = COPY_HISTORY_LOOKBACK_HOURS
        sql = f"""
        with hist as (
          select FILE_NAME
          from table(information_schema.copy_history(
            table_name => '{FQ_STAGE_TBL}',
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
        print(f"Prefiltered {len(keys) - len(remaining)} already-copied (lookback {lookback}h).")
        return remaining

    @task
    def chunk_keys(keys: list[str]) -> list[list[str]]:
        if not keys:
            raise AirflowSkipException("No files to load after prefiltering.")
        return [keys[i : i + BATCH_SIZE] for i in range(0, len(keys), BATCH_SIZE)]

    def _copy_to_stage_sql(files_clause: str, validation_only: bool = False) -> str:
        validate = " VALIDATION_MODE = 'RETURN_ERRORS'" if validation_only else ""
        return f"""
        COPY INTO {FQ_STAGE_TBL}
          (raw, source_file)
        FROM (
          SELECT
            $1,
            METADATA$FILENAME
          FROM {FQ_STAGE}
        )
        FILES = ({files_clause})
        FILE_FORMAT = (TYPE = 'JSON')
        ON_ERROR = '{ON_ERROR}'
        FORCE = {FORCE}
        {validate};
        """

    @task(retries=2)
    def validate_batch_copy_to_stage(batch: list[str]) -> int:
        if not DO_VALIDATE or not batch:
            return 0
        files_clause = ", ".join(f"'{k}'" for k in batch)
        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        hook.run(_copy_to_stage_sql(files_clause, validation_only=True))
        print(f"Validated COPY→stage for {len(batch)} files into {FQ_STAGE_TBL}.")
        return len(batch)

    @task(retries=2)
    def copy_batch_to_stage(batch: list[str]) -> int:
        if not batch:
            return 0
        files_clause = ", ".join(f"'{k}'" for k in batch)
        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        hook.run(_copy_to_stage_sql(files_clause, validation_only=False))
        print(f"Copied {len(batch)} greeks files into staging {FQ_STAGE_TBL}.")
        return len(batch)

    def _insert_parsed_sql() -> str:
        # All numeric fields: VARIANT -> ::STRING -> TRY_TO_NUMBER(…, 38, 12) to avoid VARIANT→FLOAT try-cast path.
        return f"""
        INSERT INTO {FQ_TYPED_TBL} (
            underlying_ticker, contract_symbol, as_of_ts, trade_date,
            delta, gamma, theta, vega, implied_vol, open_interest,
            bid_price, ask_price,
            source_file, source_row_number
        )
        SELECT
            COALESCE(s.raw:_meta:underlying::TEXT,
                     f.value:underlying_asset:ticker::TEXT)                                  AS underlying_ticker,

            COALESCE(f.value:details:ticker::TEXT,
                     f.value:ticker::TEXT,
                     s.raw:_meta:contract::TEXT)                                            AS contract_symbol,

            TRY_TO_TIMESTAMP_NTZ(s.raw:_meta:as_of::TEXT)                                    AS as_of_ts,
            TO_DATE(TRY_TO_TIMESTAMP_NTZ(s.raw:_meta:as_of::TEXT))                           AS trade_date,

            TRY_TO_NUMBER(f.value:greeks:delta::STRING, 38, 12)                              AS delta,
            TRY_TO_NUMBER(f.value:greeks:gamma::STRING, 38, 12)                              AS gamma,
            TRY_TO_NUMBER(f.value:greeks:theta::STRING, 38, 12)                              AS theta,
            TRY_TO_NUMBER(f.value:greeks:vega::STRING, 38, 12)                               AS vega,

            COALESCE(
              TRY_TO_NUMBER(f.value:implied_volatility::STRING, 38, 12),
              TRY_TO_NUMBER(f.value:greeks:implied_volatility::STRING, 38, 12)
            )                                                                               AS implied_vol,

            TRY_TO_NUMBER(f.value:open_interest::STRING)::BIGINT                             AS open_interest,

            -- Quotes: prefer last_quote.bid / last_quote.ask; include common fallbacks (cast via STRING first)
            COALESCE(
              TRY_TO_NUMBER(f.value:last_quote:bid::STRING, 38, 12),
              TRY_TO_NUMBER(f.value:last_quote:bid_price::STRING, 38, 12),
              TRY_TO_NUMBER(f.value:bid_price::STRING, 38, 12),
              TRY_TO_NUMBER(f.value:bid::STRING, 38, 12)
            )                                                                               AS bid_price,

            COALESCE(
              TRY_TO_NUMBER(f.value:last_quote:ask::STRING, 38, 12),
              TRY_TO_NUMBER(f.value:last_quote:ask_price::STRING, 38, 12),
              TRY_TO_NUMBER(f.value:ask_price::STRING, 38, 12),
              TRY_TO_NUMBER(f.value:ask::STRING, 38, 12)
            )                                                                               AS ask_price,

            s.source_file                                                                    AS source_file,
            f.index                                                                           AS source_row_number
        FROM {FQ_STAGE_TBL} AS s,
             LATERAL FLATTEN(INPUT => COALESCE(s.raw:data:results, s.raw:data:options)) AS f
        WHERE NOT EXISTS (
            SELECT 1
            FROM {FQ_TYPED_TBL} t
            WHERE t.source_file = s.source_file
              AND t.source_row_number = f.index
        );
        """

    @task(outlets=[SNOWFLAKE_OPTIONS_GREEKS_RAW_DATASET], retries=2)
    def insert_parsed_from_stage() -> int:
        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        hook.run(_insert_parsed_sql())
        return 0

    @task
    def sum_copied_to_stage(counts: list[int]) -> int:
        total = sum(counts or [])
        print(f"Total greeks files copied to staging: {total}")
        return total

    # ────────────────────────────────────────────────────────────────────────────
    # Flow
    # ────────────────────────────────────────────────────────────────────────────
    create = create_snowflake_tables()
    stage_ok = check_stage_exists()
    rel_keys = get_stage_relative_keys_from_manifest()
    remaining = prefilter_already_copied_to_stage(rel_keys)
    batches = chunk_keys(remaining)

    validated = validate_batch_copy_to_stage.expand(batch=batches)
    copied = copy_batch_to_stage.expand(batch=batches)
    copied_total = sum_copied_to_stage(copied)
    inserted = insert_parsed_from_stage()

    create >> stage_ok >> remaining >> batches >> validated >> copied >> copied_total >> inserted


polygon_options_greeks_load_dag()
