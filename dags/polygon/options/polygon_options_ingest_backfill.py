from __future__ import annotations

import csv
import json
import os
import time
from typing import List

import pendulum
import requests
from datetime import timedelta

from airflow.decorators import dag, task
from airflow.exceptions import AirflowSkipException
from airflow.models.param import Param
from airflow.operators.python import get_current_context
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models import Variable  # Secrets Manager-backed Variables

from dags.utils.polygon_datasets import S3_OPTIONS_MANIFEST_DATASET

# ────────────────────────────────────────────────────────────────────────────────
# Config / Constants
# ────────────────────────────────────────────────────────────────────────────────
POLYGON_CONTRACTS_URL = "https://api.polygon.io/v3/reference/options/contracts"
POLYGON_AGGS_URL_BASE = "https://api.polygon.io/v2/aggs/ticker"

BUCKET_NAME = os.getenv("BUCKET_NAME", "test")
DBT_PROJECT_DIR = os.getenv("DBT_PROJECT_DIR", "/usr/local/airflow/dbt")

REQUEST_TIMEOUT_SECS = int(os.getenv("HTTP_REQUEST_TIMEOUT_SECS", "60"))
USER_AGENT = os.getenv("HTTP_USER_AGENT", "stocks-elt/polygon-options-backfill (manual)")
API_POOL = "api_pool"  # ensure the pool exists or remove this

# Rate limiting / batching
REQUEST_DELAY_SECONDS = float(os.getenv("POLYGON_REQUEST_DELAY_SECONDS", "0.5"))
SYMBOL_BATCH_SIZE = int(os.getenv("BACKFILL_SYMBOL_BATCH_SIZE", "500"))

def _get_polygon_options_key() -> str:
    """Resolve the Polygon OPTIONS API key from Airflow Variable (Secrets-backed) or env fallback."""
    try:
        val = Variable.get("polygon_options_api_key")
        if val:
            return val
    except Exception:
        pass
    val = os.getenv("POLYGON_OPTIONS_API_KEY")
    if val:
        return val
    raise RuntimeError(
        "Polygon Options API key not found. Create secret 'airflow/variables/polygon_options_api_key' "
        "in AWS Secrets Manager (plain text), or set env POLYGON_OPTIONS_API_KEY."
    )

def _rate_limited_get(url: str, params: dict | None, max_tries: int = 6) -> requests.Response:
    """
    Robust GET with exponential backoff for 429/5xx.
    Respects Retry-After when present. Raises for non-OK (except 404 which caller handles).
    """
    headers = {"User-Agent": USER_AGENT}
    backoff = 1.5
    delay = 1.0
    tries = 0
    while True:
        resp = requests.get(url, params=params, headers=headers, timeout=REQUEST_TIMEOUT_SECS)
        if resp.status_code == 429:
            retry_after = resp.headers.get("Retry-After")
            sleep_s = float(retry_after) if retry_after else delay
            time.sleep(sleep_s)
            delay *= backoff
        elif 500 <= resp.status_code < 600:
            if tries >= max_tries:
                resp.raise_for_status()
            time.sleep(delay)
            delay *= backoff
            tries += 1
        else:
            return resp

@dag(
    dag_id="polygon_options_ingest_backfill",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule=None,  # manual only
    catchup=False,
    tags=["ingestion", "polygon", "options", "backfill"],
    params={
        "start_date": Param(default="2025-10-06", type="string", description="Backfill start date (YYYY-MM-DD)"),
        "end_date": Param(default="2025-10-09", type="string", description="Backfill end date (YYYY-MM-DD)"),
    },
    dagrun_timeout=timedelta(hours=24),
)
def polygon_options_ingest_backfill_dag():
    """
    Manually backfill historical options data from Polygon:
      1) Discover option contracts for seed underlyings (via /v3/reference/options/contracts)
      2) Batch symbols to respect mapping limits and API rate limits
      3) For each symbol, pull aggregates over the date range (/v2/aggs/ticker/.../range/1/day)
      4) Write one JSON per (contract, trading day) to S3 under raw/
      5) Write manifest to trigger downstream load
    """

    @task(pool=API_POOL)
    def get_all_option_symbols() -> List[str]:
        """
        Read underlyings from dbt seeds and fetch all option contract symbols (paginated).
        """
        api_key = _get_polygon_options_key()

        custom_tickers_path = os.path.join(DBT_PROJECT_DIR, "seeds", "custom_tickers.csv")
        with open(custom_tickers_path, mode="r") as csvfile:
            reader = csv.DictReader(csvfile)
            underlyings = [row["ticker"].strip().upper() for row in reader if row.get("ticker")]

        all_symbols: List[str] = []
        for ticker in underlyings:
            params = {"underlying_ticker": ticker, "limit": 1000, "apiKey": api_key}
            # First page
            resp = _rate_limited_get(POLYGON_CONTRACTS_URL, params)
            resp.raise_for_status()
            data = resp.json()
            all_symbols.extend([c["ticker"] for c in (data.get("results") or []) if "ticker" in c])

            # Pagination
            next_url = data.get("next_url")
            while next_url:
                resp = _rate_limited_get(next_url, {"apiKey": api_key})
                resp.raise_for_status()
                data = resp.json()
                all_symbols.extend([c["ticker"] for c in (data.get("results") or []) if "ticker" in c])
                next_url = data.get("next_url")
                time.sleep(REQUEST_DELAY_SECONDS)

            time.sleep(REQUEST_DELAY_SECONDS)

        # De-dup and sort for determinism
        all_symbols = sorted(set(all_symbols))
        if not all_symbols:
            raise AirflowSkipException("No option symbols found for the provided underlyings.")
        return all_symbols

    @task
    def batch_option_symbols(symbol_list: List[str]) -> List[List[str]]:
        """Batch symbols for parallel processing to avoid mapping limits."""
        return [symbol_list[i : i + SYMBOL_BATCH_SIZE] for i in range(0, len(symbol_list), SYMBOL_BATCH_SIZE)]

    @task(retries=3, retry_delay=pendulum.duration(minutes=10), pool=API_POOL)
    def process_symbol_batch_backfill(symbol_batch: List[str]) -> List[str]:
        """
        Fetch historical aggregates for each symbol over the date range in params.
        Write one JSON file per (symbol, trading day) to s3://<bucket>/raw/options/<symbol>/<YYYY-MM-DD>.json
        """
        ctx = get_current_context()
        start_date = ctx["params"]["start_date"]
        end_date = ctx["params"]["end_date"]

        # Validate dates
        try:
            sd = pendulum.parse(start_date)
            ed = pendulum.parse(end_date)
        except Exception:
            raise ValueError("Invalid start_date or end_date. Expected format YYYY-MM-DD.")
        if sd > ed:
            raise ValueError("start_date cannot be after end_date.")

        api_key = _get_polygon_options_key()
        s3 = S3Hook()  # Option B: rely on default AWS creds (mounted ~/.aws), no Airflow connection lookup

        processed_keys: List[str] = []
        for option_symbol in symbol_batch:
            url = f"{POLYGON_AGGS_URL_BASE}/{option_symbol}/range/1/day/{start_date}/{end_date}"
            params = {"adjusted": "true", "sort": "asc", "apiKey": api_key}

            try:
                resp = _rate_limited_get(url, params)
                if resp.status_code == 404:
                    # No data for that range — skip quietly
                    time.sleep(REQUEST_DELAY_SECONDS)
                    continue
                resp.raise_for_status()
                data = resp.json()

                results = data.get("results") or []
                if results:
                    for bar in results:
                        trade_date = pendulum.from_timestamp(bar["t"] / 1000, tz="UTC").to_date_string()
                        payload = {
                            "ticker": data.get("ticker", option_symbol),
                            "resultsCount": 1,
                            "results": [bar],
                        }
                        # Write under raw/options/<symbol>/<YYYY-MM-DD>.json
                        s3_key = f"raw/options/{option_symbol}/{trade_date}.json"
                        s3.load_string(json.dumps(payload), key=s3_key, bucket_name=BUCKET_NAME, replace=True)
                        processed_keys.append(s3_key)

                time.sleep(REQUEST_DELAY_SECONDS)
            except requests.exceptions.HTTPError as e:
                status = e.response.status_code if e.response is not None else "unknown"
                print(f"[WARN] Could not process {option_symbol}. HTTP {status}. Skipping.")
            except Exception as e:
                print(f"[WARN] Unexpected error for {option_symbol}: {e}")

        return processed_keys

    @task
    def flatten_s3_key_list(nested_list: List[List[str]]) -> List[str]:
        """Flatten list of lists of S3 keys into a single list."""
        return [k for sub in (nested_list or []) for k in (sub or []) if k]

    @task(outlets=[S3_OPTIONS_MANIFEST_DATASET])
    def write_manifest_to_s3(s3_keys: List[str]):
        """Write the final manifest to S3 to trigger downstream load DAG."""
        keys = [k for k in (s3_keys or []) if k]
        if not keys:
            raise AirflowSkipException("No S3 keys were processed during the options backfill.")

        s3 = S3Hook()  # Option B
        manifest_key = "raw/manifests/polygon_options_manifest_latest.txt"
        s3.load_string("\n".join(keys), key=manifest_key, bucket_name=BUCKET_NAME, replace=True)
        print(f"Backfill manifest created with {len(keys)} keys at s3://{BUCKET_NAME}/{manifest_key}")

    # Flow
    all_symbols = get_all_option_symbols()
    symbol_batches = batch_option_symbols(all_symbols)
    processed_keys_nested = process_symbol_batch_backfill.expand(symbol_batch=symbol_batches)
    s3_keys_flat = flatten_s3_key_list(processed_keys_nested)
    write_manifest_to_s3(s3_keys_flat)

polygon_options_ingest_backfill_dag()
