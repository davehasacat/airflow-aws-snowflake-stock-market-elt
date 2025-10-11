from __future__ import annotations

import csv
import json
import os
import time
from datetime import timedelta
from typing import List, Optional

import pendulum
import requests
from airflow.decorators import dag, task, task_group
from airflow.exceptions import AirflowSkipException
from airflow.hooks.base import BaseHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

# ────────────────────────────────────────────────────────────────────────────────
# Config / Constants
# ────────────────────────────────────────────────────────────────────────────────
POLYGON_CONTRACTS_URL = "https://api.polygon.io/v3/reference/options/contracts"
POLYGON_AGGS_URL_BASE = "https://api.polygon.io/v2/aggs/ticker"
S3_PREFIX = "raw_data/options/"

# Centralized defaults (best practice)
default_args = {
    "owner": "data-platform",
    "retries": 3,
    "retry_delay": pendulum.duration(minutes=5),
}

# Optional tunables from env
REQUEST_TIMEOUT_SECS = int(os.getenv("HTTP_REQUEST_TIMEOUT_SECS", "60"))
API_POOL_NAME = os.getenv("API_POOL_NAME", "api_pool")
USER_AGENT = os.getenv("HTTP_USER_AGENT", "stocks-elt/polygon-options-dag (daily)")


@dag(
    dag_id="polygon_options_ingest_daily",
    description="Discover option contracts by underlying, fetch daily aggregates per contract, write to S3, and build a manifest.",
    start_date=pendulum.now(tz="UTC"),
    schedule="0 0 * * 1-5",  # weekdays
    catchup=True,
    default_args=default_args,
    dagrun_timeout=timedelta(hours=12),
    tags=["ingestion", "polygon", "options", "historical", "aws"],
    max_active_runs=1,  # avoid overlapping daily runs
)
def polygon_options_ingest_daily_dag():
    # Read once from env at parse time; connections are resolved inside tasks
    S3_CONN_ID = "aws_default"
    BUCKET_NAME = os.getenv("BUCKET_NAME")
    DBT_PROJECT_DIR = os.getenv("DBT_PROJECT_DIR", "/usr/local/airflow/dbt")
    API_CONN_ID = "polygon_options_api"

    # ── Helpers ────────────────────────────────────────────────────────────────
    def _get_polygon_api_key(conn_id: str, env_fallback: str) -> str:
        """
        Resolve the Polygon API key from an Airflow connection's 'password'.
        Falls back to an environment variable if the connection doesn't exist.
        """
        try:
            return BaseHook.get_connection(conn_id).password
        except Exception:
            key = os.getenv(env_fallback)
            if not key:
                raise RuntimeError(
                    f"Polygon API key not found. Define Airflow connection '{conn_id}' "
                    f"or set env var {env_fallback}"
                )
            return key

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

    # ── Inputs ────────────────────────────────────────────────────────────────
    @task
    def get_custom_tickers() -> List[str]:
        """Load custom underlyings from dbt seeds (idempotent)."""
        path = os.path.join(DBT_PROJECT_DIR, "seeds", "custom_tickers.csv")
        tickers: List[str] = []
        with open(path, mode="r") as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                if row.get("ticker"):
                    tickers.append(row["ticker"].strip().upper())
        return tickers

    @task
    def compute_target_date(execution_date: str) -> str:
        """
        Use previous calendar day; if you need true trading days,
        swap to an exchange calendar service.
        """
        return pendulum.parse(execution_date).subtract(days=1).to_date_string()

    # ── Contract Discovery ──────────────────────────────────────────────────────
    @task_group(group_id="discover_contracts")
    def tg_discover_contracts(custom_tickers: List[str], target_date: str):
        @task(retries=3, retry_delay=pendulum.duration(minutes=5), pool=API_POOL_NAME)
        def discover_for_ticker(ticker: str, target_date: str) -> List[str]:
            """
            Return all contract tickers for the underlying as of target_date.
            Uses pagination and robust retry.
            """
            api_key = _get_polygon_api_key(API_CONN_ID, "POLYGON_OPTIONS_API_KEY")

            params = {
                "underlying_ticker": ticker,
                "expiration_date.gte": target_date,
                "as_of": target_date,
                "limit": 1000,
                "apiKey": api_key,
            }

            contracts: List[str] = []
            # First page
            resp = _rate_limited_get(POLYGON_CONTRACTS_URL, params)
            resp.raise_for_status()
            data = resp.json()
            for r in data.get("results", []) or []:
                if "ticker" in r:
                    contracts.append(r["ticker"])

            # Pagination
            next_url = data.get("next_url")
            while next_url:
                # next_url may already include query params; we only ensure apiKey is present
                resp = _rate_limited_get(next_url, {"apiKey": api_key})
                resp.raise_for_status()
                data = resp.json()
                for r in data.get("results", []) or []:
                    if "ticker" in r:
                        contracts.append(r["ticker"])
                next_url = data.get("next_url")

            # De-dup and sort for determinism
            return sorted(set(contracts))

        @task
        def flatten(nested: List[List[str]]) -> List[str]:
            return [c for sub in (nested or []) for c in (sub or [])]

        discovered = discover_for_ticker.expand(
            ticker=custom_tickers,          # list -> mapped
            target_date=target_date,        # scalar -> broadcast
        )
        all_contracts = flatten(discovered)
        return all_contracts

    # ── Fetch Aggregates ───────────────────────────────────────────────────────
    @task_group(group_id="fetch_and_store")
    def tg_fetch_and_store(contracts: List[str], target_date: str):
        @task(retries=3, retry_delay=pendulum.duration(minutes=5), pool=API_POOL_NAME)
        def fetch_contract_bars(contract_ticker: str, target_date: str) -> Optional[str]:
            """
            Fetch a contract's daily bar on target_date and write to S3 (idempotent).
            Returns S3 key or None if no data.
            """
            api_key = _get_polygon_api_key(API_CONN_ID, "POLYGON_OPTIONS_API_KEY")
            s3_hook = S3Hook(aws_conn_id=S3_CONN_ID)

            url = f"{POLYGON_AGGS_URL_BASE}/{contract_ticker}/range/1/day/{target_date}/{target_date}"
            params = {"adjusted": "true", "apiKey": api_key}

            resp = _rate_limited_get(url, params)
            if resp.status_code == 404:
                return None
            resp.raise_for_status()
            data = resp.json()
            results = data.get("results") or []
            if not results:
                return None

            payload = json.dumps(data)
            s3_key = f"{S3_PREFIX}{contract_ticker}_{target_date}.json"
            s3_hook.load_string(
                string_data=payload,
                key=s3_key,
                bucket_name=BUCKET_NAME,
                replace=True,  # idempotent overwrite
            )
            return s3_key

        s3_keys = fetch_contract_bars.expand(
            contract_ticker=contracts,   # list -> mapped
            target_date=target_date,     # scalar -> broadcast
        )
        return s3_keys

    # ── Manifest ───────────────────────────────────────────────────────────────
    @task
    def create_manifest(s3_keys: List[Optional[str]]) -> None:
        """Collect keys, write manifest; skip if none."""
        keys = [k for k in (s3_keys or []) if k]
        if not keys:
            raise AirflowSkipException("No new files created; skipping manifest.")
        s3_hook = S3Hook(aws_conn_id=S3_CONN_ID)
        manifest_key = "manifests/polygon_options_manifest_latest.txt"
        s3_hook.load_string(
            string_data="\n".join(keys),
            key=manifest_key,
            bucket_name=BUCKET_NAME,
            replace=True,
        )

    # ── Wiring ─────────────────────────────────────────────────────────────────
    custom_tickers = get_custom_tickers()
    target_date = compute_target_date("{{ ds }}")

    all_contracts = tg_discover_contracts(custom_tickers, target_date)
    s3_keys = tg_fetch_and_store(all_contracts, target_date)
    create_manifest(s3_keys)


polygon_options_ingest_daily_dag()
