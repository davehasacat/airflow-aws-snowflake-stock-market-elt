# dags/polygon/options/polygon_options_ingest_backfill.py
from __future__ import annotations

import csv
import json
import os
import time
from typing import List, Optional

import pendulum
import requests
from datetime import timedelta

from airflow.decorators import dag, task
from airflow.exceptions import AirflowSkipException
from airflow.models.param import Param
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
API_POOL = "api_pool"  # ensure this pool exists (airflow_settings.yaml)

# Rate limiting / batching
REQUEST_DELAY_SECONDS = float(os.getenv("POLYGON_REQUEST_DELAY_SECONDS", "0.5"))
CONTRACTS_BATCH_SIZE = int(os.getenv("BACKFILL_CONTRACTS_BATCH_SIZE", "400"))  # cap mapped work inside a day

# ────────────────────────────────────────────────────────────────────────────────
# Helpers
# ────────────────────────────────────────────────────────────────────────────────
def _get_polygon_options_key() -> str:
    """
    Return a clean Polygon OPTIONS API key whether the Airflow Variable is stored as:
      - raw: "abc123"
      - JSON object: {"polygon_options_api_key":"abc123"} (console sometimes does this)
    Falls back to env POLYGON_OPTIONS_API_KEY.
    """
    # 1) JSON variable
    try:
        v = Variable.get("polygon_options_api_key", deserialize_json=True)
        if isinstance(v, dict):
            for k in ("polygon_options_api_key", "api_key", "key", "value"):
                s = v.get(k)
                if isinstance(s, str) and s.strip():
                    return s.strip()
            if len(v) == 1:
                s = next(iter(v.values()))
                if isinstance(s, str) and s.strip():
                    return s.strip()
    except Exception:
        pass
    # 2) Plain-text (or JSON-ish string)
    try:
        s = Variable.get("polygon_options_api_key")
        if s:
            s = s.strip()
            if s.startswith("{"):
                try:
                    obj = json.loads(s)
                    if isinstance(obj, dict):
                        for k in ("polygon_options_api_key", "api_key", "key", "value"):
                            v = obj.get(k)
                            if isinstance(v, str) and v.strip():
                                return v.strip()
                        if len(obj) == 1:
                            v = next(iter(obj.values()))
                            if isinstance(v, str) and v.strip():
                                return v.strip()
                except Exception:
                    pass
            return s
    except Exception:
        pass
    # 3) Env fallback
    env = os.getenv("POLYGON_OPTIONS_API_KEY", "").strip()
    if env:
        return env
    raise RuntimeError(
        "Polygon Options API key not found. Create secret 'airflow/variables/polygon_options_api_key' "
        "in AWS Secrets Manager (plain text), or set env POLYGON_OPTIONS_API_KEY."
    )

def _rate_limited_get(url: str, params: dict | None, max_tries: int = 6) -> requests.Response:
    """Robust GET with exponential backoff for 429/5xx; respects Retry-After."""
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

def _batch(lst: List[str], n: int) -> List[List[str]]:
    return [lst[i : i + n] for i in range(0, len(lst), n)]

# ────────────────────────────────────────────────────────────────────────────────
# DAG
# ────────────────────────────────────────────────────────────────────────────────
@dag(
    dag_id="polygon_options_ingest_backfill",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule=None,  # manual only
    catchup=False,
    tags=["ingestion", "polygon", "options", "backfill", "aws"],
    params={
        "start_date": Param(default="2025-10-06", type="string", description="Backfill start date (YYYY-MM-DD)"),
        "end_date":   Param(default="2025-10-09", type="string", description="Backfill end date (YYYY-MM-DD)"),
    },
    dagrun_timeout=timedelta(hours=24),
    max_active_runs=1,
)
def polygon_options_ingest_backfill_dag():
    """
    Backfill that mirrors the daily DAG:
      For each trading day D in [start_date, end_date]:
        - Discover contracts as_of D with expiration >= D
        - Batch contracts (limit mapped work inside the task)
        - For each contract, fetch daily bars for D and write JSON:
          s3://<bucket>/raw/options/<CONTRACT>/<D>.json
      Finally, write/replace manifest at:
          s3://<bucket>/raw/manifests/polygon_options_manifest_latest.txt
    """

    @task
    def read_underlyings() -> List[str]:
        path = os.path.join(DBT_PROJECT_DIR, "seeds", "custom_tickers.csv")
        tickers: List[str] = []
        with open(path, mode="r") as f:
            reader = csv.DictReader(f)
            for row in reader:
                t = (row.get("ticker") or "").strip().upper()
                if t:
                    tickers.append(t)
        if not tickers:
            raise AirflowSkipException("No underlyings found in seeds/custom_tickers.csv")
        return tickers

    @task
    def compute_trading_dates(**kwargs) -> List[str]:
        start = pendulum.parse(kwargs["params"]["start_date"])
        end = pendulum.parse(kwargs["params"]["end_date"])
        if start > end:
            raise ValueError("start_date cannot be after end_date.")
        dates: List[str] = []
        cur = start
        while cur <= end:
            if cur.day_of_week not in (5, 6):  # skip Sat/Sun
                dates.append(cur.to_date_string())
            cur = cur.add(days=1)
        if not dates:
            raise AirflowSkipException("No trading dates in the given range.")
        return dates

    @task(retries=3, retry_delay=pendulum.duration(minutes=5), pool=API_POOL)
    def process_trading_day(underlyings: List[str], target_date: str) -> List[str]:
        """
        For a single trading day:
          1) Discover contracts across all underlyings (as_of target_date, expiration_date>=target_date)
          2) Batch contracts
          3) Fetch daily bars for target_date and write to S3 under raw/options/<CONTRACT>/<DATE>.json
          4) Return list of written keys
        """
        api_key = _get_polygon_options_key()
        s3 = S3Hook()

        # 1) Discover all contracts for this date
        contracts: List[str] = []
        for ticker in underlyings:
            params = {
                "underlying_ticker": ticker,
                "expiration_date.gte": target_date,
                "as_of": target_date,
                "limit": 1000,
                "apiKey": api_key,
            }
            resp = _rate_limited_get(POLYGON_CONTRACTS_URL, params)
            resp.raise_for_status()
            data = resp.json()
            for r in data.get("results", []) or []:
                t = r.get("ticker")
                if t:
                    contracts.append(t)
            next_url = data.get("next_url")
            while next_url:
                resp = _rate_limited_get(next_url, {"apiKey": api_key})
                resp.raise_for_status()
                data = resp.json()
                for r in data.get("results", []) or []:
                    t = r.get("ticker")
                    if t:
                        contracts.append(t)
                next_url = data.get("next_url")
                time.sleep(REQUEST_DELAY_SECONDS)
            time.sleep(REQUEST_DELAY_SECONDS)

        # Dedupe + sort
        contracts = sorted(set(contracts))
        if not contracts:
            # No contracts for this day → nothing to write
            return []

        # 2) Batch contracts
        batches = _batch(contracts, CONTRACTS_BATCH_SIZE)

        # 3) Fetch bars per contract and write to S3
        written: List[str] = []
        for batch in batches:
            for contract in batch:
                url = f"{POLYGON_AGGS_URL_BASE}/{contract}/range/1/day/{target_date}/{target_date}"
                params = {"adjusted": "true", "apiKey": api_key}
                resp = _rate_limited_get(url, params)
                if resp.status_code == 404:
                    continue
                resp.raise_for_status()
                obj = resp.json()
                results = obj.get("results") or []
                if not results:
                    continue
                key = f"raw/options/{contract}/{target_date}.json"
                s3.load_string(json.dumps(obj), key=key, bucket_name=BUCKET_NAME, replace=True)
                written.append(key)
                time.sleep(REQUEST_DELAY_SECONDS)

        return written

    @task
    def flatten(list_of_lists: List[List[str]]) -> List[str]:
        return [k for sub in (list_of_lists or []) for k in (sub or []) if k]

    @task(outlets=[S3_OPTIONS_MANIFEST_DATASET])
    def write_manifest(all_keys: List[str]) -> None:
        if not all_keys:
            raise AirflowSkipException("No files written; skipping manifest.")
        s3 = S3Hook()
        manifest_key = "raw/manifests/polygon_options_manifest_latest.txt"
        s3.load_string("\n".join(all_keys), key=manifest_key, bucket_name=BUCKET_NAME, replace=True)
        print(f"✅ Manifest updated with {len(all_keys)} keys at s3://{BUCKET_NAME}/{manifest_key}")

    # ───────────────────────────────
    # Wiring
    # ───────────────────────────────
    underlyings = read_underlyings()
    trading_days = compute_trading_dates()
    # Map over dates (single mapped task per date, avoiding TaskGroup expansion)
    keys_per_day = process_trading_day.partial(underlyings=underlyings).expand(target_date=trading_days)
    all_keys = flatten(keys_per_day)
    write_manifest(all_keys)

polygon_options_ingest_backfill_dag()
