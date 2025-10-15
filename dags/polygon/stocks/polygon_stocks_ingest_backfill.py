from __future__ import annotations
import os
import json
import csv
import time
from typing import List

import pendulum
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from datetime import timedelta

from airflow.decorators import dag, task
from airflow.exceptions import AirflowSkipException
from airflow.models.param import Param
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models import Variable  # read from Secrets Manager via Variables

from dags.utils.polygon_datasets import S3_STOCKS_MANIFEST_DATASET


# ────────────────────────────────────────────────────────────────────────────────
# Config / Constants
# ────────────────────────────────────────────────────────────────────────────────
POLYGON_GROUPED_URL = "https://api.polygon.io/v2/aggs/grouped/locale/us/market/stocks/{date}"

BUCKET_NAME = os.getenv("BUCKET_NAME")  # expected: 'stock-market-elt'
DBT_PROJECT_DIR = os.getenv("DBT_PROJECT_DIR", "/usr/local/airflow/dbt")

# Networking & rate limits
REQUEST_TIMEOUT_SECS = int(os.getenv("HTTP_REQUEST_TIMEOUT_SECS", "60"))
REQUEST_DELAY_SECONDS = float(os.getenv("POLYGON_REQUEST_DELAY_SECONDS", "0.25"))
USER_AGENT = os.getenv("HTTP_USER_AGENT", "stocks-elt/polygon-stocks-backfill (manual)")
API_POOL = os.getenv("API_POOL", "api_pool")  # ensure this pool exists (airflow_settings.yaml)

# Toggle idempotent overwrites (for true backfills you usually want overwrite)
REPLACE_FILES = os.getenv("BACKFILL_REPLACE", "true").lower() == "true"


# ────────────────────────────────────────────────────────────────────────────────
# Helpers
# ────────────────────────────────────────────────────────────────────────────────
def _get_polygon_stocks_key() -> str:
    """
    Return a clean Polygon Stocks API key whether the Airflow Variable is stored as:
      - plain text: "abc123"
      - JSON object: {"polygon_stocks_api_key":"abc123"} (console defaults sometimes do this)
    Falls back to env POLYGON_STOCKS_API_KEY.
    """
    # 1) Try JSON variable
    try:
        v = Variable.get("polygon_stocks_api_key", deserialize_json=True)
        if isinstance(v, dict):
            for k in ("polygon_stocks_api_key", "api_key", "key", "value"):
                s = v.get(k)
                if isinstance(s, str) and s.strip():
                    return s.strip()
            if len(v) == 1:
                s = next(iter(v.values()))
                if isinstance(s, str) and s.strip():
                    return s.strip()
    except Exception:
        pass

    # 2) Try plain-text variable (or JSON-ish string)
    try:
        s = Variable.get("polygon_stocks_api_key")
        if s:
            s = s.strip()
            if s.startswith("{"):
                try:
                    obj = json.loads(s)
                    if isinstance(obj, dict):
                        for k in ("polygon_stocks_api_key", "api_key", "key", "value"):
                            v = obj.get(k)
                            if isinstance(v, str) and v.strip():
                                return v.strip()
                        if len(obj) == 1:
                            v = next(iter(obj.values()))
                            if isinstance(v, str) and v.strip():
                                return v.strip()
                except Exception:
                    # fall through to return s as-is
                    pass
            return s
    except Exception:
        pass

    # 3) Env fallback
    env = os.getenv("POLYGON_STOCKS_API_KEY", "").strip()
    if env:
        return env

    raise RuntimeError(
        "Missing Polygon API key. In AWS Secrets Manager set secret "
        "'airflow/variables/polygon_stocks_api_key' to the raw key string, "
        "or set POLYGON_STOCKS_API_KEY in the environment."
    )


def _session() -> requests.Session:
    """HTTP session with connection pooling + retries for resilience and speed."""
    s = requests.Session()
    retries = Retry(
        total=6,
        backoff_factor=1.2,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=frozenset(["GET"]),
        raise_on_status=False,
    )
    s.mount("https://", HTTPAdapter(max_retries=retries, pool_connections=32, pool_maxsize=32))
    s.headers.update({"User-Agent": USER_AGENT})
    return s


def _rate_limited_get(sess: requests.Session, url: str, params: dict | None, max_tries: int = 6) -> requests.Response:
    """
    Robust GET with exponential backoff for 429/5xx and 'Retry-After' support.
    """
    backoff = 1.5
    delay = 1.0
    tries = 0
    while True:
        resp = sess.get(url, params=params, timeout=REQUEST_TIMEOUT_SECS)
        if resp.status_code == 429:
            retry_after = resp.headers.get("Retry-After")
            sleep_s = float(retry_after) if retry_after else delay
            time.sleep(sleep_s)
            delay *= backoff
            tries += 1
        elif 500 <= resp.status_code < 600:
            if tries >= max_tries:
                resp.raise_for_status()
            time.sleep(delay)
            delay *= backoff
            tries += 1
        else:
            return resp


# ────────────────────────────────────────────────────────────────────────────────
# DAG
# ────────────────────────────────────────────────────────────────────────────────
@dag(
    dag_id="polygon_stocks_ingest_backfill",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["ingestion", "polygon", "stocks", "backfill", "aws"],
    params={
        "start_date": Param(default="2025-10-06", type="string", description="Backfill start date (YYYY-MM-DD)."),
        "end_date":   Param(default="2025-10-11", type="string", description="Backfill end date (YYYY-MM-DD)."),
    },
    dagrun_timeout=timedelta(hours=12),
    max_active_runs=1,
)
def polygon_stocks_ingest_backfill_dag():
    """
    Backfills daily grouped OHLCV from Polygon for a date range,
    filtering to the tickers in dbt/seeds/custom_tickers.csv.

    S3 writes:
      - raw/stocks/{TICKER}/{YYYY-MM-DD}.json
      - raw/manifests/manifest_latest.txt   (kept for compatibility)
    """

    # Guardrail: ensure bucket configured
    if not BUCKET_NAME:
        raise RuntimeError("BUCKET_NAME env var is required (e.g., 'stock-market-elt').")

    @task
    def get_custom_tickers() -> list[str]:
        path = os.path.join(DBT_PROJECT_DIR, "seeds", "custom_tickers.csv")
        tickers: list[str] = []
        with open(path, mode="r", newline="") as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                t = (row.get("ticker") or "").strip().upper()
                if t:
                    tickers.append(t)
        if not tickers:
            raise AirflowSkipException("No tickers found in dbt/seeds/custom_tickers.csv.")
        return tickers

    @task
    def generate_date_range(**kwargs) -> list[str]:
        start = pendulum.parse(kwargs["params"]["start_date"])
        end = pendulum.parse(kwargs["params"]["end_date"])
        if start > end:
            raise ValueError("start_date cannot be after end_date.")
        trading_dates: list[str] = []
        cur = start
        while cur <= end:
            # Skip weekends (Sat=5, Sun=6)
            if cur.day_of_week not in (5, 6):
                trading_dates.append(cur.to_date_string())
            cur = cur.add(days=1)
        if not trading_dates:
            raise AirflowSkipException("No trading dates in the given range.")
        return trading_dates

    @task(retries=3, retry_delay=pendulum.duration(minutes=10), pool=API_POOL)
    def process_date(target_date: str, custom_tickers: list[str]) -> list[str]:
        """
        Fetch grouped bars for the date, filter to your custom tickers, and write per-ticker JSON.
        Uses pooled session + polite backoff. Skips market holidays (404).
        """
        s3_hook = S3Hook()
        api_key = _get_polygon_stocks_key()
        custom_tickers_set = set(custom_tickers)

        sess = _session()
        url = POLYGON_GROUPED_URL.format(date=target_date)
        params = {"adjusted": "true", "apiKey": api_key}

        resp = _rate_limited_get(sess, url, params)
        if resp.status_code == 404:
            # Likely a market holiday/closure
            return []
        if resp.status_code in (401, 403):
            # Entitlement/auth issue — surface helpful context but don't kill the whole run
            try:
                j = resp.json()
                msg = j.get("error") or j.get("message") or str(j)
            except Exception:
                msg = resp.text
            print(f"⚠️  {target_date} -> {resp.status_code}: {msg[:200]}")
            return []
        resp.raise_for_status()

        data = resp.json()
        processed_s3_keys: list[str] = []
        for result in (data.get("results") or []):
            ticker = (result.get("T") or "").upper()
            if ticker and ticker in custom_tickers_set:
                formatted_result = {
                    "ticker": ticker,
                    "queryCount": 1,
                    "resultsCount": 1,
                    "adjusted": True,
                    "results": [{
                        "v": result.get("v"),
                        "vw": result.get("vw"),
                        "o": result.get("o"),
                        "c": result.get("c"),
                        "h": result.get("h"),
                        "l": result.get("l"),
                        "t": result.get("t"),
                        "n": result.get("n"),
                    }],
                    "status": "OK",
                    "request_id": data.get("request_id"),
                }
                s3_key = f"raw/stocks/{ticker}/{target_date}.json"
                if not REPLACE_FILES and s3_hook.check_for_key(s3_key, bucket_name=BUCKET_NAME):
                    continue
                s3_hook.load_string(
                    string_data=json.dumps(formatted_result, separators=(",", ":")),
                    key=s3_key,
                    bucket_name=BUCKET_NAME,
                    replace=True,
                )
                processed_s3_keys.append(s3_key)

                time.sleep(REQUEST_DELAY_SECONDS)

        return processed_s3_keys

    @task
    def flatten_s3_key_list(nested_list: list[list[str]]) -> list[str]:
        return [key for sub in (nested_list or []) for key in (sub or []) if key]

    @task(outlets=[S3_STOCKS_MANIFEST_DATASET])
    def write_manifest_to_s3(s3_keys: list[str]):
        if not s3_keys:
            raise AirflowSkipException("No S3 keys were processed during the backfill.")
        manifest_content = "\n".join(s3_keys)
        s3_hook = S3Hook()
        # Kept the original manifest path for compatibility with your downstream loader.
        manifest_key = "raw/manifests/manifest_latest.txt"
        s3_hook.load_string(string_data=manifest_content, key=manifest_key, bucket_name=BUCKET_NAME, replace=True)
        print(f"✅ Backfill manifest file created: s3://{BUCKET_NAME}/{manifest_key} (files: {len(s3_keys)})")

    # Flow
    custom_tickers = get_custom_tickers()
    date_range = generate_date_range()
    processed_nested = process_date.partial(custom_tickers=custom_tickers).expand(target_date=date_range)
    s3_keys_flat = flatten_s3_key_list(processed_nested)
    write_manifest_to_s3(s3_keys_flat)


polygon_stocks_ingest_backfill_dag()
