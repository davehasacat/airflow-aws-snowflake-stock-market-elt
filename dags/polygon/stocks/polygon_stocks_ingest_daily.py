# dags/polygon/stocks/polygon_stocks_ingest_daily.py
# =============================================================================
# Polygon Stocks → S3 (Daily)
# -----------------------------------------------------------------------------
# Purpose:
#   On each weekday run, fetch the previous trading day's grouped OHLCV bars
#   from Polygon, filter to your curated ticker list (dbt/seeds/custom_tickers.csv),
#   write one JSON per (ticker, date) to S3, and publish a POINTER manifest
#   that your loader consumes.
#
# Outputs:
#   - raw/stocks/<TICKER>/<YYYY-MM-DD>.json
#   - raw/manifests/stocks/daily/manifest_<timestamp>.txt    (concrete)
#   - raw/manifests/manifest_latest.txt                      (POINTER=<concrete>)
#
# Notes:
#   - Uses a pooled HTTP session and sensible retry/backoff.
#   - Weekend-aware (targets previous trading day). Market holidays (404) are no-ops.
#   - API key is resolved from Airflow Variable backed by AWS Secrets Manager,
#     with an env-var fallback for local use.
# =============================================================================

from __future__ import annotations
import os
import json
import csv
import time
from datetime import timedelta
from typing import List

import pendulum
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from airflow.decorators import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.exceptions import AirflowSkipException
from airflow.models import Variable  # read secrets from AWS SM via Airflow Variables

from dags.utils.polygon_datasets import S3_STOCKS_MANIFEST_DATASET


# ────────────────────────────────────────────────────────────────────────────────
# Config (env-first; fail-fast on required BUCKET_NAME inside the DAG)
# ────────────────────────────────────────────────────────────────────────────────
POLYGON_GROUPED_URL = "https://api.polygon.io/v2/aggs/grouped/locale/us/market/stocks/{date}"

BUCKET_NAME = os.getenv("BUCKET_NAME")  # required: e.g., stock-market-elt
DBT_PROJECT_DIR = os.getenv("DBT_PROJECT_DIR", "/usr/local/airflow/dbt")
USER_AGENT = os.getenv("HTTP_USER_AGENT", "stocks-elt/polygon-stocks-daily (auto)")
API_POOL = os.getenv("API_POOL", "api_pool")
REQUEST_TIMEOUT_SECS = int(os.getenv("HTTP_REQUEST_TIMEOUT_SECS", "60"))
REQUEST_DELAY_SECONDS = float(os.getenv("POLYGON_REQUEST_DELAY_SECONDS", "0.25"))

# Manifests: loader supports flat + pointer; daily writes POINTER for atomicity
LATEST_MANIFEST_KEY = os.getenv("STOCKS_MANIFEST_KEY", "raw/manifests/manifest_latest.txt")
DAILY_MANIFEST_PREFIX = os.getenv("STOCKS_DAILY_MANIFEST_PREFIX", "raw/manifests/stocks/daily")


# ────────────────────────────────────────────────────────────────────────────────
# Helpers (API key resolution, session, prev-trading-day)
# ────────────────────────────────────────────────────────────────────────────────
def _get_polygon_stocks_key() -> str:
    """
    Resolve Polygon Stocks API key from:
      1) Airflow Variable 'polygon_stocks_api_key' (plain string OR JSON object)
      2) Env POLYGON_STOCKS_API_KEY (fallback)
    """
    # 1) JSON variable
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
    # 2) Plain text (or JSON-ish string)
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


def _previous_trading_day(iso_date_str: str) -> str:
    """
    Given schedule date (ds), return the prior **weekday**:
    - Subtract one calendar day.
    - If Sat/Sun, roll back to Friday.
    Market holidays are handled downstream by 404 from Polygon.
    """
    d = pendulum.parse(iso_date_str)
    d = d.subtract(days=1)
    while d.day_of_week in (5, 6):  # Sat=5, Sun=6
        d = d.subtract(days=1)
    return d.to_date_string()


# ────────────────────────────────────────────────────────────────────────────────
# DAG
# ────────────────────────────────────────────────────────────────────────────────
@dag(
    dag_id="polygon_stocks_ingest_daily",
    start_date=pendulum.now(tz="UTC"),
    schedule="0 0 * * 1-5",  # Mon–Fri at 00:00 UTC; targets previous trading day
    catchup=True,            # backfill-friendly (uses logical_date inside task)
    tags=["ingestion", "polygon", "daily", "aws", "stocks"],
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=4),
)
def polygon_stocks_ingest_daily_dag():
    # Guardrail: the bucket is foundational to all writes
    if not BUCKET_NAME:
        raise RuntimeError("BUCKET_NAME env var is required (e.g., 'stock-market-elt').")

    @task
    def get_custom_tickers() -> list[str]:
        """Load your curated ticker list from the dbt seed."""
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

    @task(retries=3, retry_delay=pendulum.duration(minutes=10), pool=API_POOL)
    def get_grouped_daily_data_and_split(custom_tickers: list[str], **context) -> list[str]:
        """
        Pull Polygon grouped bars for the previous trading day, split into per-ticker JSON,
        write to S3, and return the list of written S3 keys.
        """
        sess = _session()
        s3 = S3Hook()
        api_key = _get_polygon_stocks_key()
        custom = set(custom_tickers)

        execution_date: str = context["ds"]
        target_date = _previous_trading_day(execution_date)

        url = POLYGON_GROUPED_URL.format(date=target_date)
        params = {"adjusted": "true", "apiKey": api_key}

        resp = sess.get(url, params=params, timeout=REQUEST_TIMEOUT_SECS)
        if resp.status_code == 404:
            # Market holiday: nothing to write, skip gracefully
            return []
        if resp.status_code in (401, 403):
            # Auth/entitlement issues: log context and continue to next run
            try:
                j = resp.json()
                msg = j.get("error") or j.get("message") or str(j)
            except Exception:
                msg = resp.text
            print(f"⚠️  {target_date} -> {resp.status_code}: {msg[:200]}")
            return []
        resp.raise_for_status()

        data = resp.json() or {}
        results = data.get("results") or []
        if not results:
            return []

        processed: list[str] = []
        for r in results:
            ticker = (r.get("T") or "").upper()
            if not ticker or ticker not in custom:
                continue

            # Normalize single-bar shape to match loader expectations
            payload = {
                "ticker": ticker,
                "queryCount": 1,
                "resultsCount": 1,
                "adjusted": True,
                "results": [{
                    "v": r.get("v"),
                    "vw": r.get("vw"),
                    "o": r.get("o"),
                    "c": r.get("c"),
                    "h": r.get("h"),
                    "l": r.get("l"),
                    "t": r.get("t"),
                    "n": r.get("n"),
                }],
                "status": "OK",
                "request_id": data.get("request_id"),
            }

            s3_key = f"raw/stocks/{ticker}/{target_date}.json"
            s3.load_string(
                string_data=json.dumps(payload, separators=(",", ":")),
                key=s3_key,
                bucket_name=BUCKET_NAME,
                replace=True,  # idempotent daily writes
            )
            processed.append(s3_key)

            # Gentle pacing to play nice with the API and S3
            time.sleep(REQUEST_DELAY_SECONDS)

        return processed

    @task(outlets=[S3_STOCKS_MANIFEST_DATASET])
    def write_pointer_manifest(s3_keys: list[str], **context):
        """
        Write a timestamped "concrete" manifest containing exactly the keys from
        this run, then atomically update manifest_latest.txt with:
          POINTER=<concrete_manifest_key>
        This keeps daily runs isolated, easy to replay, and safe for loaders.
        """
        if not s3_keys:
            raise AirflowSkipException("No S3 keys were processed.")

        s3 = S3Hook()
        ts = pendulum.now("UTC").format("YYYY-MM-DDTHH-mm-ss")
        pointed_key = f"{DAILY_MANIFEST_PREFIX}/manifest_{ts}.txt"

        # 1) Concrete manifest for this run
        s3.load_string(
            string_data="\n".join(s3_keys),
            key=pointed_key,
            bucket_name=BUCKET_NAME,
            replace=True,
        )

        # 2) Update the stable pointer
        s3.load_string(
            string_data=f"POINTER={pointed_key}\n",
            key=LATEST_MANIFEST_KEY,
            bucket_name=BUCKET_NAME,
            replace=True,
        )
        print(
            f"✅ Updated manifest pointer: s3://{BUCKET_NAME}/{LATEST_MANIFEST_KEY} "
            f"→ {pointed_key} (files: {len(s3_keys)})"
        )

    # ────────────────────────────────────────────────────────────────────────────
    # Wiring
    # ────────────────────────────────────────────────────────────────────────────
    custom_tickers = get_custom_tickers()
    s3_keys = get_grouped_daily_data_and_split(custom_tickers)
    write_pointer_manifest(s3_keys)


# Instantiate the DAG
polygon_stocks_ingest_daily_dag()
