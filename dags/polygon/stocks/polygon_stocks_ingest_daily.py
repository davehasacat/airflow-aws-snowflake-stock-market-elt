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
# Config
# ────────────────────────────────────────────────────────────────────────────────
POLYGON_GROUPED_URL = "https://api.polygon.io/v2/aggs/grouped/locale/us/market/stocks/{date}"

BUCKET_NAME = os.getenv("BUCKET_NAME")  # expected: stock-market-elt
DBT_PROJECT_DIR = os.getenv("DBT_PROJECT_DIR", "/usr/local/airflow/dbt")
USER_AGENT = os.getenv("HTTP_USER_AGENT", "stocks-elt/polygon-stocks-daily (auto)")
API_POOL = os.getenv("API_POOL", "api_pool")
REQUEST_TIMEOUT_SECS = int(os.getenv("HTTP_REQUEST_TIMEOUT_SECS", "60"))
REQUEST_DELAY_SECONDS = float(os.getenv("POLYGON_REQUEST_DELAY_SECONDS", "0.25"))

# Manifests: loader supports both flat and pointer; we’ll write pointer for atomicity
LATEST_MANIFEST_KEY = os.getenv("STOCKS_MANIFEST_KEY", "raw/manifests/manifest_latest.txt")
DAILY_MANIFEST_PREFIX = os.getenv("STOCKS_DAILY_MANIFEST_PREFIX", "raw/manifests/stocks/daily")


# ────────────────────────────────────────────────────────────────────────────────
# Helpers
# ────────────────────────────────────────────────────────────────────────────────
def _get_polygon_stocks_key() -> str:
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
    """Given the schedule date (ds), return the previous weekday (skip Sat/Sun)."""
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
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    schedule="0 0 * * 1-5",  # Mon–Fri at 00:00 UTC; targets previous trading day
    catchup=True,
    tags=["ingestion", "polygon", "daily", "aws", "stocks"],
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=4),
)
def polygon_stocks_ingest_daily_dag():
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
                replace=True,
            )
            processed.append(s3_key)
            time.sleep(REQUEST_DELAY_SECONDS)  # polite pacing

        return processed

    @task(outlets=[S3_STOCKS_MANIFEST_DATASET])
    def write_pointer_manifest(s3_keys: list[str], **context):
        """
        Write a timestamped manifest containing the just-written keys, then atomically
        update manifest_latest.txt with a POINTER=<key>. This keeps daily and backfills
        isolated and lets the loader ingest exactly this batch.
        """
        if not s3_keys:
            raise AirflowSkipException("No S3 keys were processed.")

        s3 = S3Hook()
        ts = pendulum.now("UTC").format("YYYY-MM-DDTHH-mm-ss")
        pointed_key = f"{DAILY_MANIFEST_PREFIX}/manifest_{ts}.txt"

        # 1) Write the concrete manifest
        s3.load_string(
            string_data="\n".join(s3_keys),
            key=pointed_key,
            bucket_name=BUCKET_NAME,
            replace=True,
        )
        # 2) Point latest → concrete manifest
        s3.load_string(
            string_data=f"POINTER={pointed_key}\n",
            key=LATEST_MANIFEST_KEY,
            bucket_name=BUCKET_NAME,
            replace=True,
        )
        print(
            f"Updated manifest pointer: s3://{BUCKET_NAME}/{LATEST_MANIFEST_KEY} "
            f"→ {pointed_key} (files: {len(s3_keys)})"
        )

    custom_tickers = get_custom_tickers()
    s3_keys = get_grouped_daily_data_and_split(custom_tickers)
    write_pointer_manifest(s3_keys)


polygon_stocks_ingest_daily_dag()
