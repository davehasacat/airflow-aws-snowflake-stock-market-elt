# dags/polygon/stocks/polygon_stocks_ingest_daily.py
# =============================================================================
# Polygon Stocks → S3 (Daily) — Connection-aware key lookup + header auth
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
from airflow.models import Variable
from airflow.hooks.base import BaseHook  # NEW

from dags.utils.polygon_datasets import S3_STOCKS_MANIFEST_DATASET

POLYGON_GROUPED_URL = "https://api.polygon.io/v2/aggs/grouped/locale/us/market/stocks/{date}"

BUCKET_NAME = os.getenv("BUCKET_NAME")
DBT_PROJECT_DIR = os.getenv("DBT_PROJECT_DIR", "/usr/local/airflow/dbt")
USER_AGENT = os.getenv("HTTP_USER_AGENT", "stocks-elt/polygon-stocks-daily (auto)")
API_POOL = os.getenv("API_POOL", "api_pool")
REQUEST_TIMEOUT_SECS = int(os.getenv("HTTP_REQUEST_TIMEOUT_SECS", "60"))
REQUEST_DELAY_SECONDS = float(os.getenv("POLYGON_REQUEST_DELAY_SECONDS", "0.25"))

LATEST_MANIFEST_KEY = os.getenv("STOCKS_MANIFEST_KEY", "raw/manifests/manifest_latest.txt")
DAILY_MANIFEST_PREFIX = os.getenv("STOCKS_DAILY_MANIFEST_PREFIX", "raw/manifests/stocks/daily")


def _extract_api_key_from_conn(conn) -> str | None:
    # Priority: password, login, extra['api_key']
    if conn and conn.password and conn.password.strip():
        return conn.password.strip()
    if conn and conn.login and conn.login.strip():
        return conn.login.strip()
    try:
        extra = (conn.extra_dejson or {}) if conn else {}
        v = extra.get("api_key")
        if isinstance(v, str) and v.strip():
            return v.strip()
    except Exception:
        pass
    return None


def _get_polygon_stocks_key() -> str:
    # 1) Connection: airflow/connections/polygon_stocks_api_key
    try:
        conn = BaseHook.get_connection("polygon_stocks_api_key")
        s = _extract_api_key_from_conn(conn)
        if s:
            return s
    except Exception:
        pass

    # 2) Variable: airflow/variables/polygon_stocks_api_key
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
    try:
        s = Variable.get("polygon_stocks_api_key")
        if s:
            s = s.strip()
            if s.startswith("{"):
                try:
                    obj = json.loads(s)
                    if isinstance(obj, dict):
                        for k in ("polygon_stocks_api_key", "api_key", "key", "value"):
                            v2 = obj.get(k)
                            if isinstance(v2, str) and v2.strip():
                                return v2.strip()
                        if len(obj) == 1:
                            v2 = next(iter(obj.values()))
                            if isinstance(v2, str) and v2.strip():
                                return v2.strip()
                except Exception:
                    pass
            return s
    except Exception:
        pass

    # 3) Env
    env = os.getenv("POLYGON_STOCKS_API_KEY", "").strip()
    if env:
        return env

    raise RuntimeError(
        "Missing Polygon API key. Provide one via either:\n"
        "- Connection: airflow/connections/polygon_stocks_api_key (password/login/extra.api_key), or\n"
        "- Variable:   airflow/variables/polygon_stocks_api_key, or\n"
        "- Env:        POLYGON_STOCKS_API_KEY"
    )


def _session_with_auth(api_key: str) -> requests.Session:
    s = requests.Session()
    retries = Retry(
        total=6,
        backoff_factor=1.2,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=frozenset(["GET"]),
        raise_on_status=False,
        respect_retry_after_header=True,
    )
    s.mount("https://", HTTPAdapter(max_retries=retries, pool_connections=32, pool_maxsize=32))
    s.headers.update({
        "User-Agent": USER_AGENT,
        "Authorization": f"Bearer {api_key}",
    })
    return s


def _previous_trading_day(iso_date_str: str) -> str:
    d = pendulum.parse(iso_date_str).subtract(days=1)
    while d.day_of_week in (5, 6):  # Sat/Sun
        d = d.subtract(days=1)
    return d.to_date_string()


@dag(
    dag_id="polygon_stocks_ingest_daily",
    start_date=pendulum.now(tz="UTC"),
    schedule="0 0 * * 1-5",
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
            for row in csv.DictReader(csvfile):
                t = (row.get("ticker") or "").strip().upper()
                if t:
                    tickers.append(t)
        if not tickers:
            raise AirflowSkipException("No tickers found in dbt/seeds/custom_tickers.csv.")
        return tickers

    @task(retries=3, retry_delay=pendulum.duration(minutes=10), pool=API_POOL)
    def get_grouped_daily_data_and_split(custom_tickers: list[str], **context) -> list[str]:
        s3 = S3Hook()
        api_key = _get_polygon_stocks_key()
        sess = _session_with_auth(api_key)
        custom = set(custom_tickers)

        execution_date: str = context["ds"]
        target_date = _previous_trading_day(execution_date)

        url = POLYGON_GROUPED_URL.format(date=target_date)
        params = {"adjusted": "true"}

        resp = sess.get(url, params=params, timeout=REQUEST_TIMEOUT_SECS)
        if resp.status_code == 404:
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
            s3.load_string(json.dumps(payload, separators=(",", ":")), key=s3_key,
                           bucket_name=BUCKET_NAME, replace=True)
            processed.append(s3_key)
            time.sleep(REQUEST_DELAY_SECONDS)

        return processed

    @task(outlets=[S3_STOCKS_MANIFEST_DATASET])
    def write_pointer_manifest(s3_keys: list[str], **context):
        if not s3_keys:
            raise AirflowSkipException("No S3 keys were processed.")
        s3 = S3Hook()
        ts = pendulum.now("UTC").format("YYYY-MM-DDTHH-mm-ss")
        pointed_key = f"{DAILY_MANIFEST_PREFIX}/manifest_{ts}.txt"

        s3.load_string("\n".join(s3_keys), key=pointed_key, bucket_name=BUCKET_NAME, replace=True)
        s3.load_string(f"POINTER={pointed_key}\n", key=LATEST_MANIFEST_KEY,
                       bucket_name=BUCKET_NAME, replace=True)
        print(f"✅ Updated manifest pointer: s3://{BUCKET_NAME}/{LATEST_MANIFEST_KEY} → {pointed_key} (files: {len(s3_keys)})")

    custom_tickers = get_custom_tickers()
    s3_keys = get_grouped_daily_data_and_split(custom_tickers)
    write_pointer_manifest(s3_keys)

polygon_stocks_ingest_daily_dag()
