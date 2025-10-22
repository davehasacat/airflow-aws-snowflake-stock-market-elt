# dags/polygon/stocks/polygon_stocks_ingest_daily.py
# =============================================================================
# Polygon Stocks → S3 (Daily) — Connection-aware key lookup + header auth
# Incremental-friendly: gzip output + audit/lineage fields at root.
#
# Enterprise/modern updates:
#   • S3 layout (Hive-style): raw/stocks/year=YYYY/month=MM/day=DD/ticker=<T>.json.gz
#   • Per-day manifest (immutable): raw/manifests/stocks/YYYY-MM-DD/manifest.txt
#   • “Latest” manifest is a POINTER file (not a copy of the manifest contents):
#         raw/manifests/polygon_stocks_manifest_latest.txt
#         → contains a single line: POINTER=raw/manifests/stocks/YYYY-MM-DD/manifest.txt
#   • Dataset emit happens only after the pointer is atomically updated
#   • All S3 writes encrypted at rest (encrypt=True)
#   • Static start_date, catchup=False, and pool inheritance for uniform throttling
# =============================================================================
from __future__ import annotations
import os
import json
import csv
import time
import gzip
from io import BytesIO
from datetime import timedelta
from typing import List

import pendulum
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from airflow.decorators import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.exceptions import AirflowSkipException
from airflow.hooks.base import BaseHook

from dags.utils.polygon_datasets import S3_STOCKS_MANIFEST_DATASET

POLYGON_GROUPED_URL = "https://api.polygon.io/v2/aggs/grouped/locale/us/market/stocks/{date}"

# ────────────────────────────────────────────────────────────────────────────────
# Config / Constants
# ────────────────────────────────────────────────────────────────────────────────
BUCKET_NAME = os.getenv("BUCKET_NAME")
DBT_PROJECT_DIR = os.getenv("DBT_PROJECT_DIR", "/usr/local/airflow/dbt")
USER_AGENT = os.getenv("HTTP_USER_AGENT", "stocks-elt/polygon-stocks-daily (auto)")
API_POOL = os.getenv("API_POOL", "api_pool")
REQUEST_TIMEOUT_SECS = int(os.getenv("HTTP_REQUEST_TIMEOUT_SECS", "60"))
REQUEST_DELAY_SECONDS = float(os.getenv("POLYGON_REQUEST_DELAY_SECONDS", "0.25"))
REQUEST_JITTER_SECONDS = float(os.getenv("POLYGON_REQUEST_JITTER_SECONDS", "0.05"))

# Manifests: immutable per-day + mutable pointer
DAY_MANIFEST_PREFIX = os.getenv("STOCKS_DAY_MANIFEST_PREFIX", "raw/manifests/stocks")
LATEST_POINTER_KEY  = os.getenv("STOCKS_LATEST_POINTER_KEY", "raw/manifests/polygon_stocks_manifest_latest.txt")

# ────────────────────────────────────────────────────────────────────────────────
# Auth helpers (connection-first; env fallback)
# ────────────────────────────────────────────────────────────────────────────────
def _get_polygon_stocks_key() -> str:
    """
    Retrieve Polygon Stocks API key from Airflow Connection 'polygon_stocks_api_key'
    (password/login/extras.api_key), else env POLYGON_STOCKS_API_KEY.
    """
    try:
        conn = BaseHook.get_connection("polygon_stocks_api_key")
        for candidate in (conn.password, conn.login):
            if isinstance(candidate, str) and candidate.strip():
                return candidate.strip()
        extra = (conn.extra_dejson or {})
        for k in ("api_key", "key", "token", "password", "polygon_stocks_api_key", "value"):
            v = extra.get(k)
            if isinstance(v, str) and v.strip():
                return v.strip()
    except Exception:
        pass

    env = os.getenv("POLYGON_STOCKS_API_KEY", "").strip()
    if env:
        return env
    raise RuntimeError(
        "Missing Polygon API key. Provide one via either:\n"
        "- Connection: airflow/connections/polygon_stocks_api_key (password/login/extra.api_key), or\n"
        "- Env:        POLYGON_STOCKS_API_KEY"
    )

def _session_with_auth(api_key: str) -> requests.Session:
    """HTTP session with retry + Bearer token auth (apiKey not required for grouped endpoint)."""
    s = requests.Session()
    retries = Retry(
        total=6,
        backoff_factor=1.2,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=frozenset(["GET"]),
        raise_on_status=False,
        respect_retry_after_header=True,
    )
    s.mount("https://", HTTPAdapter(max_retries=retries, pool_connections=64, pool_maxsize=64))
    s.headers.update({
        "User-Agent": USER_AGENT,
        "Authorization": f"Bearer {api_key}",
    })
    return s

def _previous_trading_day_from_logical(logical: pendulum.DateTime) -> str:
    """Previous business day (Fri for Sat/Sun); holidays handled by empty manifest skip."""
    d = logical.subtract(days=1)
    while d.day_of_week in (5, 6):  # 5=Sat, 6=Sun
        d = d.subtract(days=1)
    return d.to_date_string()

# ────────────────────────────────────────────────────────────────────────────────
# DAG
# ────────────────────────────────────────────────────────────────────────────────
default_args = {
    "owner": "data-platform",
    "retries": 3,
    "retry_delay": pendulum.duration(minutes=5),
    "pool": API_POOL,  # inherit API pool across tasks
}

@dag(
    dag_id="polygon_stocks_ingest_daily",
    description="Polygon stocks daily ingest — Hive-layout raw JSON.gz + per-day & latest manifests (pointer).",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),  # static; daily ops don't use catchup
    schedule="0 0 * * 1-5",   # weekdays; previous trading day logic inside task
    catchup=False,
    tags=["ingestion", "polygon", "daily", "aws", "stocks"],
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=4),
    default_args=default_args,
)
def polygon_stocks_ingest_daily_dag():
    if not BUCKET_NAME:
        raise RuntimeError("BUCKET_NAME env var is required (e.g., 'stock-market-elt').")

    # ───────────────────────────────
    # Inputs
    # ───────────────────────────────
    @task
    def get_custom_tickers() -> list[str]:
        """Read seed tickers from dbt seeds."""
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

    # ───────────────────────────────
    # Fetch grouped bars and write Hive-partitioned objects
    # ───────────────────────────────
    @task(retries=3, retry_delay=pendulum.duration(minutes=10))
    def get_grouped_daily_data_and_split(custom_tickers: list[str], **context) -> list[str]:
        """
        Calls Polygon grouped bars for the previous business day, filters to seed tickers,
        writes one gz JSON per {ticker,day} under:
            raw/stocks/year=YYYY/month=MM/day=DD/ticker=<T>.json.gz
        Returns the list of written S3 keys.
        """
        s3 = S3Hook()
        api_key = _get_polygon_stocks_key()
        sess = _session_with_auth(api_key)
        custom = set(custom_tickers)

        logical: pendulum.DateTime = context["logical_date"]
        target_date = _previous_trading_day_from_logical(logical)

        url = POLYGON_GROUPED_URL.format(date=target_date)
        params = {"adjusted": "true"}

        resp = sess.get(url, params=params, timeout=REQUEST_TIMEOUT_SECS)
        if resp.status_code == 404:
            return []
        if resp.status_code in (401, 403):
            # log and skip gracefully
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

        # Audit/lineage values for dbt
        run_id = context["run_id"]
        now_utc = pendulum.now("UTC").to_iso8601_string()

        yyyy, mm, dd = target_date.split("-")
        processed: list[str] = []

        for r in results:
            ticker = (r.get("T") or "").upper()
            if not ticker or ticker not in custom:
                continue

            payload = {
                # business keys (unchanged so loader SQL keeps working)
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

                # lineage/audit fields (helpful for dbt incremental + troubleshooting)
                "ingested_at_utc": now_utc,
                "as_of_date": target_date,
                "run_id": run_id,
            }

            # Write as gzip (encrypted)
            buf = BytesIO()
            with gzip.GzipFile(filename="", mode="wb", fileobj=buf) as gz:
                gz.write(json.dumps(payload, separators=(",", ":")).encode("utf-8"))
            s3_key = f"raw/stocks/year={yyyy}/month={mm}/day={dd}/ticker={ticker}.json.gz"
            s3.load_bytes(buf.getvalue(), key=s3_key, bucket_name=BUCKET_NAME, replace=True, encrypt=True)
            processed.append(s3_key)
            # light client-side throttle
            time.sleep(REQUEST_DELAY_SECONDS + (REQUEST_JITTER_SECONDS or 0.0))

        return processed

    # ───────────────────────────────
    # Manifest writing (immutable per-day + mutable pointer)
    # ───────────────────────────────
    @task
    def write_day_manifest(s3_keys: list[str], **context) -> str | None:
        """
        Write an immutable per-day manifest listing all S3 keys written for the day.
        Stored at: raw/manifests/stocks/YYYY-MM-DD/manifest.txt
        """
        if not s3_keys:
            return None
        logical: pendulum.DateTime = context["logical_date"]
        target_date = _previous_trading_day_from_logical(logical)
        s3 = S3Hook()
        manifest_key = f"{DAY_MANIFEST_PREFIX}/{target_date}/manifest.txt"
        body = "\n".join(sorted(set(s3_keys))) + "\n"
        s3.load_string(body, key=manifest_key, bucket_name=BUCKET_NAME, replace=True, encrypt=True)
        return manifest_key

    @task(outlets=[S3_STOCKS_MANIFEST_DATASET])
    def update_latest_pointer(day_manifest_key: str | None) -> str | None:
        """
        Atomically point 'latest' to the immutable per-day manifest via a POINTER file.
        This is what the loader subscribes to.
        """
        if not day_manifest_key:
            raise AirflowSkipException("No per-day manifest; skipping latest pointer.")
        s3 = S3Hook()
        pointer_body = f"POINTER={day_manifest_key}\n"
        s3.load_string(pointer_body, key=LATEST_POINTER_KEY, bucket_name=BUCKET_NAME, replace=True, encrypt=True)
        return LATEST_POINTER_KEY

    # ───────────────────────────────
    # Wiring
    # ───────────────────────────────
    custom_tickers = get_custom_tickers()
    s3_keys = get_grouped_daily_data_and_split(custom_tickers)
    day_manifest = write_day_manifest(s3_keys)
    _ = update_latest_pointer(day_manifest)

polygon_stocks_ingest_daily_dag()
