# dags/polygon/stocks/polygon_stocks_ingest_daily.py
# =============================================================================
# Polygon Stocks Ingest (Daily) — stream path aligned to dbt incremental strategy
# -----------------------------------------------------------------------------
# Enterprise/modern strategy:
#   • S3 layout (per-ticker, per-day, gz):
#       raw/stocks/<TICKER>/<YYYY-MM-DD>.json.gz
#   • Per-day manifest (immutable):
#       raw/manifests/stocks/YYYY-MM-DD/manifest.txt
#   • “Latest” manifest is a POINTER file (tiny & stable):
#       raw/manifests/polygon_stocks_manifest_latest.txt
#       → single line: POINTER=raw/manifests/stocks/YYYY-MM-DD/manifest.txt
#   • Dataset emit happens only after the pointer is atomically updated
#   • All S3 writes encrypted at rest (encrypt=True) with deterministic keys
#   • Previous-business-day targeting using Airflow’s logical date
#   • Connection-aware API key resolution + robust retries/backoff/throttling
#   • Output JSON includes minimal lineage fields to aid dbt incremental models:
#       as_of_date (YYYY-MM-DD), ingested_at_utc (ISO8601)
#
# dbt fit (incremental MERGE-friendly):
#   • Loader projects into RAW with lineage columns, append-only, idempotent
#   • dbt staging models can use `inserted_at` (loader-populated) or event time
#   • Daily stream updates trigger downstream loaders via Dataset subscription
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

# ────────────────────────────────────────────────────────────────────────────────
# Config / Constants
# ────────────────────────────────────────────────────────────────────────────────
POLYGON_GROUPED_URL = "https://api.polygon.io/v2/aggs/grouped/locale/us/market/stocks/{date}"

BUCKET_NAME = os.getenv("BUCKET_NAME")  # expected: 'stock-market-elt'
DBT_PROJECT_DIR = os.getenv("DBT_PROJECT_DIR", "/usr/local/airflow/dbt")

REQUEST_TIMEOUT_SECS = int(os.getenv("HTTP_REQUEST_TIMEOUT_SECS", "60"))
USER_AGENT = os.getenv("HTTP_USER_AGENT", "stocks-elt/polygon-stocks-daily (auto)")
API_POOL = os.getenv("API_POOL", "api_pool")

REQUEST_DELAY_SECONDS = float(os.getenv("POLYGON_REQUEST_DELAY_SECONDS", "0.25"))
REQUEST_JITTER_SECONDS = float(os.getenv("POLYGON_REQUEST_JITTER_SECONDS", "0.05"))  # kept for parity, not used directly
REPLACE_FILES = os.getenv("DAILY_REPLACE", "true").lower() == "true"

# Manifests: immutable per-day + mutable pointer
DAY_MANIFEST_PREFIX = os.getenv("STOCKS_DAY_MANIFEST_PREFIX", "raw/manifests/stocks")
LATEST_POINTER_KEY = os.getenv("STOCKS_MANIFEST_KEY", "raw/manifests/polygon_stocks_manifest_latest.txt")

if not BUCKET_NAME:
    raise RuntimeError("BUCKET_NAME env var is required (e.g., 'stock-market-elt').")

# Airflow defaults (enterprise-safe)
default_args = {
    "owner": "data-platform",
    "retries": 3,
    "retry_delay": pendulum.duration(minutes=5),
    "pool": API_POOL,  # all tasks inherit; heavy ones may still specify pool explicitly
}

# ────────────────────────────────────────────────────────────────────────────────
# Helpers
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
        x = conn.extra_dejson or {}
        for k in ("api_key", "key", "token", "password", "polygon_stocks_api_key", "value"):
            v = x.get(k)
            if isinstance(v, str) and v.strip():
                return v.strip()
    except Exception:
        pass
    env = os.getenv("POLYGON_STOCKS_API_KEY", "").strip()
    if env:
        return env
    raise RuntimeError(
        "Polygon Stocks API key not found. Define Airflow connection 'polygon_stocks_api_key' "
        "or set env POLYGON_STOCKS_API_KEY."
    )

def _session_with_auth(api_key: str) -> requests.Session:
    """HTTP session with retry + Bearer token auth."""
    s = requests.Session()
    retries = Retry(
        total=6,
        backoff_factor=1.2,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=frozenset(["GET"]),
        raise_on_status=False,
        respect_retry_after_header=True,  # honor vendor-provided backoff precisely
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

# ────────────────────────────────────────────────────────────────────────────────
# DAG
# ────────────────────────────────────────────────────────────────────────────────
@dag(
    dag_id="polygon_stocks_ingest_daily",
    description="Polygon stocks daily ingest — raw JSON.gz to S3 + immutable per-day manifest + POINTER latest.",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),  # static start; daily ops don’t use catchup
    schedule="0 0 * * 1-5",  # weekdays; previous-biz-day targeting inside
    catchup=False,            # backfills are handled by the dedicated backfill DAG
    default_args=default_args,
    dagrun_timeout=timedelta(hours=6),
    tags=["ingestion", "polygon", "stocks", "daily", "aws"],
    max_active_runs=1,
)
def polygon_stocks_ingest_daily_dag():

    # ───────────────────────────────
    # Inputs
    # ───────────────────────────────
    @task
    def get_custom_tickers() -> List[str]:
        """Read seed tickers to limit ingest scope (keeps costs/risk low)."""
        path = os.path.join(DBT_PROJECT_DIR, "seeds", "custom_tickers.csv")
        tickers: List[str] = []
        with open(path, mode="r", newline="") as csvfile:
            for row in csv.DictReader(csvfile):
                t = (row.get("ticker") or "").strip().upper()
                if t:
                    tickers.append(t)
        if not tickers:
            raise AirflowSkipException("No tickers found in dbt/seeds/custom_tickers.csv.")
        return tickers

    @task
    def compute_target_date(**context) -> str:
        """
        Target the previous business day (Fri for Sat/Sun).
        Later we can swap to an exchange/market calendar.
        """
        execution_date: str = context["ds"]
        return _previous_trading_day(execution_date)

    # ───────────────────────────────
    # Fetch grouped, split per-ticker, write raw
    # ───────────────────────────────
    @task(retries=3, retry_delay=pendulum.duration(minutes=10), pool=API_POOL)
    def get_grouped_and_write(custom_tickers: List[str], target_date: str) -> List[str]:
        """
        Fetch grouped daily bars for target_date:
          - Filter to your custom tickers,
          - Transform to the per-ticker JSON shape your loaders expect (results[0]),
          - Add minimal lineage (as_of_date, ingested_at_utc),
          - Write to s3://<bucket>/raw/stocks/<TICKER>/<YYYY-MM-DD>.json.gz (encrypted).
        Returns the list of S3 keys written.
        """
        api_key = _get_polygon_stocks_key()
        sess = _session_with_auth(api_key)
        s3 = S3Hook()
        custom = set(custom_tickers)

        url = POLYGON_GROUPED_URL.format(date=target_date)
        params = {"adjusted": "true"}

        resp = sess.get(url, params=params, timeout=REQUEST_TIMEOUT_SECS)
        if resp.status_code == 404:
            # Market holiday/closure → nothing to do
            return []
        if resp.status_code in (401, 403):
            # Auth/entitlement. Log context; continue to next day.
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

        run_ts = pendulum.now("UTC").to_iso8601_string()
        written: List[str] = []

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

                # minimal lineage for dbt/ops
                "as_of_date": target_date,
                "ingested_at_utc": run_ts,
            }

            s3_key = f"raw/stocks/{ticker}/{target_date}.json.gz"

            if not REPLACE_FILES and s3.check_for_key(s3_key, bucket_name=BUCKET_NAME):
                continue

            buf = BytesIO()
            with gzip.GzipFile(filename="", mode="wb", fileobj=buf) as gz:
                gz.write(json.dumps(payload, separators=(",", ":")).encode("utf-8"))

            s3.load_bytes(
                buf.getvalue(),
                key=s3_key,
                bucket_name=BUCKET_NAME,
                replace=True,
                encrypt=True,
            )
            written.append(s3_key)
            time.sleep(REQUEST_DELAY_SECONDS)

        return written

    # ───────────────────────────────
    # Manifest writing (immutable per-day + hardened POINTER update)
    # ───────────────────────────────
    @task
    def write_day_manifest(s3_keys: List[str], target_date: str) -> str | None:
        """
        Write an immutable per-day manifest listing all S3 keys written for the day.
        Stored at: raw/manifests/stocks/YYYY-MM-DD/manifest.txt
        """
        if not s3_keys:
            return None
        s3 = S3Hook()
        manifest_key = f"{DAY_MANIFEST_PREFIX}/{target_date}/manifest.txt"
        body = "\n".join(sorted(set(s3_keys))) + "\n"
        s3.load_string(
            body,
            key=manifest_key,
            bucket_name=BUCKET_NAME,
            replace=True,
            encrypt=True,
        )
        return manifest_key

    @task(outlets=[S3_STOCKS_MANIFEST_DATASET])
    def update_latest_pointer(day_manifest_key: str | None) -> str | None:
        """
        Atomically point 'latest' to the immutable per-day manifest via a POINTER file.
        This is what the stream loader subscribes to.
        Includes a content check to avoid emitting events for empty manifests.
        """
        if not day_manifest_key:
            raise AirflowSkipException("No per-day manifest; skipping latest pointer.")
        s3 = S3Hook()
        body = s3.read_key(key=day_manifest_key, bucket_name=BUCKET_NAME) or ""
        if not body.strip():
            raise AirflowSkipException(f"Per-day manifest is empty: {day_manifest_key}")
        pointer_body = f"POINTER={day_manifest_key}\n"
        s3.load_string(
            pointer_body,
            key=LATEST_POINTER_KEY,
            bucket_name=BUCKET_NAME,
            replace=True,
            encrypt=True,
        )
        return LATEST_POINTER_KEY

    # ───────────────────────────────
    # Wiring
    # ───────────────────────────────
    custom_tickers = get_custom_tickers()
    target_date = compute_target_date()
    keys = get_grouped_and_write(custom_tickers, target_date)
    day_manifest = write_day_manifest(keys, target_date)
    _ = update_latest_pointer(day_manifest)

polygon_stocks_ingest_daily_dag()
