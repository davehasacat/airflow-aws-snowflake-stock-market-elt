# dags/polygon/stocks/polygon_stocks_ingest_backfill.py
# =============================================================================
# Polygon Stocks → S3 (Backfill)
# -----------------------------------------------------------------------------
# Purpose:
#   Backfill grouped daily OHLCV bars for an inclusive date range from Polygon,
#   filtered to tickers in dbt/seeds/custom_tickers.csv.
#
# S3 layout (Hive-style, gzip):
#   raw/stocks/year=YYYY/month=MM/day=DD/ticker=<T>.json.gz
#
# Manifests (immutable; per-day):
#   raw/manifests/stocks/YYYY-MM-DD/manifest.txt
#
# Important:
#   • This backfill DAG does NOT update the "latest" pointer (stream loader stays idle).
#   • API auth via Bearer header (connection-first, env fallback).
#   • Small XComs: workers return tiny {day:count} maps; manifests built via S3 listing.
#   • Encrypted S3 writes by default (encrypt=True).
#   • Weekends skipped; market holidays appear as 404/empty and produce no manifest.
# =============================================================================

from __future__ import annotations
import os
import csv
import json
import time
import gzip
from io import BytesIO
from typing import List, Dict
from datetime import timedelta

import pendulum
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from airflow.decorators import dag, task
from airflow.exceptions import AirflowSkipException
from airflow.models.param import Param
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.hooks.base import BaseHook

# ────────────────────────────────────────────────────────────────────────────────
# Config / Constants (env-first with sensible defaults)
# ────────────────────────────────────────────────────────────────────────────────
POLYGON_GROUPED_URL = "https://api.polygon.io/v2/aggs/grouped/locale/us/market/stocks/{date}"

BUCKET_NAME = os.getenv("BUCKET_NAME")  # expected: 'stock-market-elt'
DBT_PROJECT_DIR = os.getenv("DBT_PROJECT_DIR", "/usr/local/airflow/dbt")

# Networking & rate limits
REQUEST_TIMEOUT_SECS = int(os.getenv("HTTP_REQUEST_TIMEOUT_SECS", "60"))
USER_AGENT = os.getenv("HTTP_USER_AGENT", "stocks-elt/polygon-stocks-backfill (manual)")
API_POOL = os.getenv("API_POOL", "api_pool")  # ensure the pool exists
REQUEST_DELAY_SECONDS = float(os.getenv("POLYGON_REQUEST_DELAY_SECONDS", "0.25"))
REQUEST_JITTER_SECONDS = float(os.getenv("POLYGON_REQUEST_JITTER_SECONDS", "0.05"))

# Batch/map sizing (kept simple for grouped endpoint)
MAP_CHUNK_SIZE = int(os.getenv("BACKFILL_MAP_CHUNK_SIZE", "2000"))

# Manifests
DAY_MANIFEST_PREFIX = os.getenv("STOCKS_DAY_MANIFEST_PREFIX", "raw/manifests/stocks")

# Idempotency: overwrite during backfills unless explicitly disabled
REPLACE_FILES = os.getenv("BACKFILL_REPLACE", "true").lower() == "true"


# ────────────────────────────────────────────────────────────────────────────────
# Helpers (API key resolution, session, resilient GET)
# ────────────────────────────────────────────────────────────────────────────────
def _get_polygon_stocks_key() -> str:
    """
    Resolve Polygon Stocks API key from Airflow Connection 'polygon_stocks_api_key'
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
        "Missing Polygon API key. Provide via Airflow connection 'polygon_stocks_api_key' "
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
        respect_retry_after_header=True,
    )
    s.mount("https://", HTTPAdapter(max_retries=retries, pool_connections=64, pool_maxsize=64))
    s.headers.update({
        "User-Agent": USER_AGENT,
        "Authorization": f"Bearer {api_key}",
    })
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
            time.sleep(float(retry_after) if retry_after else delay)
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
default_args = {
    "owner": "data-platform",
    "retries": 3,
    "retry_delay": pendulum.duration(minutes=5),
    "pool": API_POOL,
}

@dag(
    dag_id="polygon_stocks_ingest_backfill",
    description="Polygon stocks backfill ingest — Hive-layout raw JSON.gz & per-day manifests (no latest pointer).",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule=None,          # manual backfills only
    catchup=False,
    tags=["ingestion", "polygon", "stocks", "backfill", "aws"],
    params={
        "start_date": Param(default="2025-10-17", type="string", description="Backfill start date (YYYY-MM-DD)."),
        "end_date":   Param(default="2025-10-17", type="string", description="Backfill end date (YYYY-MM-DD)."),
    },
    dagrun_timeout=timedelta(hours=12),
    max_active_runs=1,
    default_args=default_args,
)
def polygon_stocks_ingest_backfill_dag():

    # Guardrail: ensure bucket configured
    if not BUCKET_NAME:
        raise RuntimeError("BUCKET_NAME env var is required (e.g., 'stock-market-elt').")

    # ───────────────────────────────
    # Inputs
    # ───────────────────────────────
    @task
    def get_custom_tickers() -> List[str]:
        """Seed tickers from dbt seeds."""
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
    def compute_trading_dates(**kwargs) -> List[str]:
        """
        Compute weekdays for the inclusive [start_date, end_date] range.
        Market holidays will be skipped later when the grouped call yields 404/empty.
        """
        start = pendulum.parse(kwargs["params"]["start_date"])
        end   = pendulum.parse(kwargs["params"]["end_date"])
        if start > end:
            raise ValueError("start_date cannot be after end_date.")
        out: List[str] = []
        cur = start
        while cur <= end:
            if cur.day_of_week not in (5, 6):  # Sat/Sun
                out.append(cur.to_date_string())
            cur = cur.add(days=1)
        if not out:
            raise AirflowSkipException("No weekdays in the given range.")
        return out

    # ───────────────────────────────
    # Fetch grouped bars & write raw files (only when non-empty)
    # ───────────────────────────────
    @task(retries=3, retry_delay=pendulum.duration(minutes=10))
    def process_day(target_date: str, custom_tickers: List[str]) -> Dict[str, int]:
        """
        For a given date:
          - Fetch grouped daily bars (all U.S. stocks),
          - Filter to seed tickers,
          - Write gz JSON under Hive layout:
              raw/stocks/year=YYYY/month=MM/day=DD/ticker=<T>.json.gz
        Returns a tiny map {target_date: wrote_count} for XCom-friendly aggregation.
        """
        api_key = _get_polygon_stocks_key()
        sess = _session_with_auth(api_key)
        s3 = S3Hook()
        custom = set(custom_tickers)

        url = POLYGON_GROUPED_URL.format(date=target_date)
        params = {"adjusted": "true"}

        resp = _rate_limited_get(sess, url, params)
        if resp.status_code == 404:
            return {}
        if resp.status_code in (401, 403):
            try:
                j = resp.json()
                msg = j.get("error") or j.get("message") or str(j)
            except Exception:
                msg = resp.text
            print(f"⚠️  {target_date} -> {resp.status_code}: {msg[:200]}")
            return {}
        resp.raise_for_status()

        data = resp.json() or {}
        results = data.get("results") or []
        if not results:
            return {}

        yyyy, mm, dd = target_date.split("-")
        wrote = 0
        now_utc = pendulum.now("UTC").to_iso8601_string()

        for r in results:
            ticker = (r.get("T") or "").upper()
            if not ticker or ticker not in custom:
                continue

            payload = {
                # business keys (unchanged so loaders keep working)
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

                # helpful lineage/audit (dbt incremental & troubleshooting)
                "ingested_at_utc": now_utc,
                "as_of_date": target_date,
            }

            # Gzip + encrypted write
            buf = BytesIO()
            with gzip.GzipFile(filename="", mode="wb", fileobj=buf) as gz:
                gz.write(json.dumps(payload, separators=(",", ":")).encode("utf-8"))

            s3_key = f"raw/stocks/year={yyyy}/month={mm}/day={dd}/ticker={ticker}.json.gz"

            if not REPLACE_FILES and s3.check_for_key(s3_key, bucket_name=BUCKET_NAME):
                wrote += 1
                continue

            s3.load_bytes(buf.getvalue(), key=s3_key, bucket_name=BUCKET_NAME, replace=True, encrypt=True)
            wrote += 1

            time.sleep(REQUEST_DELAY_SECONDS + (REQUEST_JITTER_SECONDS or 0.0))

        return {target_date: wrote} if wrote else {}

    @task
    def merge_day_counts(list_of_maps: List[Dict[str, int]]) -> Dict[str, int]:
        out: Dict[str, int] = {}
        for m in (list_of_maps or []):
            for k, v in (m or {}).items():
                out[k] = out.get(k, 0) + int(v or 0)
        return out

    # ───────────────────────────────
    # Build immutable per-day manifests by S3 listing (no pointer update)
    # ───────────────────────────────
    @task
    def write_day_manifests_from_s3(trading_days: List[str], counts_by_day: Dict[str, int]) -> List[str]:
        """
        List S3 under each day's Hive prefix; write manifest iff files exist.
        Writes to: raw/manifests/stocks/YYYY-MM-DD/manifest.txt (encrypted)
        """
        s3 = S3Hook()
        manifest_keys: List[str] = []

        for iso in sorted(trading_days):
            y, m, d = iso.split("-")
            prefix = f"raw/stocks/year={y}/month={m}/day={d}/"
            keys = s3.list_keys(bucket_name=BUCKET_NAME, prefix=prefix) or []
            keys = [k for k in keys if k and k.startswith(prefix)]
            if not keys:
                continue

            mkey = f"{DAY_MANIFEST_PREFIX}/{iso}/manifest.txt"
            s3.load_string("\n".join(sorted(set(keys))) + "\n",
                           key=mkey, bucket_name=BUCKET_NAME, replace=True, encrypt=True)
            manifest_keys.append(mkey)

        if not manifest_keys:
            raise AirflowSkipException("No non-empty per-day manifests produced.")
        return manifest_keys

    # ───────────────────────────────
    # Wiring
    # ───────────────────────────────
    custom_tickers = get_custom_tickers()
    trading_days = compute_trading_dates()
    per_day_counts = process_day.partial(custom_tickers=custom_tickers).expand(target_date=trading_days)
    counts_by_day = merge_day_counts(per_day_counts)
    _manifests = write_day_manifests_from_s3(trading_days, counts_by_day)


# Instantiate
polygon_stocks_ingest_backfill_dag()
