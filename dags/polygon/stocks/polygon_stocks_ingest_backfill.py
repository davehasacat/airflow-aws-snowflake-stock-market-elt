# dags/polygon/stocks/polygon_stocks_ingest_backfill.py
# =============================================================================
# Polygon Stocks Backfill (Ingestion)
# -----------------------------------------------------------------------------
# Enterprise/modern strategy (aligned with options + dbt incremental):
#   • S3 layout (per-ticker, per-day, gz):
#       raw/stocks/<TICKER>/<YYYY-MM-DD>.json.gz
#   • Per-day manifest (immutable; NO latest-pointer update in backfills):
#       raw/manifests/stocks/YYYY-MM-DD/manifest.txt
#   • All S3 writes encrypted at rest (encrypt=True) with deterministic keys
#   • Inclusive date range params; weekends skipped; holidays handled via 404
#   • Connection-aware API key resolution + robust retries/backoff
#   • Minimal lineage fields for downstream dbt:
#       as_of_date (YYYY-MM-DD), ingested_at_utc (ISO8601)
#
# Backfill behavior:
#   • Does NOT emit S3_STOCKS_MANIFEST_DATASET and does NOT touch the “latest” pointer.
#   • Instead, optionally TRIGGERS the manual loader DAG `polygon_stocks_load`
#     with one run per non-empty day (conf={"manifest_key": <mkey>}). Controlled by
#     param trigger_loader (default: True).
# =============================================================================

from __future__ import annotations

import os
import json
import csv
import time
import gzip
from io import BytesIO
from typing import List, Optional

import pendulum
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from datetime import timedelta

from airflow.decorators import dag, task
from airflow.exceptions import AirflowSkipException
from airflow.hooks.base import BaseHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models.param import Param
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

# ────────────────────────────────────────────────────────────────────────────────
# Config / Constants
# ────────────────────────────────────────────────────────────────────────────────
POLYGON_GROUPED_URL = "https://api.polygon.io/v2/aggs/grouped/locale/us/market/stocks/{date}"

BUCKET_NAME = os.getenv("BUCKET_NAME")  # expected: 'stock-market-elt'
DBT_PROJECT_DIR = os.getenv("DBT_PROJECT_DIR", "/usr/local/airflow/dbt")

REQUEST_TIMEOUT_SECS = int(os.getenv("HTTP_REQUEST_TIMEOUT_SECS", "60"))
USER_AGENT = os.getenv("HTTP_USER_AGENT", "stocks-elt/polygon-stocks-backfill (manual)")
API_POOL = os.getenv("API_POOL", "api_pool")

REQUEST_DELAY_SECONDS = float(os.getenv("POLYGON_REQUEST_DELAY_SECONDS", "0.25"))
REPLACE_FILES = os.getenv("BACKFILL_REPLACE", "true").lower() == "true"

DAY_MANIFEST_PREFIX = os.getenv("STOCKS_DAY_MANIFEST_PREFIX", "raw/manifests/stocks")

# Optional pool for fan-out triggers to the loader
LOAD_POOL = os.getenv("LOAD_POOL", "load_pool")

if not BUCKET_NAME:
    raise RuntimeError("BUCKET_NAME env var is required (e.g., 'stock-market-elt').")

# ────────────────────────────────────────────────────────────────────────────────
# Helpers (API key, session, trading calendar)
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
    """HTTP session with retry/backoff + Bearer token auth."""
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

def _date_list_inclusive(start_iso: str, end_iso: str) -> List[str]:
    """Inclusive [start, end]; skip Sat/Sun; holidays handled via 404 later."""
    start = pendulum.parse(start_iso)
    end = pendulum.parse(end_iso)
    if start > end:
        raise ValueError("start_date cannot be after end_date.")
    out: List[str] = []
    cur = start
    while cur <= end:
        if cur.day_of_week not in (5, 6):  # Sat/Sun
            out.append(cur.to_date_string())
        cur = cur.add(days=1)
    return out

# ────────────────────────────────────────────────────────────────────────────────
# DAG
# ────────────────────────────────────────────────────────────────────────────────
@dag(
    dag_id="polygon_stocks_ingest_backfill",
    description="Polygon stocks backfill — raw JSON.gz to S3 + immutable per-day manifests; optionally triggers polygon_stocks_load.",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule=None,           # manual runs only
    catchup=False,
    tags=["ingestion", "polygon", "stocks", "backfill", "aws"],
    params={
        "start_date": Param(default="2025-10-17", type="string", description="Backfill start date (YYYY-MM-DD)"),
        "end_date":   Param(default="2025-10-20", type="string", description="Backfill end date (YYYY-MM-DD)"),
        # NEW: directly trigger the manual loader per non-empty day
        "trigger_loader": Param(default=True, type="boolean",
                                description="When true, trigger polygon_stocks_load for each per-day manifest."),
    },
    dagrun_timeout=timedelta(hours=12),
    max_active_runs=1,
)
def polygon_stocks_ingest_backfill_dag():

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
    def generate_trading_dates(**kwargs) -> List[str]:
        """Compute inclusive weekday date list; holidays handled downstream."""
        dates = _date_list_inclusive(kwargs["params"]["start_date"], kwargs["params"]["end_date"])
        if not dates:
            raise AirflowSkipException("No trading dates in the given range.")
        return dates

    # ───────────────────────────────
    # Fetch grouped, split per-ticker, write raw
    # ───────────────────────────────
    @task(retries=3, retry_delay=pendulum.duration(minutes=10), pool=API_POOL)
    def process_date(target_date: str, custom_tickers: List[str]) -> List[str]:
        """
        For a given date:
          - Fetch grouped daily bars (all U.S. stocks),
          - Filter to your custom tickers,
          - Transform to the per-ticker JSON shape your loaders expect (results[0]),
          - Add minimal lineage (as_of_date, ingested_at_utc),
          - Write to s3://<bucket>/raw/stocks/<TICKER>/<YYYY-MM-DD>.json.gz (encrypted).
        Returns written S3 keys for this date.
        """
        s3 = S3Hook()
        api_key = _get_polygon_stocks_key()
        sess = _session_with_auth(api_key)
        custom = set(custom_tickers)

        url = POLYGON_GROUPED_URL.format(date=target_date)
        params = {"adjusted": "true"}

        resp = sess.get(url, params=params, timeout=REQUEST_TIMEOUT_SECS)
        if resp.status_code == 404:
            # Market holiday/closure → nothing to do
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
    # Manifest writing (immutable per-day; NO pointer update here)
    # ───────────────────────────────
    @task
    def write_day_manifest(s3_keys: List[str], target_date: str) -> Optional[str]:
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

    @task
    def build_loader_confs(manifest_keys: List[Optional[str]], **kwargs) -> List[dict]:
        """
        Build a list of {'manifest_key': <mkey>} for non-empty days — only if
        params.trigger_loader is True.
        """
        trigger = bool(kwargs["params"].get("trigger_loader", True))
        if not trigger:
            raise AirflowSkipException("trigger_loader=False — not triggering polygon_stocks_load.")
        kept = [k for k in (manifest_keys or []) if k]
        if not kept:
            raise AirflowSkipException("No non-empty per-day manifests to trigger loads for.")
        return [{"manifest_key": k} for k in kept]

    # ───────────────────────────────
    # Wiring
    # ───────────────────────────────
    custom_tickers = get_custom_tickers()
    dates = generate_trading_dates()
    per_day_keys = process_date.partial(custom_tickers=custom_tickers).expand(target_date=dates)
    manifest_keys = write_day_manifest.partial(target_date=None).expand(
        s3_keys=per_day_keys,
        target_date=dates,
    )
    loader_confs = build_loader_confs(manifest_keys)

    # One TriggerDagRunOperator mapped over per-day confs (no duplicates)
    TriggerDagRunOperator.partial(
        task_id="trigger_polygon_stocks_load",
        trigger_dag_id="polygon_stocks_load",
        reset_dag_run=False,
        wait_for_completion=False,
        pool=LOAD_POOL,
    ).expand(conf=loader_confs)

# Instantiate
polygon_stocks_ingest_backfill_dag()
