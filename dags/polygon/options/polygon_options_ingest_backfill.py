# dags/polygon/options/polygon_options_ingest_backfill.py
# =============================================================================
# Polygon Options → S3 (Backfill) — Contracts-as-of + Day Aggs
# -----------------------------------------------------------------------------
# For each trading day in [params.start_date, params.end_date] and each
# underlying from dbt/seeds/custom_tickers.csv, fetch contracts "as_of" the day,
# then fetch 1D aggs for each contract. Writes compact JSON(.gz) and a per-day
# flat manifest, then updates a pointer manifest watched by the load DAG.
# =============================================================================
from __future__ import annotations

import csv
import gzip
import io
import json
import os
from datetime import date, timedelta
from typing import List, Optional, Tuple

import pendulum
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from airflow.decorators import dag, task
from airflow.exceptions import AirflowSkipException
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.python import get_current_context

from dags.utils.polygon_datasets import S3_OPTIONS_MANIFEST_DATASET

# ────────────────────────────────────────────────────────────────────────────────
# Config / Constants
# ────────────────────────────────────────────────────────────────────────────────
POLYGON_CONTRACTS_URL = "https://api.polygon.io/v3/reference/options/contracts"
POLYGON_AGGS_URL_BASE = "https://api.polygon.io/v2/aggs/ticker"

BUCKET_NAME = os.getenv("BUCKET_NAME")  # required
DBT_PROJECT_DIR = os.getenv("DBT_PROJECT_DIR", "/usr/local/airflow/dbt")
USER_AGENT = os.getenv("HTTP_USER_AGENT", "stocks-elt/polygon-options-backfill/1.0")

TICKERS_SEED_PATH = os.getenv("TICKERS_SEED_PATH", f"{DBT_PROJECT_DIR}/seeds/custom_tickers.csv")
TICKERS_COLUMN = os.getenv("TICKERS_COLUMN", "ticker")

API_POOL = os.getenv("API_POOL", "api_pool")
HTTP_CONNECT_TIMEOUT = int(os.getenv("HTTP_CONNECT_TIMEOUT", "5"))
HTTP_READ_TIMEOUT = int(os.getenv("HTTP_READ_TIMEOUT", "30"))
HTTP_TOTAL_RETRIES = int(os.getenv("HTTP_TOTAL_RETRIES", "3"))
HTTP_BACKOFF_FACTOR = float(os.getenv("HTTP_BACKOFF_FACTOR", "0.5"))

CONTRACTS_PAGE_LIMIT = int(os.getenv("POLYGON_CONTRACTS_PAGE_LIMIT", "1000"))

# ────────────────────────────────────────────────────────────────────────────────
# Small helpers
# ────────────────────────────────────────────────────────────────────────────────
def _get_polygon_options_api_key() -> str:
    """
    Lookup order (connection ID is canonical):
      1) Env var POLYGON_OPTIONS_API_KEY
      2) Airflow Connection: polygon_options_api_key
      3) Airflow Variable POLYGON_OPTIONS_API_KEY
    """
    k = os.getenv("POLYGON_OPTIONS_API_KEY")
    if k:
        return k

    try:
        conn = BaseHook.get_connection("polygon_options_api_key")
        if conn and conn.password:
            return conn.password
        extra = (conn.extra_dejson or {}) if conn else {}
        if extra.get("api_key"):
            return extra["api_key"]
    except Exception:
        pass

    v = Variable.get("POLYGON_OPTIONS_API_KEY", default_var=None)
    if v:
        return v

    raise RuntimeError(
        "Polygon Options API key not found. Provide POLYGON_OPTIONS_API_KEY env, "
        "Airflow connection 'polygon_options_api_key', or Variable 'POLYGON_OPTIONS_API_KEY'."
    )

def _http_session(api_key: str) -> requests.Session:
    """
    Session with retry + both header and param auth support.
    Using Bearer header helps when 'next_url' omits apiKey.
    """
    s = requests.Session()
    retry = Retry(
        total=HTTP_TOTAL_RETRIES,
        backoff_factor=HTTP_BACKOFF_FACTOR,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET"]),
        raise_on_status=False,
    )
    s.headers.update({
        "User-Agent": USER_AGENT,
        "Authorization": f"Bearer {api_key}",
    })
    s.mount("https://", HTTPAdapter(max_retries=retry))
    return s

def _gzip_bytes(data: dict) -> bytes:
    buf = io.BytesIO()
    with gzip.GzipFile(fileobj=buf, mode="wb") as gz:
        gz.write(json.dumps(data, separators=(",", ":"), ensure_ascii=False).encode("utf-8"))
    return buf.getvalue()

def _trading_days(start: date, end: date) -> List[date]:
    days: List[date] = []
    d = start
    while d <= end:
        if d.weekday() < 5:
            days.append(d)
        d += timedelta(days=1)
    return days

# ────────────────────────────────────────────────────────────────────────────────
# DAG
# ────────────────────────────────────────────────────────────────────────────────
@dag(
    dag_id="polygon_options_ingest_backfill",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["backfill", "polygon", "options", "s3"],
    default_args={"owner": "data-eng", "pool": API_POOL},
    max_active_runs=1,
    params={
        "start_date": None,  # "YYYY-MM-DD"
        "end_date": None,    # "YYYY-MM-DD"
    },
)
def polygon_options_backfill_dag():
    """Backfill options data → S3; writes per-day flat manifests & pointer."""

    if not BUCKET_NAME:
        raise RuntimeError("BUCKET_NAME env var is required (e.g., 'stock-market-elt').")

    @task
    def resolve_days() -> List[str]:
        """Resolve the trading-day list from DAG params (inside task → context-safe)."""
        ctx = get_current_context()
        p = ctx.get("params", {}) or {}
        start_str = p.get("start_date")
        end_str = p.get("end_date")
        default_day = (pendulum.today("UTC") - timedelta(days=1)).date()
        start_d = pendulum.parse(start_str).date() if start_str else default_day
        end_d = pendulum.parse(end_str).date() if end_str else start_d
        return [d.isoformat() for d in _trading_days(start_d, end_d)]

    @task
    def read_underlyings() -> List[str]:
        """Read underlying tickers from dbt seed CSV."""
        path = TICKERS_SEED_PATH
        if not os.path.exists(path):
            raise FileNotFoundError(f"Tickers seed not found at {path}")
        out: List[str] = []
        with open(path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            if TICKERS_COLUMN not in reader.fieldnames:
                raise ValueError(
                    f"Column '{TICKERS_COLUMN}' not found in {path}. "
                    f"Available: {reader.fieldnames}"
                )
            for row in reader:
                t = (row.get(TICKERS_COLUMN) or "").strip().upper()
                if t:
                    out.append(t)
        if not out:
            raise AirflowSkipException("No tickers found in custom_tickers.csv")
        return sorted(set(out))

    @task
    def make_work_pairs(days: List[str], underlyings: List[str]) -> List[Tuple[str, str]]:
        """Cartesian product of (day, underlying)."""
        if not days or not underlyings:
            raise AirflowSkipException("No days or underlyings to process.")
        return [(d, u) for d in days for u in underlyings]

    @task(retries=2)
    def process_underlying_day(work_item: Tuple[str, str]) -> List[str]:
        """
        For a (day, underlying):
          1) list all contracts as-of the day
          2) fetch daily aggs for each contract (same day)
          3) write .json.gz to S3
        Returns a list of the S3 keys written for this (day, underlying).
        """
        as_of, underlying = work_item
        api_key = _get_polygon_options_api_key()
        s = _http_session(api_key)

        # 1) contracts (first page)
        params = {
            "underlying_ticker": underlying,
            "as_of": as_of,
            "limit": CONTRACTS_PAGE_LIMIT,
            "expired": "true",
            "apiKey": api_key,  # keep query param too
        }
        r = s.get(POLYGON_CONTRACTS_URL, params=params, timeout=(HTTP_CONNECT_TIMEOUT, HTTP_READ_TIMEOUT))
        r.raise_for_status()
        payload = r.json() or {}
        tickers = [rec.get("ticker") for rec in (payload.get("results") or []) if rec.get("ticker")]

        # follow cursor via next_url — ensure apiKey present even if next_url lacks it
        next_url = payload.get("next_url")
        while next_url:
            r2 = s.get(next_url, params={"apiKey": api_key}, timeout=(HTTP_CONNECT_TIMEOUT, HTTP_READ_TIMEOUT))
            if r2.status_code == 401:
                # Helpful debug to surface when Bearer+param still fails
                raise requests.HTTPError(f"401 Unauthorized while paging contracts (as_of={as_of}, underlier={underlying}). Check Polygon plan/permissions and key.", response=r2)
            r2.raise_for_status()
            p2 = r2.json() or {}
            tickers.extend([rec.get("ticker") for rec in (p2.get("results") or []) if rec.get("ticker")])
            next_url = p2.get("next_url")

        tickers = sorted(set(tickers))
        if not tickers:
            return []

        # 2) per-contract aggs for the day
        yyyy, mm, dd = as_of.split("-")
        base_prefix = f"raw/options/year={yyyy}/month={mm}/day={dd}/underlying={underlying}/"

        s3 = S3Hook()
        written: List[str] = []
        for contract in tickers:
            url = f"{POLYGON_AGGS_URL_BASE}/{contract}/range/1/day/{as_of}/{as_of}"
            params = {"adjusted": "true", "limit": 50000, "apiKey": api_key}
            resp = s.get(url, params=params, timeout=(HTTP_CONNECT_TIMEOUT, HTTP_READ_TIMEOUT))
            if resp.status_code == 404:
                continue
            if resp.status_code == 401:
                raise requests.HTTPError(f"401 Unauthorized fetching aggs for {contract} on {as_of}.", response=resp)
            resp.raise_for_status()
            data = resp.json() or {}
            results = data.get("results") or []
            if not results:
                continue

            s3_key = f"{base_prefix}contract={contract}.json.gz"
            body = _gzip_bytes({
                "ticker": contract,
                "as_of": as_of,
                "underlying": underlying,
                "results": results,
                "query_count": data.get("queryCount"),
                "adjusted": data.get("adjusted"),
                "results_count": data.get("resultsCount"),
                "status": data.get("status"),
            })
            s3.load_bytes(bytes_data=body, key=s3_key, bucket_name=BUCKET_NAME, replace=True, gzip=False)
            written.append(s3_key)

        return written

    @task
    def write_flat_manifests(days: List[str], all_written: List[List[str]]) -> List[str]:
        """
        Aggregate all written keys across mapped tasks, write one flat manifest
        per day: raw/manifests/options/YYYY-MM-DD.txt. Returns list of manifest keys.
        """
        by_day = {d: [] for d in days}
        for keys in (all_written or []):
            for k in (keys or []):
                parts = k.split("/")
                if len(parts) >= 6 and parts[0] == "raw" and parts[1] == "options":
                    y = parts[3].split("=")[-1]
                    m = parts[4].split("=")[-1]
                    d = parts[5].split("=")[-1]
                    iso = f"{y}-{m}-{d}"
                    if iso in by_day:
                        by_day[iso].append(k)

        s3 = S3Hook()
        manifest_keys: List[str] = []
        for d in days:
            keys = by_day.get(d, [])
            manifest_key = f"raw/manifests/options/{d}.txt"
            content = "\n".join(keys) + ("\n" if keys else "")
            s3.load_string(content, key=manifest_key, bucket_name=BUCKET_NAME, replace=True)
            manifest_keys.append(manifest_key)
        return manifest_keys

    @task(outlets=[S3_OPTIONS_MANIFEST_DATASET])
    def update_latest_pointer(manifest_keys: List[str]) -> str:
        """Point the loader to the most recent day from this run."""
        if not manifest_keys:
            raise AirflowSkipException("No manifests written.")
        latest = sorted(manifest_keys)[-1]
        pointer_key = "raw/manifests/polygon_options_manifest_latest.txt"
        s3 = S3Hook()
        s3.load_string(f"POINTER={latest}\n", key=pointer_key, bucket_name=BUCKET_NAME, replace=True)
        return pointer_key

    # ────────────────────────────────────────────────────────────────────────────
    # Flow
    # ────────────────────────────────────────────────────────────────────────────
    days = resolve_days()
    underlyings = read_underlyings()
    work_pairs = make_work_pairs(days, underlyings)

    written_keys_lists = process_underlying_day.expand(work_item=work_pairs)
    manifest_keys = write_flat_manifests(days, written_keys_lists)
    _pointer = update_latest_pointer(manifest_keys)


# Instantiate the DAG
polygon_options_backfill_dag()
