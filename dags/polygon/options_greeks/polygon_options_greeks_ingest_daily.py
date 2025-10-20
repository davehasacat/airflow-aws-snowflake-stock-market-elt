# =============================================================================
# Polygon Options Greeks Ingest (Daily)
# -----------------------------------------------------------------------------
# - Reads custom underlyings from dbt/seeds/custom_tickers.csv
# - Fetches Polygon chain snapshots (incl. greeks/IV/OI) once per trading day
# - Writes gzip’d JSON to: raw/options_greeks/year=YYYY/month=MM/day=DD/underlying=<U>/chain.json.gz
# - Emits per-day manifest + “latest” pointer (separate namespace from bars)
# - No import-time secret checks; all validation happens at runtime
# - Concurrency, retries, and rate-limit handling aligned with bars ingest
# =============================================================================

from __future__ import annotations

import csv
import json
import os
import time
import gzip
import random
from io import BytesIO
from typing import List, Dict, Any, Optional

import pendulum
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from airflow.decorators import dag, task, task_group
from airflow.operators.python import get_current_context
from airflow.exceptions import AirflowSkipException, AirflowFailException
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.hooks.base import BaseHook

from dags.utils.polygon_datasets import (
    S3_OPTIONS_GREEKS_MANIFEST_DATASET,  # emitted when latest pointer updates
)

# ────────────────────────────────────────────────────────────────────────────────
# Config / Constants (env-first; validated at runtime)
# ────────────────────────────────────────────────────────────────────────────────
POLYGON_CHAIN_SNAPSHOT_URL = "https://api.polygon.io/v3/snapshot/options/{underlying}"

BUCKET_NAME = os.getenv("BUCKET_NAME")                    # validated in preflight
DBT_PROJECT_DIR = os.getenv("DBT_PROJECT_DIR", "/usr/local/airflow/dbt")

REQUEST_TIMEOUT_SECS = int(os.getenv("HTTP_REQUEST_TIMEOUT_SECS", "60"))
USER_AGENT = os.getenv("HTTP_USER_AGENT", "stocks-elt/polygon-options-greeks-daily (auto)")
API_POOL = os.getenv("API_POOL", "api_pool")

REQUEST_DELAY_SECONDS = float(os.getenv("POLYGON_REQUEST_DELAY_SECONDS", "0.25"))
REQUEST_JITTER_SECONDS = float(os.getenv("POLYGON_REQUEST_JITTER_SECONDS", "0.05"))
REPLACE_FILES = os.getenv("DAILY_REPLACE", "true").lower() == "true"

default_args = {
    "owner": "data-platform",
    "retries": 3,
    "retry_delay": pendulum.duration(minutes=5),
}

# ────────────────────────────────────────────────────────────────────────────────
# Helpers
# ────────────────────────────────────────────────────────────────────────────────
def _get_polygon_options_key() -> str:
    """
    Retrieve Polygon Options API key from Airflow Connection 'polygon_options_api_key'
    (password/login/extras.api_key), else env POLYGON_OPTIONS_API_KEY.
    Called inside tasks (no import-time secret access).
    """
    try:
        conn = BaseHook.get_connection("polygon_options_api_key")
        for candidate in (conn.password, conn.login):
            if isinstance(candidate, str) and candidate.strip():
                return candidate.strip()
        x = conn.extra_dejson or {}
        for k in ("api_key", "key", "token", "password", "polygon_options_api_key", "value"):
            v = x.get(k)
            if isinstance(v, str) and v.strip():
                return v.strip()
    except Exception:
        pass
    env = os.getenv("POLYGON_OPTIONS_API_KEY", "").strip()
    return env  # may be empty; preflight will error if missing

def _session(api_key: str) -> requests.Session:
    """HTTP session with retry + Bearer token auth (and apiKey still sent)."""
    s = requests.Session()
    retries = Retry(
        total=6,
        backoff_factor=1.2,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=frozenset(["GET"]),
        raise_on_status=False,
    )
    s.mount("https://", HTTPAdapter(max_retries=retries, pool_connections=64, pool_maxsize=64))
    s.headers.update({"User-Agent": USER_AGENT, "Authorization": f"Bearer {api_key}"})
    return s

def _sleep():
    time.sleep(REQUEST_DELAY_SECONDS + random.uniform(0, REQUEST_JITTER_SECONDS))

def _rate_limited_get(sess: requests.Session, url: str, params: dict | None, max_tries: int = 6) -> requests.Response:
    """GET honoring 429 Retry-After + 5xx backoff; one soft retry on 401."""
    backoff = 1.5
    delay = 1.0
    tries = 0
    while True:
        resp = sess.get(url, params=params, timeout=REQUEST_TIMEOUT_SECS)
        if resp.status_code == 401 and tries < 1:
            time.sleep(0.5); tries += 1; continue
        if resp.status_code == 429:
            retry_after = resp.headers.get("Retry-After")
            time.sleep(float(retry_after) if retry_after else delay)
            delay *= backoff; tries += 1
        elif 500 <= resp.status_code < 600:
            if tries >= max_tries: resp.raise_for_status()
            time.sleep(delay); delay *= backoff; tries += 1
        else:
            return resp

def _json_gz_bytes(obj: Any) -> bytes:
    buf = BytesIO()
    with gzip.GzipFile(filename="", mode="wb", fileobj=buf) as gz:
        gz.write(json.dumps(obj, separators=(",", ":")).encode("utf-8"))
    return buf.getvalue()

# ────────────────────────────────────────────────────────────────────────────────
# DAG
# ────────────────────────────────────────────────────────────────────────────────
@dag(
    dag_id="polygon_options_greeks_ingest_daily",
    description="Polygon options Greeks daily ingest: chain snapshots → S3 + per-day manifest & latest pointer.",
    start_date=pendulum.datetime(2025, 10, 1, tz="UTC"),
    schedule_interval="0 0 * * 1-5",  # Mon–Fri midnight UTC
    catchup=False,
    default_args=default_args,
    dagrun_timeout=pendulum.duration(hours=6),
    tags=["ingestion", "polygon", "options", "daily", "aws", "greeks"],
    max_active_runs=1,
)
def polygon_options_greeks_ingest_daily_dag():

    # ───────────────────────────────
    # Preflight (runtime validation)
    # ───────────────────────────────
    @task
    def preflight_validate_env() -> None:
        if not BUCKET_NAME:
            raise AirflowFailException("BUCKET_NAME env var is required (e.g., 'stock-market-elt').")
        if not _get_polygon_options_key():
            raise AirflowFailException(
                "Polygon Options API key not found. Define Airflow connection 'polygon_options_api_key' "
                "backed by AWS Secrets Manager, or set env POLYGON_OPTIONS_API_KEY."
            )

    # ───────────────────────────────
    # Inputs
    # ───────────────────────────────
    @task
    def get_custom_tickers() -> List[str]:
        """Read seed underlyings (these define the <U> in S3 layout)."""
        path = os.path.join(DBT_PROJECT_DIR, "seeds", "custom_tickers.csv")
        tickers: List[str] = []
        with open(path, mode="r") as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                t = (row.get("ticker") or "").strip().upper()
                if t:
                    tickers.append(t)
        if not tickers:
            raise AirflowSkipException("No tickers found in dbt/seeds/custom_tickers.csv.")
        return tickers

    @task
    def compute_target_date() -> str:
        """Target the previous business day (Fri for Sat/Sun)."""
        ctx = get_current_context()
        d = ctx["logical_date"].subtract(days=1)
        while d.day_of_week in (5, 6):  # 5=Sat, 6=Sun
            d = d.subtract(days=1)
        return d.to_date_string()

    # ───────────────────────────────
    # Greeks capture (per underlying)
    # ───────────────────────────────
    @task_group(group_id="capture_greeks")
    def tg_capture_greeks(custom_tickers: List[str], target_date: str):

        @task(retries=3, retry_delay=pendulum.duration(minutes=5), pool=API_POOL)
        def get_chain_snapshot(underlying: str, target_date: str) -> Optional[Dict[str, Any]]:
            """Fetch chain snapshot (includes greeks/IV/OI). Return None if empty/unavailable."""
            api_key = _get_polygon_options_key()
            sess = _session(api_key)
            url = POLYGON_CHAIN_SNAPSHOT_URL.format(underlying=underlying)
            params = {"apiKey": api_key}

            resp = _rate_limited_get(sess, url, params)
            if resp.status_code == 404:
                return None
            resp.raise_for_status()
            data = resp.json() or {}

            # Accept either `results` or `options` array shapes
            arr = data.get("results", None)
            if arr is None:
                arr = data.get("options")
            if not arr:
                return None

            return {
                "_meta": {
                    "underlying": underlying,
                    "target_date": target_date,
                    "endpoint": url,
                    "status": "ok",
                    "as_of": pendulum.now("UTC").to_iso8601_string(),
                },
                "data": data,
            }

        @task
        def write_chain_snapshot_to_s3(payloads: List[Optional[Dict[str, Any]]], target_date: str) -> List[str]:
            """Write payloads to S3 with idempotency guard (REPLACE_FILES=false => skip existing)."""
            s3 = S3Hook()
            keys: List[str] = []
            if not payloads:
                return keys

            yyyy, mm, dd = target_date.split("-")
            for p in payloads:
                if not p:
                    continue
                u = p["_meta"]["underlying"]
                key = f"raw/options_greeks/year={yyyy}/month={mm}/day={dd}/underlying={u}/chain.json.gz"

                # Idempotency guard to mirror bars ingest behavior
                if not REPLACE_FILES and s3.check_for_key(key, bucket_name=BUCKET_NAME):
                    continue

                s3.load_bytes(_json_gz_bytes(p), key=key, bucket_name=BUCKET_NAME, replace=True)
                keys.append(key)
            return keys

        snapshots = get_chain_snapshot.partial(target_date=target_date).expand(underlying=custom_tickers)
        return write_chain_snapshot_to_s3(snapshots, target_date)

    # ───────────────────────────────
    # Manifests (per-day + latest pointer)
    # ───────────────────────────────
    @task
    def write_greeks_manifest(greeks_keys: List[str], target_date: str) -> Optional[str]:
        if not greeks_keys:
            return None
        s3 = S3Hook()
        manifest_key = f"raw/manifests/options_greeks/{target_date}.txt"
        s3.load_string("\n".join(sorted(set(greeks_keys))) + "\n",
                       key=manifest_key, bucket_name=BUCKET_NAME, replace=True)
        return manifest_key

    @task(outlets=[S3_OPTIONS_GREEKS_MANIFEST_DATASET])
    def update_greeks_latest_pointer(day_greeks_manifest_key: Optional[str]) -> Optional[str]:
        if not day_greeks_manifest_key:
            raise AirflowSkipException("No per-day greeks manifest; skipping latest pointer.")
        s3 = S3Hook()
        content = s3.read_key(key=day_greeks_manifest_key, bucket_name=BUCKET_NAME) or ""
        if not content.strip():
            raise AirflowSkipException("Greeks manifest empty; skipping latest pointer.")
        latest_key = "raw/manifests/polygon_options_greeks_manifest_latest.txt"
        s3.load_string(content, key=latest_key, bucket_name=BUCKET_NAME, replace=True)
        return latest_key

    # ───────────────────────────────
    # Wiring
    # ───────────────────────────────
    preflight = preflight_validate_env()
    custom_tickers = get_custom_tickers()
    target_date = compute_target_date()

    # Ensure checks run first
    preflight >> [custom_tickers, target_date]

    greeks_keys = tg_capture_greeks(custom_tickers, target_date)
    day_greeks_manifest = write_greeks_manifest(greeks_keys, target_date)
    _ = update_greeks_latest_pointer(day_greeks_manifest)


polygon_options_greeks_ingest_daily_dag()
