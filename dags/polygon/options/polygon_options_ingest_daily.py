# dags/polygon/options/polygon_options_ingest_daily.py
# =============================================================================
# Polygon Options Ingest (Daily) — backfill-parity raw ingest + Greeks snapshots
# -----------------------------------------------------------------------------
# Parity with backfill for contract bars:
#  - S3 layout: raw/options/year=YYYY/month=MM/day=DD/underlying=<U>/contract=<C>.json.gz
#  - Write only when Polygon returns non-empty `results` (skip 404/empty)
#  - Keep Authorization header + apiKey param
#  - Filter contracts to those alive on target day (toggle via env)
#  - Per-day manifest at raw/manifests/options/YYYY-MM-DD.txt + “latest” pointer
#
# Greeks (NEW):
#  - Per-underlying chain snapshot (includes greeks/IV/OI)
#  - S3 layout: raw/options_greeks/year=YYYY/month=MM/day=DD/underlying=<U>/chain.json.gz
#  - Per-day manifest at raw/manifests/options_greeks/YYYY-MM-DD.txt + “latest” pointer
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

from dags.utils.polygon_datasets import S3_OPTIONS_MANIFEST_DATASET

# ────────────────────────────────────────────────────────────────────────────────
# Config / Constants
# ────────────────────────────────────────────────────────────────────────────────
POLYGON_CONTRACTS_URL = "https://api.polygon.io/v3/reference/options/contracts"
POLYGON_AGGS_URL_BASE = "https://api.polygon.io/v2/aggs/ticker"
POLYGON_CHAIN_SNAPSHOT_URL = "https://api.polygon.io/v3/snapshot/options/{underlying}"

BUCKET_NAME = os.getenv("BUCKET_NAME")  # validated at runtime
DBT_PROJECT_DIR = os.getenv("DBT_PROJECT_DIR", "/usr/local/airflow/dbt")

REQUEST_TIMEOUT_SECS = int(os.getenv("HTTP_REQUEST_TIMEOUT_SECS", "60"))
USER_AGENT = os.getenv("HTTP_USER_AGENT", "stocks-elt/polygon-options-daily (auto)")
API_POOL = os.getenv("API_POOL", "api_pool")

CONTRACT_BATCH_SIZE = int(os.getenv("OPTIONS_DAILY_CONTRACT_BATCH_SIZE", "400"))
REQUEST_DELAY_SECONDS = float(os.getenv("POLYGON_REQUEST_DELAY_SECONDS", "0.25"))
REQUEST_JITTER_SECONDS = float(os.getenv("POLYGON_REQUEST_JITTER_SECONDS", "0.05"))
REPLACE_FILES = os.getenv("DAILY_REPLACE", "true").lower() == "true"

# Toggle the date-alive filter (same env flag used by backfill)
DISABLE_CONTRACT_DATE_FILTER = os.getenv(
    "POLYGON_DISABLE_CONTRACT_DATE_FILTER", "false"
).lower() in ("1", "true", "yes")

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
    NOTE: Called inside tasks only (no import-time secret access).
    """
    try:
        conn = BaseHook.get_connection("polygon_options_api_key")
        for candidate in [conn.password, conn.login]:
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
    if env:
        return env
    # Do not raise here—preflight task handles the error with a clear message.
    return ""

def _session(api_key: str) -> requests.Session:
    """HTTP session with retry + Bearer token auth (and apiKey still sent on requests)."""
    s = requests.Session()
    retries = Retry(
        total=6,
        backoff_factor=1.2,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=frozenset(["GET"]),
        raise_on_status=False,
    )
    s.mount("https://", HTTPAdapter(max_retries=retries, pool_connections=64, pool_maxsize=64))
    s.headers.update({
        "User-Agent": USER_AGENT,
        "Authorization": f"Bearer {api_key}",
    })
    return s

def _sleep():
    """Light client-side throttle with jitter."""
    time.sleep(REQUEST_DELAY_SECONDS + random.uniform(0, REQUEST_JITTER_SECONDS))

def _rate_limited_get(sess: requests.Session, url: str, params: dict | None, max_tries: int = 6) -> requests.Response:
    """GET with honor for 429 Retry-After + 5xx backoff; one soft retry on 401."""
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

def _alive_on_day(rec: Dict[str, Any], day_iso: str) -> bool:
    """
    True if target day is within [list_date, expiration_date] when present.
    Missing fields ⇒ permissive (True). Toggle can disable this filter.
    """
    if DISABLE_CONTRACT_DATE_FILTER:
        return True
    try:
        day = pendulum.parse(day_iso).date()
        list_date = rec.get("list_date") or rec.get("listed_date") or rec.get("created_at")
        exp_date  = rec.get("expiration_date") or rec.get("expire_date") or rec.get("expired_at")
        if list_date and day < pendulum.parse(str(list_date)).date():
            return False
        if exp_date and day > pendulum.parse(str(exp_date)).date():
            return False
        return True
    except Exception:
        return True

def _json_gz_bytes(obj: Any) -> bytes:
    buf = BytesIO()
    with gzip.GzipFile(filename="", mode="wb", fileobj=buf) as gz:
        gz.write(json.dumps(obj, separators=(",", ":")).encode("utf-8"))
    return buf.getvalue()

# ────────────────────────────────────────────────────────────────────────────────
# DAG
# ────────────────────────────────────────────────────────────────────────────────
@dag(
    dag_id="polygon_options_ingest_daily",
    description="Polygon options daily ingest (backfill-parity) + Greeks chain snapshots.",
    start_date=pendulum.datetime(2025, 10, 18, tz="UTC"),  # static start date for determinism
    schedule_interval="0 0 * * 1-5",                      # Mon–Fri midnight UTC
    catchup=True,   # set True temporarily for rolling backfill from start_date
    default_args=default_args,
    dagrun_timeout=pendulum.duration(hours=12),
    tags=["ingestion", "polygon", "options", "daily", "aws", "greeks"],
    max_active_runs=1,
)
def polygon_options_ingest_daily_dag():

    # ───────────────────────────────
    # Preflight (runtime validation)
    # ───────────────────────────────
    @task
    def preflight_validate_env() -> None:
        if not BUCKET_NAME:
            raise AirflowFailException("BUCKET_NAME env var is required (e.g., 'stock-market-elt').")
        # Secrets validation
        key = _get_polygon_options_key()
        if not key:
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
    # Discovery (contracts per underlying)
    # ───────────────────────────────
    @task_group(group_id="discover_contracts")
    def tg_discover_contracts(custom_tickers: List[str], target_date: str):

        @task(retries=3, retry_delay=pendulum.duration(minutes=5), pool=API_POOL)
        def discover_for_ticker(ticker: str, target_date: str) -> Dict[str, Any]:
            api_key = _get_polygon_options_key()
            sess = _session(api_key)

            params = {
                "underlying_ticker": ticker,
                "expiration_date.gte": target_date,
                "as_of": target_date,
                "limit": 1000,
                "apiKey": api_key,
            }
            all_recs: List[Dict[str, Any]] = []

            resp = _rate_limited_get(sess, POLYGON_CONTRACTS_URL, params)
            resp.raise_for_status()
            data = resp.json() or {}
            all_recs.extend([r for r in (data.get("results") or []) if r and r.get("ticker")])

            next_url = data.get("next_url")
            while next_url:
                resp = _rate_limited_get(sess, next_url, {"apiKey": api_key})
                resp.raise_for_status()
                page = resp.json() or {}
                all_recs.extend([r for r in (page.get("results") or []) if r and r.get("ticker")])
                next_url = page.get("next_url")
                _sleep()

            filtered = [r for r in all_recs if _alive_on_day(r, target_date)]
            contracts = sorted({r["ticker"] for r in filtered})
            return {"underlying": ticker, "contracts": contracts}

        @task
        def to_pairs(discoveries: List[Dict[str, Any]], target_date: str) -> List[Dict[str, str]]:
            pairs: List[Dict[str, str]] = []
            for d in (discoveries or []):
                u = d.get("underlying")
                for c in (d.get("contracts") or []):
                    pairs.append({"underlying": u, "contract": c, "target_date": target_date})
            if not pairs:
                raise AirflowSkipException("No eligible contracts discovered for target day.")
            return pairs

        discoveries = (
            discover_for_ticker.partial(target_date=target_date).expand(ticker=custom_tickers)
        )
        return to_pairs(discoveries, target_date)

    # ───────────────────────────────
    # Batching & Fetch (contract bars)
    # ───────────────────────────────
    @task
    def batch_pairs(pairs: List[Dict[str, str]]) -> List[List[Dict[str, str]]]:
        if not pairs:
            raise AirflowSkipException("No contracts discovered.")
        sz = CONTRACT_BATCH_SIZE
        return [pairs[i:i+sz] for i in range(0, len(pairs), sz)]

    @task(retries=3, retry_delay=pendulum.duration(minutes=5), pool=API_POOL)
    def process_batch(batch: List[Dict[str, str]]) -> List[str]:
        if not batch:
            return []

        api_key = _get_polygon_options_key()
        sess = _session(api_key)
        s3 = S3Hook()
        written: List[str] = []

        for rec in batch:
            underlying = rec["underlying"]
            contract = rec["contract"]
            target_date = rec["target_date"]
            yyyy, mm, dd = target_date.split("-")
            s3_key = (
                f"raw/options/year={yyyy}/month={mm}/day={dd}/"
                f"underlying={underlying}/contract={contract}.json.gz"
            )

            if not REPLACE_FILES and s3.check_for_key(s3_key, bucket_name=BUCKET_NAME):
                _sleep()
                continue

            url = f"{POLYGON_AGGS_URL_BASE}/{contract}/range/1/day/{target_date}/{target_date}"
            params = {"adjusted": "true", "apiKey": api_key}

            try:
                resp = _rate_limited_get(sess, url, params)
                if resp.status_code == 404:
                    _sleep()
                    continue
                resp.raise_for_status()
                obj = resp.json() or {}
                results = obj.get("results") or []
                if not results:
                    _sleep()
                    continue  # only write if we have a bar
            except requests.RequestException:
                _sleep()
                continue

            s3.load_bytes(_json_gz_bytes(obj), key=s3_key, bucket_name=BUCKET_NAME, replace=True)
            written.append(s3_key)
            _sleep()

        return written

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

            # Heuristics for "has content": support either `results` or `options` shapes
            payload = data.get("results")
            if payload is None:
                payload = data.get("options")
            if not payload:
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
            s3 = S3Hook()
            keys: List[str] = []
            if not payloads:
                return keys

            yyyy, mm, dd = target_date.split("-")
            for p in payloads:
                if not p:
                    continue
                u = p["_meta"]["underlying"]
                key = (
                    f"raw/options_greeks/year={yyyy}/month={mm}/day={dd}/"
                    f"underlying={u}/chain.json.gz"
                )
                s3.load_bytes(_json_gz_bytes(p), key=key, bucket_name=BUCKET_NAME, replace=True)
                keys.append(key)
            return keys

        # FIX: use partial() for scalar target_date; expand only over underlying (no len(XComArg))
        snapshots = get_chain_snapshot.partial(target_date=target_date).expand(underlying=custom_tickers)
        return write_chain_snapshot_to_s3(snapshots, target_date)

    # ───────────────────────────────
    # Manifests (bars + greeks)
    # ───────────────────────────────
    @task
    def flatten_keys(list_of_lists: List[List[str]]) -> List[str]:
        return [k for sub in (list_of_lists or []) for k in (sub or []) if k]

    @task
    def write_day_manifest(all_keys: List[str], target_date: str) -> Optional[str]:
        if not all_keys:
            return None
        s3 = S3Hook()
        manifest_key = f"raw/manifests/options/{target_date}.txt"
        s3.load_string("\n".join(sorted(set(all_keys))) + "\n",
                       key=manifest_key, bucket_name=BUCKET_NAME, replace=True)
        return manifest_key

    @task(outlets=[S3_OPTIONS_MANIFEST_DATASET])
    def update_latest_pointer(day_manifest_key: Optional[str]) -> Optional[str]:
        if not day_manifest_key:
            raise AirflowSkipException("No per-day bars manifest; skipping.")
        s3 = S3Hook()
        content = s3.read_key(key=day_manifest_key, bucket_name=BUCKET_NAME) or ""
        if not content.strip():
            raise AirflowSkipException("Bars manifest empty; skipping latest pointer.")
        latest_key = "raw/manifests/polygon_options_manifest_latest.txt"
        s3.load_string(content, key=latest_key, bucket_name=BUCKET_NAME, replace=True)
        return latest_key

    @task
    def write_greeks_manifest(greeks_keys: List[str], target_date: str) -> Optional[str]:
        if not greeks_keys:
            return None
        s3 = S3Hook()
        manifest_key = f"raw/manifests/options_greeks/{target_date}.txt"
        s3.load_string("\n".join(sorted(set(greeks_keys))) + "\n",
                       key=manifest_key, bucket_name=BUCKET_NAME, replace=True)
        return manifest_key

    @task
    def update_greeks_latest_pointer(day_greeks_manifest_key: Optional[str]) -> Optional[str]:
        if not day_greeks_manifest_key:
            raise AirflowSkipException("No per-day greeks manifest; skipping.")
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

    # Ensure preflight runs first
    preflight >> [custom_tickers, target_date]

    # Contract bars path (existing parity)
    pairs = tg_discover_contracts(custom_tickers, target_date)
    batches = batch_pairs(pairs)
    written_lists = process_batch.expand(batch=batches)
    all_bar_keys = flatten_keys(written_lists)
    day_bars_manifest = write_day_manifest(all_bar_keys, target_date)
    _ = update_latest_pointer(day_bars_manifest)

    # Greeks path (new)
    greeks_keys = tg_capture_greeks(custom_tickers, target_date)
    day_greeks_manifest = write_greeks_manifest(greeks_keys, target_date)
    _ = update_greeks_latest_pointer(day_greeks_manifest)

polygon_options_ingest_daily_dag()
