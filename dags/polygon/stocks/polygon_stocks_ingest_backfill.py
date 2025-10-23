# dags/polygon/stocks/polygon_stocks_ingest_backfill.py
# =============================================================================
# Polygon Stocks → S3 (Backfill)
# -----------------------------------------------------------------------------
# Historical backfill pipeline for missing days.
#
# Responsibilities:
#   • Fetch Polygon "grouped" endpoint once per day
#   • Write per-ticker gz files: raw/stocks/<TICKER>/<YYYY-MM-DD>.json.gz
#   • Write immutable per-day manifest:
#         raw/manifests/stocks/YYYY-MM-DD/manifest.txt
#   • Skip weekends and market holidays (404)
#   • Does NOT update “latest” pointer or emit a Dataset
#   • Triggers polygon_stocks_load with {"manifest_key": <manifest>} per day
#
# Downstream:
#   - polygon_stocks_load loads data into RAW schema
#   - dbt incremental models consume it downstream
# =============================================================================

from __future__ import annotations

import io
import os
import json
import gzip
from datetime import timedelta
from typing import Dict, List, Tuple, Any

import pendulum
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models.param import Param
from airflow.exceptions import AirflowSkipException
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import get_current_context

# ────────────────────────────────────────────────────────────────────────────────
# Config
# ────────────────────────────────────────────────────────────────────────────────

BUCKET_NAME = os.getenv("BUCKET_NAME")  # expected: stock-market-elt
if not BUCKET_NAME:
    raise RuntimeError("BUCKET_NAME env var is required (e.g., 'stock-market-elt').")

API_POOL = os.getenv("API_POOL", "api_pool")
INGEST_POOL = os.getenv("INGEST_POOL", "ingest_pool")
POLYGON_STOCKS_CONN_ID = os.getenv("POLYGON_STOCKS_CONN_ID", "polygon_stocks_api")
USER_AGENT = os.getenv("HTTP_USER_AGENT", "stocks-elt/polygon-stocks-ingest-backfill/1.0")
POLYGON_GROUPED_URL = "https://api.polygon.io/v2/aggs/grouped/locale/us/market/stocks/{date}"

# The manual loader DAG to trigger after each successful manifest
STOCKS_LOAD_DAG_ID = os.getenv("STOCKS_LOAD_DAG_ID", "polygon_stocks_load")

# ────────────────────────────────────────────────────────────────────────────────
# Helpers
# ────────────────────────────────────────────────────────────────────────────────

def _get_polygon_api_key() -> str:
    """Resolve API key from Airflow connection."""
    conn = BaseHook.get_connection(POLYGON_STOCKS_CONN_ID)
    if conn.password:
        return conn.password
    extra = conn.extra_dejson or {}
    key = (extra.get("api_key") or extra.get("token") or "").strip()
    if not key:
        raise RuntimeError(f"Polygon key not found in connection '{POLYGON_STOCKS_CONN_ID}'.")
    return key


def _requests_session() -> requests.Session:
    """Resilient HTTP session for Polygon."""
    sess = requests.Session()
    retry = Retry(
        total=5,
        backoff_factor=1.5,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",),
        raise_on_status=False,
        respect_retry_after_header=True,
    )
    sess.mount("https://", HTTPAdapter(max_retries=retry))
    sess.headers.update({"User-Agent": USER_AGENT})
    return sess


def _date_range_inclusive(start: pendulum.DateTime, end: pendulum.DateTime) -> List[str]:
    """Return list of YYYY-MM-DD strings inclusive of [start, end]."""
    days = []
    cur = start
    while cur <= end:
        days.append(cur.to_date_string())
        cur = cur.add(days=1)
    return days


def _is_weekend(yyyy_mm_dd: str) -> bool:
    """True if Saturday or Sunday."""
    d = pendulum.parse(yyyy_mm_dd)
    return d.weekday() >= 5


def _split_grouped_payload(payload: Dict[str, Any]) -> List[Tuple[str, Dict[str, Any]]]:
    """Convert grouped payload to per-ticker JSON docs."""
    results = payload.get("results") or []
    docs = []
    for r in results:
        t = r.get("T")
        if not t:
            continue
        docs.append((t, {
            "ticker": t,
            "results": [{
                "o": r.get("o"),
                "h": r.get("h"),
                "l": r.get("l"),
                "c": r.get("c"),
                "v": r.get("v"),
                "n": r.get("n"),
                "vw": r.get("vw"),
                "t": r.get("t"),
            }]
        }))
    return docs


def _gzip_bytes(obj: Dict[str, Any]) -> bytes:
    buf = io.BytesIO()
    with gzip.GzipFile(fileobj=buf, mode="wb") as gz:
        gz.write(json.dumps(obj, separators=(",", ":"), ensure_ascii=False).encode("utf-8"))
    return buf.getvalue()


# ────────────────────────────────────────────────────────────────────────────────
# DAG
# ────────────────────────────────────────────────────────────────────────────────

default_args = {
    "owner": "data-platform",
    "retries": 2,
    "retry_delay": pendulum.duration(minutes=5),
    "pool": INGEST_POOL,
}

@dag(
    dag_id="polygon_stocks_ingest_backfill",
    description="Polygon stocks backfill ingest — writes immutable per-day manifests and triggers polygon_stocks_load.",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    schedule=None,  # manual only
    catchup=False,
    dagrun_timeout=timedelta(hours=6),
    max_active_runs=1,
    tags=["ingest", "polygon", "stocks", "backfill"],
    default_args=default_args,
    params={
        "date_from": Param(default="2025-09-29", type="string"),
        "date_to": Param(default="2025-10-15", type="string"),
    },
)
def polygon_stocks_ingest_backfill_dag():

    @task
    def expand_dates() -> List[str]:
        ctx = get_current_context()
        dfrom = pendulum.parse(ctx["params"]["date_from"]).start_of("day")
        dto = pendulum.parse(ctx["params"]["date_to"]).start_of("day")
        if dto < dfrom:
            raise ValueError("date_to must be >= date_from")
        dates = _date_range_inclusive(dfrom, dto)
        print(f"Backfill range: {dates[0]} → {dates[-1]} ({len(dates)} days)")
        return dates

    @task(pool=API_POOL)
    def fetch_day_payload(yyyy_mm_dd: str) -> Dict[str, Any]:
        if _is_weekend(yyyy_mm_dd):
            raise AirflowSkipException(f"{yyyy_mm_dd} is weekend; skipping.")
        api_key = _get_polygon_api_key()
        url = POLYGON_GROUPED_URL.format(date=yyyy_mm_dd)
        sess = _requests_session()
        resp = sess.get(url, params={"adjusted": "true", "apiKey": api_key}, timeout=(5, 30))
        if resp.status_code == 404:
            raise AirflowSkipException(f"{yyyy_mm_dd}: market holiday (404).")
        if resp.status_code >= 400:
            raise RuntimeError(f"{yyyy_mm_dd}: HTTP {resp.status_code}: {resp.text[:200]}")
        payload = resp.json() or {}
        if not (payload.get("results") or []):
            raise AirflowSkipException(f"{yyyy_mm_dd}: empty results; skipping.")
        return {"date": yyyy_mm_dd, "payload": payload}

    @task
    def write_per_ticker_files(day: Dict[str, Any]) -> Dict[str, Any]:
        yyyy_mm_dd = day["date"]
        payload = day["payload"]
        s3 = S3Hook()
        docs = _split_grouped_payload(payload)
        if not docs:
            raise AirflowSkipException(f"{yyyy_mm_dd}: no per-ticker docs.")
        docs.sort(key=lambda x: x[0])  # deterministic order

        keys = []
        for ticker, doc in docs:
            key = f"raw/stocks/{ticker}/{yyyy_mm_dd}.json.gz"
            body = _gzip_bytes(doc)
            s3.load_bytes(body, key=key, bucket_name=BUCKET_NAME, replace=True, encrypt=True)
            keys.append(key)
        print(f"{yyyy_mm_dd}: wrote {len(keys)} ticker files.")
        return {"date": yyyy_mm_dd, "keys": keys}

    @task
    def write_manifest(day_files: Dict[str, Any]) -> str:
        yyyy_mm_dd = day_files["date"]
        keys = day_files["keys"]
        if not keys:
            raise AirflowSkipException(f"{yyyy_mm_dd}: no keys for manifest.")
        manifest_key = f"raw/manifests/stocks/{yyyy_mm_dd}/manifest.txt"
        s3 = S3Hook()
        s3.load_string("\n".join(keys) + "\n", manifest_key, bucket_name=BUCKET_NAME, replace=True, encrypt=True)
        print(f"{yyyy_mm_dd}: wrote manifest at s3://{BUCKET_NAME}/{manifest_key}")
        return manifest_key

    # ──────────────────────────────────────────────────────────────────────
    # Flow: add synchronization barrier between fetch and write phases
    # ──────────────────────────────────────────────────────────────────────
    dates = expand_dates()
    day_payloads = fetch_day_payload.expand(yyyy_mm_dd=dates)

    wait_for_all_fetches = EmptyOperator(task_id="wait_for_all_fetches")
    day_payloads >> wait_for_all_fetches

    day_files = write_per_ticker_files.expand(day=day_payloads)
    wait_for_all_fetches >> day_files

    manifests = write_manifest.expand(day_files=day_files)

    TriggerDagRunOperator.partial(
        task_id="trigger_polygon_stocks_load",
        trigger_dag_id=STOCKS_LOAD_DAG_ID,
        reset_dag_run=False,
        wait_for_completion=False,
    ).expand(conf=manifests)

# Instantiate
polygon_stocks_ingest_backfill_dag()
