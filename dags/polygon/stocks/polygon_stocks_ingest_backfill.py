# dags/polygon/stocks/polygon_stocks_ingest_backfill.py
# =============================================================================
# Polygon Stocks → S3 (Backfill) — immutable per-day manifest writer
# -----------------------------------------------------------------------------
# For an inclusive date range:
#   • Fetches Polygon "grouped" endpoint once per day
#   • Writes per-ticker, per-day JSON(.gz): raw/stocks/<TICKER>/<YYYY-MM-DD>.json.gz
#   • Writes per-day manifest (flat list of raw/* keys):
#         raw/manifests/stocks/<YYYY-MM-DD>/manifest.txt
#   • Does NOT update the "latest" pointer
#   • Triggers polygon_stocks_load with {"manifest_key": ...} per produced day
#
# Notes:
#   • Weekend/holiday handling: 404 or empty "results" → skip day (no manifest)
#   • Connection-aware: Polygon API key read from Airflow Connection
#   • Retries/backoff on HTTP errors and 429s
#   • Deterministic file ordering in manifests (sorted by ticker)
# =============================================================================

from __future__ import annotations

import io
import os
import json
import gzip
from datetime import timedelta
from typing import Dict, List, Tuple

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

# ────────────────────────────────────────────────────────────────────────────────
# Constants & Config
# ────────────────────────────────────────────────────────────────────────────────

# S3 bucket (required)
BUCKET_NAME = os.getenv("BUCKET_NAME")  # e.g., stock-market-elt
if not BUCKET_NAME:
    raise RuntimeError("BUCKET_NAME env var is required (e.g., 'stock-market-elt').")

# Pools
API_POOL = os.getenv("API_POOL", "api_pool")
INGEST_POOL = os.getenv("INGEST_POOL", "ingest_pool")

# Polygon
POLYGON_STOCKS_CONN_ID = os.getenv("POLYGON_STOCKS_CONN_ID", "polygon_stocks_api")
USER_AGENT = os.getenv("HTTP_USER_AGENT", "stocks-elt/polygon-stocks-ingest-backfill/1.0")

# Grouped endpoint (daily aggregates by ticker)
POLYGON_GROUPED_URL = "https://api.polygon.io/v2/aggs/grouped/locale/us/market/stocks/{date}"

# Triggered loader DAG id
STOCKS_LOAD_DAG_ID = os.getenv("STOCKS_LOAD_DAG_ID", "polygon_stocks_load")

# ────────────────────────────────────────────────────────────────────────────────
# Helpers
# ────────────────────────────────────────────────────────────────────────────────

def _get_polygon_api_key() -> str:
    """
    Resolve API key from Airflow connection.
    - Prefer conn.password
    - Fallback to conn.extra['api_key']
    """
    conn = BaseHook.get_connection(POLYGON_STOCKS_CONN_ID)
    if conn.password:
        return conn.password
    extra = conn.extra_dejson or {}
    key = (extra.get("api_key") or extra.get("token") or "").strip()
    if not key:
        raise RuntimeError(
            f"Polygon key not found in connection '{POLYGON_STOCKS_CONN_ID}'. "
            "Put the key in the connection password or extras.api_key."
        )
    return key


def _requests_session() -> requests.Session:
    """
    Build a retrying session:
    - Retries 5x on 429/5xx with backoff
    - Short timeouts (connect=5s, read=30s)
    """
    sess = requests.Session()
    retry = Retry(
        total=5,
        backoff_factor=1.5,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    sess.mount("https://", adapter)
    sess.headers.update({"User-Agent": USER_AGENT})
    return sess


def _date_range_inclusive(start: pendulum.DateTime, end: pendulum.DateTime) -> List[str]:
    days = []
    cur = start
    while cur <= end:
        days.append(cur.to_date_string())
        cur = cur.add(days=1)
    return days


def _is_weekend(yyyy_mm_dd: str, tz: str = "America/New_York") -> bool:
    d = pendulum.parse(yyyy_mm_dd, tz=tz)
    return d.weekday() >= 5  # 5=Sat, 6=Sun


def _split_grouped_payload_to_per_ticker_files(payload: Dict) -> List[Tuple[str, Dict]]:
    """
    Convert Polygon grouped response into per-ticker mini-documents that match
    the loader's COPY SELECT expectation:

    { "ticker": "<TICKER>", "results": [ { o, h, l, c, v, n, vw, t } ] }

    Returns list of (ticker, doc) tuples.
    """
    results = payload.get("results") or []
    docs: List[Tuple[str, Dict]] = []
    for r in results:
        ticker = r.get("T") or r.get("tiker") or r.get("ticker") or r.get("TICKER")
        if not ticker:
            ticker = r.get("symbol")  # defensive
        if not ticker:
            # Skip malformed row
            continue
        # Normalize fields to lower-case keys similar to Polygon docs:
        # T (ticker), t (timestamp ms), o,h,l,c (prices), v (volume), n (transactions), vw (vwap)
        doc = {
            "ticker": ticker,
            "results": [
                {
                    "o": r.get("o"),
                    "h": r.get("h"),
                    "l": r.get("l"),
                    "c": r.get("c"),
                    "v": r.get("v"),
                    "n": r.get("n"),
                    "vw": r.get("vw"),
                    "t": r.get("t"),
                }
            ],
        }
        docs.append((ticker, doc))
    return docs


def _gzip_bytes(obj: Dict) -> bytes:
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
    description="Backfill Polygon grouped daily → per-ticker gz files + per-day manifest (no latest pointer).",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    schedule=None,  # trigger manually
    catchup=False,
    dagrun_timeout=timedelta(hours=6),
    max_active_runs=1,
    tags=["ingest", "polygon", "stocks", "backfill"],
    default_args=default_args,
    params={
        # Inclusive date range (YYYY-MM-DD)
        "date_from": Param(default=pendulum.now("UTC").subtract(days=1).to_date_string(), type="string"),
        "date_to": Param(default=pendulum.now("UTC").subtract(days=1).to_date_string(), type="string"),
        # Optional: pass through to loader when triggering (override env defaults there)
        "loader_prefilter_loaded": Param(default=True, type="boolean"),
        "loader_force": Param(default=False, type="boolean"),
        # Optional: dry-run (fetch only; do not write S3 or trigger)
        "dry_run": Param(default=False, type="boolean"),
    },
)
def polygon_stocks_ingest_backfill_dag():

    # ────────────────────────────────────────────────────────────────────────────
    # Tasks
    # ────────────────────────────────────────────────────────────────────────────

    @task(pool=API_POOL)
    def fetch_day_payload(yyyy_mm_dd: str) -> Dict:
        """
        Call Polygon grouped endpoint for a single date.
        Weekend/holiday handling:
          - 404 → skip
          - 200 with empty results → skip
        """
        if _is_weekend(yyyy_mm_dd):
            raise AirflowSkipException(f"{yyyy_mm_dd} is weekend; skipping.")

        api_key = _get_polygon_api_key()
        url = POLYGON_GROUPED_URL.format(date=yyyy_mm_dd)

        sess = _requests_session()
        resp = sess.get(url, params={"adjusted": "true", "apiKey": api_key}, timeout=(5, 30))

        if resp.status_code == 404:
            # Market holiday or missing data
            raise AirflowSkipException(f"{yyyy_mm_dd}: Polygon returned 404 (likely market holiday).")
        if resp.status_code >= 400:
            # Let retry handle transient; if it reaches here it's final failure
            raise RuntimeError(f"{yyyy_mm_dd}: HTTP {resp.status_code} from Polygon: {resp.text[:200]}")

        payload = resp.json()
        results = payload.get("results") or []
        if not results:
            raise AirflowSkipException(f"{yyyy_mm_dd}: empty 'results' from Polygon; skipping.")
        return {"date": yyyy_mm_dd, "payload": payload}

    @task
    def write_per_ticker_files(day: Dict) -> Dict:
        """
        Transform grouped payload into per-ticker gz files and upload to S3.
        Returns a dict with the date and list of raw keys written.
        """
        yyyy_mm_dd = day["date"]
        payload = day["payload"]
        docs = _split_grouped_payload_to_per_ticker_files(payload)

        if not docs:
            raise AirflowSkipException(f"{yyyy_mm_dd}: no per-ticker docs after split; skipping.")

        # Sort by ticker for deterministic manifest ordering
        docs.sort(key=lambda x: x[0])

        s3 = S3Hook()
        raw_keys: List[str] = []

        for ticker, doc in docs:
            key = f"raw/stocks/{ticker}/{yyyy_mm_dd}.json.gz"
            body = _gzip_bytes(doc)
            s3.load_bytes(
                bytes_data=body,
                key=key,
                bucket_name=BUCKET_NAME,
                replace=True,
                encrypt=True,  # SSE-S3
            )
            raw_keys.append(key)

        print(f"{yyyy_mm_dd}: wrote {len(raw_keys)} per-ticker files (sample: {raw_keys[:5]})")
        return {"date": yyyy_mm_dd, "raw_keys": raw_keys}

    @task
    def write_manifest(day_files: Dict, **context) -> Dict:
        """
        Write per-day flat manifest listing each raw key on its own line.
        Returns manifest key metadata to feed the loader trigger.
        """
        yyyy_mm_dd = day_files["date"]
        raw_keys: List[str] = day_files["raw_keys"]

        if not raw_keys:
            raise AirflowSkipException(f"{yyyy_mm_dd}: no raw keys; manifest not written.")

        # Deterministic order (already sorted); join with newline
        manifest_body = "\n".join(raw_keys) + "\n"
        manifest_key = f"raw/manifests/stocks/{yyyy_mm_dd}/manifest.txt"

        s3 = S3Hook()
        s3.load_string(
            string_data=manifest_body,
            key=manifest_key,
            bucket_name=BUCKET_NAME,
            replace=True,
            encrypt=True,
        )

        prm = context["params"]
        print(
            f"{yyyy_mm_dd}: manifest written at s3://{BUCKET_NAME}/{manifest_key} "
            f"(prefilter_loaded={prm['loader_prefilter_loaded']} force={prm['loader_force']})"
        )
        return {
            "date": yyyy_mm_dd,
            "manifest_key": manifest_key,
            "prefilter_loaded": bool(prm["loader_prefilter_loaded"]),
            "force": bool(prm["loader_force"]),
        }

    @task
    def expand_dates(**context) -> List[str]:
        """
        Compute the inclusive date list from params.
        """
        dfrom = pendulum.parse(context["params"]["date_from"]).start_of("day")
        dto = pendulum.parse(context["params"]["date_to"]).start_of("day")
        if dto < dfrom:
            raise ValueError("date_to must be >= date_from")
        dates = _date_range_inclusive(dfrom, dto)
        print(f"Backfill range: {dates[0]} → {dates[-1]} (n={len(dates)})")
        return dates

    @task
    def maybe_skip_side_effects(**context) -> bool:
        """
        If dry_run=True, signal downstream to skip S3 writes and triggers.
        """
        return bool(context["params"]["dry_run"])

    # ────────────────────────────────────────────────────────────────────────────
    # Graph
    # ────────────────────────────────────────────────────────────────────────────

    dates = expand_dates()
    dry = maybe_skip_side_effects()

    # Map over dates → fetch → write files → write manifest
    # Properly gate S3 side-effects on dry_run
    day_payloads = fetch_day_payload.expand(yyyy_mm_dd=dates).as_teardown(dry)  # fetch still runs in dry-run
    day_files = write_per_ticker_files.expand(day=day_payloads).skip_if(dry)
    manifests = write_manifest.expand(day_files=day_files).skip_if(dry)

    # One TriggerDagRun per produced manifest
    trigger_loads = TriggerDagRunOperator.partial(
        task_id="trigger_stocks_load",
        trigger_dag_id=STOCKS_LOAD_DAG_ID,
        reset_dag_run=True,           # replace if same run_id occurs
        wait_for_completion=False,    # fire-and-forget
    ).expand(
        conf=[
            {
                "manifest_key": m["manifest_key"],
                "prefilter_loaded": m["prefilter_loaded"],
                "force": m["force"],
            }
            for m in manifests
        ]
    )

    # Basic ordering: fetch → files → manifest → trigger
    day_payloads >> day_files >> manifests >> trigger_loads

# Instantiate
polygon_stocks_ingest_backfill_dag()
