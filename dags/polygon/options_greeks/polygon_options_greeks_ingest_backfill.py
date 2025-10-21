# =============================================================================
# Polygon Options Greeks Backfill (Ingestion) — raw in / raw out
# -----------------------------------------------------------------------------
# Outputs:
#  - S3 key per (day, underlying):
#      raw/options_greeks/year=YYYY/month=MM/day=DD/underlying=<U>/chain.json.gz
#  - Per-day manifests:
#      raw/manifests/options_greeks/YYYY-MM-DD.txt
#  - "Latest" pointer (content of most recent non-empty day):
#      raw/manifests/polygon_options_greeks_manifest_latest.txt
#
# Standards aligned with options backfill:
#  - Retries with 429/5xx handling, Bearer + apiKey param
#  - Chunked parallelization via map, bounded pool
#  - Skip empty/404 payloads (write only when results/options exist)
#  - XCom-safe (workers return tiny maps), manifests via S3 listing
#  - Observability worklogs per (day, underlying)
#  - Triggers downstream loads via Dataset
# =============================================================================

from __future__ import annotations

import os
import json
import time
import gzip
from io import BytesIO
from typing import List, Dict, Any, Tuple
from datetime import timedelta

import pendulum
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from airflow.decorators import dag, task
from airflow.exceptions import AirflowSkipException, AirflowFailException
from airflow.models.param import Param
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.hooks.base import BaseHook

from dags.utils.polygon_datasets import S3_OPTIONS_GREEKS_MANIFEST_DATASET

# ────────────────────────────────────────────────────────────────────────────────
# Config / Constants
# ────────────────────────────────────────────────────────────────────────────────
# Snapshot chain endpoint: /v3/snapshot/options/{underlying}?date=YYYY-MM-DD
POLYGON_GREEKS_BASE = "https://api.polygon.io/v3/snapshot/options"

BUCKET_NAME = os.getenv("BUCKET_NAME")  # e.g., 'stock-market-elt'
DBT_PROJECT_DIR = os.getenv("DBT_PROJECT_DIR", "/usr/local/airflow/dbt")

REQUEST_TIMEOUT_SECS = int(os.getenv("HTTP_REQUEST_TIMEOUT_SECS", "60"))
USER_AGENT = os.getenv("HTTP_USER_AGENT", "stocks-elt/polygon-options-greeks-backfill (manual)")
API_POOL = os.getenv("API_POOL", "api_pool")

REQUEST_DELAY_SECONDS = float(os.getenv("POLYGON_REQUEST_DELAY_SECONDS", "0.25"))
REQUEST_JITTER_SECONDS = float(os.getenv("POLYGON_REQUEST_JITTER_SECONDS", "0.05"))
REPLACE_FILES = os.getenv("BACKFILL_GREEKS_REPLACE", "false").lower() == "true"

# Concurrency shaping for mapped tasks (count of (day, underlying) per chunk)
MAP_CHUNK_SIZE = int(os.getenv("BACKFILL_GREEKS_MAP_CHUNK_SIZE", "1000"))

if not BUCKET_NAME:
    raise RuntimeError("BUCKET_NAME env var is required (e.g., 'stock-market-elt').")

# ────────────────────────────────────────────────────────────────────────────────
# Helpers
# ────────────────────────────────────────────────────────────────────────────────
def _get_polygon_options_key() -> str:
    """
    Get Polygon API key from Airflow Connection 'polygon_options_api_key' (password/login/extras),
    else env POLYGON_OPTIONS_API_KEY.
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

    raise RuntimeError(
        "Polygon Options API key not found. Define Airflow connection 'polygon_options_api_key' "
        "or set env POLYGON_OPTIONS_API_KEY."
    )

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

def _rate_limited_get(sess: requests.Session, url: str, params: dict | None, max_tries: int = 6) -> requests.Response:
    """
    GET with explicit handling for 429/5xx and a soft 401 retry.
    """
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

def _sleep():
    import random
    time.sleep(REQUEST_DELAY_SECONDS + random.uniform(0, REQUEST_JITTER_SECONDS))

def _batch(lst: List[Any], n: int) -> List[List[Any]]:
    return [lst[i:i+n] for i in range(0, len(lst), n)]

# ────────────────────────────────────────────────────────────────────────────────
# DAG
# ────────────────────────────────────────────────────────────────────────────────
@dag(
    dag_id="polygon_options_greeks_ingest_backfill",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["ingestion", "polygon", "options", "greeks", "backfill", "aws"],
    params={
        "start_date": Param(default="2025-10-09", type="string", description="Backfill start date (YYYY-MM-DD)"),
        "end_date":   Param(default="2025-10-10", type="string", description="Backfill end date (YYYY-MM-DD)"),
    },
    dagrun_timeout=timedelta(hours=36),
    max_active_runs=1,
)
def polygon_options_greeks_ingest_backfill_dag():

    # ───────────────────────────────
    # Inputs
    # ───────────────────────────────
    @task
    def read_underlyings() -> List[str]:
        """
        Read the seed underlyings list (dbt/seeds/custom_tickers.csv).
        """
        import csv, os
        path = os.path.join(DBT_PROJECT_DIR, "seeds", "custom_tickers.csv")
        tickers: List[str] = []
        with open(path, mode="r") as f:
            reader = csv.DictReader(f)
            for row in reader:
                t = (row.get("ticker") or "").strip().upper()
                if t:
                    tickers.append(t)
        if not tickers:
            raise AirflowSkipException("No underlyings found in seeds/custom_tickers.csv")
        return tickers

    @task
    def compute_trading_dates(**kwargs) -> List[str]:
        """
        Build inclusive [start_date, end_date] list skipping weekends.
        """
        start = pendulum.parse(kwargs["params"]["start_date"]).date()
        end   = pendulum.parse(kwargs["params"]["end_date"]).date()
        if start > end:
            raise AirflowFailException("start_date cannot be after end_date.")
        dates: List[str] = []
        cur = pendulum.date(start.year, start.month, start.day)
        while cur <= end:
            if cur.day_of_week not in (5, 6):  # 5=Sat, 6=Sun
                dates.append(cur.to_date_string())
            cur = cur.add(days=1)
        if not dates:
            raise AirflowSkipException("No trading dates in the given range.")
        return dates

    @task
    def make_pairs(underlyings: List[str], trading_days: List[str]) -> List[Dict[str, str]]:
        """
        Cross join (underlying, day) → one fetch per pair.
        """
        return [{"underlying": u, "target_date": d} for d in trading_days for u in underlyings]

    @task
    def chunk_pairs(pairs: List[Dict[str, str]]) -> List[List[Dict[str, str]]]:
        """
        Chunk to limit concurrent mapped tasks and control memory/connection pressure.
        """
        if not pairs:
            return []
        return _batch(pairs, MAP_CHUNK_SIZE)

    # ───────────────────────────────
    # Fetch
    # ───────────────────────────────
    @task
    def process_chunk(pairs_chunk: List[Dict[str, str]]) -> Dict[str, int]:
        """
        Download snapshot chain for each (day, underlying). Writes only when results/options exist.
        Returns compact map {day_iso: wrote_count} for XCom safety.
        Also writes per-(day, underlying) worklogs.
        """
        if not pairs_chunk:
            return {}

        api_key = _get_polygon_options_key()
        sess = _session(api_key)
        s3 = S3Hook()

        counts_by_day: Dict[str, int] = {}
        # {(iso, underlying): {attempted, wrote, empty, err_404, err_other}}
        wl: Dict[Tuple[str, str], Dict[str, int]] = {}

        def bump(iso: str, und: str, field: str, inc: int = 1):
            d = wl.setdefault((iso, und), {"attempted":0, "wrote":0, "empty":0, "err_404":0, "err_other":0})
            d[field] += inc

        for item in pairs_chunk:
            underlying = item["underlying"]
            target_date = item["target_date"]
            yyyy, mm, dd = target_date.split("-")
            s3_key = f"raw/options_greeks/year={yyyy}/month={mm}/day={dd}/underlying={underlying}/chain.json.gz"

            bump(target_date, underlying, "attempted")

            if not REPLACE_FILES and s3.check_for_key(s3_key, bucket_name=BUCKET_NAME):
                bump(target_date, underlying, "wrote")
                counts_by_day[target_date] = counts_by_day.get(target_date, 0) + 1
                continue

            url = f"{POLYGON_GREEKS_BASE}/{underlying}"
            params = {
                "date": target_date,
                "include_greeks": "true",
                "limit": 1000,
                "apiKey": api_key,
            }

            try:
                resp = _rate_limited_get(sess, url, params)
                if resp.status_code == 404:
                    bump(target_date, underlying, "err_404")
                    _sleep()
                    continue
                resp.raise_for_status()
                obj = resp.json() or {}

                # Chain payloads vary across plans; accept either shape.
                results = obj.get("results") or obj.get("options") or []
                if not results:
                    bump(target_date, underlying, "empty")
                    _sleep()
                    continue

                buf = BytesIO()
                with gzip.GzipFile(filename="", mode="wb", fileobj=buf) as gz:
                    gz.write(json.dumps(obj, separators=(",", ":")).encode("utf-8"))
                s3.load_bytes(buf.getvalue(), key=s3_key, bucket_name=BUCKET_NAME, replace=True)

                bump(target_date, underlying, "wrote")
                counts_by_day[target_date] = counts_by_day.get(target_date, 0) + 1

            except requests.RequestException:
                bump(target_date, underlying, "err_other")
                # continue to next pair

            _sleep()

        # Write worklogs
        for (iso, und), stats in wl.items():
            log_key = f"raw/manifests/options_greeks/worklogs/{iso}/{und}.log"
            header = (
                f"# as_of={iso} underlying={und}\n"
                f"# attempted={stats['attempted']} wrote={stats['wrote']} "
                f"empty={stats['empty']} err_404={stats['err_404']} err_other={stats['err_other']}\n"
            )
            s3.load_string(header, key=log_key, bucket_name=BUCKET_NAME, replace=True)

        return counts_by_day

    @task
    def merge_day_counts(list_of_maps: List[Dict[str, int]]) -> Dict[str, int]:
        """
        Merge tiny {day: count} maps from workers.
        """
        out: Dict[str, int] = {}
        for m in (list_of_maps or []):
            for k, v in (m or {}).items():
                out[k] = out.get(k, 0) + int(v or 0)
        return out

    # ───────────────────────────────
    # Manifests
    # ───────────────────────────────
    @task
    def write_day_manifests_from_s3(trading_days: List[str], counts_by_day: Dict[str, int]) -> List[str]:
        """
        Build per-day manifests (listing S3 to avoid large XComs). Only write for days with files.
        """
        s3 = S3Hook()
        manifest_keys: List[str] = []

        for iso in sorted(trading_days):
            y, m, d = iso.split("-")
            prefix = f"raw/options_greeks/year={y}/month={m}/day={d}/"
            keys = s3.list_keys(bucket_name=BUCKET_NAME, prefix=prefix) or []
            keys = [k for k in keys if k and k.startswith(prefix)]
            if not keys:
                continue

            mkey = f"raw/manifests/options_greeks/{iso}.txt"
            s3.load_string("\n".join(sorted(set(keys))) + "\n",
                           key=mkey, bucket_name=BUCKET_NAME, replace=True)
            manifest_keys.append(mkey)

        return manifest_keys

    @task(outlets=[S3_OPTIONS_GREEKS_MANIFEST_DATASET])
    def update_latest_pointer(manifest_keys: List[str]) -> None:
        """
        Update the latest pointer with the content of the most recent non-empty day manifest.
        """
        if not manifest_keys:
            raise AirflowSkipException("No non-empty per-day manifests; skipping latest pointer update.")
        latest_mkey = sorted(manifest_keys)[-1]
        s3 = S3Hook()
        content = s3.read_key(key=latest_mkey, bucket_name=BUCKET_NAME) or ""
        if not content.strip():
            raise AirflowSkipException(f"Latest day manifest {latest_mkey} is empty.")
        latest_ptr = "raw/manifests/polygon_options_greeks_manifest_latest.txt"
        s3.load_string(content, key=latest_ptr, bucket_name=BUCKET_NAME, replace=True)

    # ───────────────────────────────
    # Wiring
    # ───────────────────────────────
    underlyings = read_underlyings()
    trading_days = compute_trading_dates()

    pairs = make_pairs(underlyings, trading_days)
    chunks = chunk_pairs(pairs)

    maps = process_chunk.expand(pairs_chunk=chunks)
    counts_by_day = merge_day_counts(maps)

    day_manifests = write_day_manifests_from_s3(trading_days, counts_by_day)
    update_latest_pointer(day_manifests)

# Instantiate
polygon_options_greeks_ingest_backfill_dag()
