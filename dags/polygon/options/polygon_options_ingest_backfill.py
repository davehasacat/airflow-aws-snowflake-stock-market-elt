# dags/polygon/options/polygon_options_ingest_backfill.py
# =============================================================================
# Polygon Options Backfill (Ingestion)
# -----------------------------------------------------------------------------
# Enterprise strategy (stream/backfill split):
#   1) S3 layout (Hive-style): raw/options/year=YYYY/month=MM/day=DD/underlying=<U>/contract=<C>.json.gz
#   2) Per-day manifests (immutable): raw/manifests/options/YYYY-MM-DD/manifest.txt
#      - Default: this backfill DAG does NOT update the "latest" pointer and thus
#        does NOT emit a Dataset (keeps stream loader idle).
#      - Opt-in: pass update_latest_pointer=true in dag_run.conf to write the pointer
#        and emit the Dataset to trigger polygon_options_load.
#   3) Contract filter: only contracts "alive" on target day (list_date ≤ day ≤ expiration_date)
#   4) Small XComs: workers return tiny {day:count} maps; manifests built via S3 listing
#   5) Retry & rate-limit hardened: Bearer auth + apiKey param, 429/5xx handling
#   6) Observability: per-(day,underlying) worklogs at raw/manifests/options/worklogs/YYYY-MM-DD/<U>.log
#   7) Encrypted S3 writes by default (encrypt=True)
# =============================================================================
from __future__ import annotations

import os
import csv
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
from airflow.operators.python import get_current_context
from airflow.exceptions import AirflowSkipException
from airflow.models.param import Param
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.hooks.base import BaseHook

from dags.utils.polygon_datasets import S3_OPTIONS_MANIFEST_DATASET

# ────────────────────────────────────────────────────────────────────────────────
# Config / Constants
# ────────────────────────────────────────────────────────────────────────────────
POLYGON_CONTRACTS_URL = "https://api.polygon.io/v3/reference/options/contracts"
POLYGON_AGGS_URL_BASE = "https://api.polygon.io/v2/aggs/ticker"

BUCKET_NAME = os.getenv("BUCKET_NAME")  # expected: stock-market-elt
DBT_PROJECT_DIR = os.getenv("DBT_PROJECT_DIR", "/usr/local/airflow/dbt")

REQUEST_TIMEOUT_SECS = int(os.getenv("HTTP_REQUEST_TIMEOUT_SECS", "60"))
USER_AGENT = os.getenv("HTTP_USER_AGENT", "stocks-elt/polygon-options-backfill (manual)")
API_POOL = os.getenv("API_POOL", "api_pool")

REQUEST_DELAY_SECONDS = float(os.getenv("POLYGON_REQUEST_DELAY_SECONDS", "0.25"))
REQUEST_JITTER_SECONDS = float(os.getenv("POLYGON_REQUEST_JITTER_SECONDS", "0.05"))
CONTRACTS_BATCH_SIZE  = int(os.getenv("BACKFILL_CONTRACTS_BATCH_SIZE", "300"))
REPLACE_FILES        = os.getenv("BACKFILL_REPLACE", "false").lower() == "true"
MAP_CHUNK_SIZE       = int(os.getenv("BACKFILL_MAP_CHUNK_SIZE", "2000"))

# Toggle the date-alive filter if needed
DISABLE_CONTRACT_DATE_FILTER = os.getenv(
    "POLYGON_DISABLE_CONTRACT_DATE_FILTER", "false"
).lower() in ("1","true","yes")

# Manifests (immutable per-day) + optional latest pointer
DAY_MANIFEST_PREFIX = os.getenv("OPTIONS_DAY_MANIFEST_PREFIX", "raw/manifests/options")
LATEST_POINTER_KEY  = os.getenv("OPTIONS_LATEST_POINTER_KEY", "raw/manifests/polygon_options_manifest_latest.txt")

if not BUCKET_NAME:
    raise RuntimeError("BUCKET_NAME env var is required (e.g., 'stock-market-elt').")

# ────────────────────────────────────────────────────────────────────────────────
# Helpers
# ────────────────────────────────────────────────────────────────────────────────
def _get_polygon_options_key() -> str:
    """
    Resolve the Polygon Options API key from Airflow Connection 'polygon_options_api_key',
    falling back to env POLYGON_OPTIONS_API_KEY.
    """
    try:
        conn = BaseHook.get_connection("polygon_options_api_key")
        if conn:
            for candidate in (conn.password, conn.login):
                if isinstance(candidate, str) and candidate.strip():
                    return candidate.strip()
            extra = (conn.extra_dejson or {})
            for k in ("api_key", "key", "token", "password", "polygon_options_api_key", "value"):
                v = extra.get(k)
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
    """
    HTTP session with pooling + retries and Authorization: Bearer <key>.
    Keep apiKey in params too (belt & suspenders).
    """
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
    GET with client-side rate limiting:
      - Honor 'Retry-After' for 429
      - Exponential backoff for 5xx
      - Respect REQUEST_TIMEOUT_SECS
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

def _batch(lst: List[str], n: int) -> List[List[str]]:
    return [lst[i : i + n] for i in range(0, len(lst), n)]

def _alive_on_day(rec: Dict[str, Any], day_iso: str) -> bool:
    """
    True if target day is within [list_date, expiration_date] when present.
    Missing fields → permissive (True).
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
        return True  # permissive on parse hiccups

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
    dag_id="polygon_options_ingest_backfill",
    description="Polygon options backfill ingest — raw JSON.gz & immutable per-day manifests (pointer update optional).",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule=None,           # manual runs only
    catchup=False,
    tags=["ingestion", "polygon", "options", "backfill", "aws"],
    params={
        "start_date": Param(default="2025-10-17", type="string", description="Backfill start date (YYYY-MM-DD)"),
        "end_date":   Param(default="2025-10-17", type="string", description="Backfill end date (YYYY-MM-DD)"),
        # Opt-in: update 'latest' pointer and emit Dataset to trigger polygon_options_load
        "update_latest_pointer": Param(default=False, type="boolean",
                                       description="When true, write 'latest' POINTER and emit Dataset"),
    },
    dagrun_timeout=timedelta(hours=36),
    max_active_runs=1,
    default_args=default_args,
)
def polygon_options_ingest_backfill_dag():

    # ───────────────────────────────
    # Inputs
    # ───────────────────────────────
    @task
    def read_underlyings() -> List[str]:
        """Seed underlyings from dbt seeds."""
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
        Compute trading dates (weekdays only) inclusive of [start_date, end_date].
        Note: market holidays are not filtered here.
        """
        start = pendulum.parse(kwargs["params"]["start_date"])
        end   = pendulum.parse(kwargs["params"]["end_date"])
        if start > end:
            raise ValueError("start_date cannot be after end_date.")
        dates: List[str] = []
        cur = start
        while cur <= end:
            if cur.day_of_week not in (5, 6):  # 5=Sat, 6=Sun
                dates.append(cur.to_date_string())
            cur = cur.add(days=1)
        if not dates:
            raise AirflowSkipException("No weekdays in the given range.")
        return dates

    @task
    def make_pairs(underlyings: List[str], trading_days: List[str]) -> List[Dict[str, str]]:
        """Cartesian of underlyings × days."""
        return [{"ticker": u, "target_date": d} for d in trading_days for u in underlyings]

    # ───────────────────────────────
    # Discovery: contracts per (underlying, day)
    # ───────────────────────────────
    @task(retries=3, retry_delay=pendulum.duration(minutes=5), pool=API_POOL)
    def discover_for_pair(pair: Dict[str, str]) -> Dict[str, Any]:
        """
        Discover contracts for (underlying, target_date) and filter to those
        alive on the date (when list/expiration dates exist).
        Returns: {'underlying','target_date','contracts':[tickers...]}
        """
        underlying = pair["ticker"]
        target_date = pair["target_date"]
        api_key = _get_polygon_options_key()
        sess = _session(api_key)

        params = {
            "underlying_ticker": underlying,
            "expiration_date.gte": target_date,
            "as_of": target_date,
            "limit": 1000,
            "apiKey": api_key,  # keep in params too
        }
        all_recs: List[Dict[str, Any]] = []

        # First page
        resp = _rate_limited_get(sess, POLYGON_CONTRACTS_URL, params)
        if resp.status_code == 401:
            time.sleep(0.5)
            resp = _rate_limited_get(sess, POLYGON_CONTRACTS_URL, params)
        resp.raise_for_status()
        data = resp.json() or {}
        all_recs.extend([r for r in (data.get("results") or []) if r and r.get("ticker")])

        # Cursor pages
        next_url = data.get("next_url")
        while next_url:
            resp = _rate_limited_get(sess, next_url, {"apiKey": api_key})
            if resp.status_code == 401:
                time.sleep(0.5)
                resp = _rate_limited_get(sess, next_url, {"apiKey": api_key})
            resp.raise_for_status()
            p = resp.json() or {}
            all_recs.extend([r for r in (p.get("results") or []) if r and r.get("ticker")])
            next_url = p.get("next_url")
            time.sleep(REQUEST_DELAY_SECONDS + REQUEST_JITTER_SECONDS)

        filtered = [r for r in all_recs if _alive_on_day(r, target_date)]
        contracts = sorted({r["ticker"] for r in filtered})

        return {"underlying": underlying, "target_date": target_date, "contracts": contracts}

    @task
    def batch_contracts_for_pair(discovery: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Split discovered contracts into batches for the fetch step."""
        underlying  = discovery["underlying"]
        target_date = discovery["target_date"]
        contracts: List[str] = discovery.get("contracts") or []
        if not contracts:
            return []
        batches = _batch(contracts, CONTRACTS_BATCH_SIZE)
        return [{"underlying": underlying, "target_date": target_date, "contract_batch": b} for b in batches]

    @task
    def flatten_batch_specs(nested: List[List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
        return [d for sub in (nested or []) for d in (sub or [])]

    @task
    def chunk_batch_specs(batch_specs: List[Dict[str, Any]]) -> List[List[Dict[str, Any]]]:
        """Optional: map-of-maps chunking to prevent extremely large mapped graphs."""
        if not batch_specs:
            return []
        sz = MAP_CHUNK_SIZE
        return [batch_specs[i:i+sz] for i in range(0, len(batch_specs), sz)]

    # ───────────────────────────────
    # Fetch bars & write raw files (only when non-empty)
    # ───────────────────────────────
    @task(retries=3, retry_delay=pendulum.duration(minutes=5), pool=API_POOL)
    def process_chunk(specs: List[Dict[str, Any]]) -> Dict[str, int]:
        """
        Process a chunk of batch-specs and return a tiny map of {day_iso: wrote_count}.
        Also writes per-(day, underlying) worklogs to S3 for observability.
        """
        if not specs:
            return {}

        api_key = _get_polygon_options_key()
        sess = _session(api_key)
        s3 = S3Hook()

        counts_by_day: Dict[str, int] = {}
        # For worklogs: {(iso, underlying): {attempted, wrote, empty, err_404, err_other}}
        wl: Dict[Tuple[str, str], Dict[str, int]] = {}

        def bump(iso: str, und: str, field: str, inc: int = 1):
            d = wl.setdefault((iso, und), {"attempted":0, "wrote":0, "empty":0, "err_404":0, "err_other":0})
            d[field] += inc

        for item in specs:
            underlying  = item["underlying"]
            target_date = item["target_date"]
            yyyy, mm, dd = target_date.split("-")
            wrote = 0

            for contract in (item.get("contract_batch") or []):
                bump(target_date, underlying, "attempted")
                s3_key = (
                    f"raw/options/year={yyyy}/month={mm}/day={dd}/"
                    f"underlying={underlying}/contract={contract}.json.gz"
                )

                if not REPLACE_FILES and s3.check_for_key(s3_key, bucket_name=BUCKET_NAME):
                    # Treat existing as already "wrote" for sane accounting
                    wrote += 1
                    bump(target_date, underlying, "wrote")
                    continue

                url = f"{POLYGON_AGGS_URL_BASE}/{contract}/range/1/day/{target_date}/{target_date}"
                params = {"adjusted": "true", "apiKey": api_key}

                try:
                    resp = _rate_limited_get(sess, url, params)
                    if resp.status_code == 401:
                        time.sleep(0.5)
                        resp = _rate_limited_get(sess, url, params)

                    if resp.status_code == 404:
                        bump(target_date, underlying, "err_404")
                        continue

                    resp.raise_for_status()
                    obj = resp.json() or {}
                    results = obj.get("results") or []
                    if not results:
                        bump(target_date, underlying, "empty")
                        continue

                    buf = BytesIO()
                    with gzip.GzipFile(filename="", mode="wb", fileobj=buf) as gz:
                        gz.write(json.dumps(obj, separators=(",", ":")).encode("utf-8"))

                    # Encrypted write for enterprise defaults
                    s3.load_bytes(
                        buf.getvalue(),
                        key=s3_key,
                        bucket_name=BUCKET_NAME,
                        replace=True,
                        encrypt=True,
                    )

                    wrote += 1
                    bump(target_date, underlying, "wrote")

                except requests.RequestException:
                    bump(target_date, underlying, "err_other")
                    continue

                time.sleep(REQUEST_DELAY_SECONDS + REQUEST_JITTER_SECONDS)

            if wrote:
                counts_by_day[target_date] = counts_by_day.get(target_date, 0) + wrote

        # Write worklogs (encrypted)
        for (iso, und), stats in wl.items():
            log_key = f"{DAY_MANIFEST_PREFIX}/worklogs/{iso}/{und}.log"
            header = (
                f"# as_of={iso} underlying={und}\n"
                f"# attempted={stats['attempted']} wrote={stats['wrote']} "
                f"empty={stats['empty']} err_404={stats['err_404']} err_other={stats['err_other']}\n"
            )
            s3.load_string(header, key=log_key, bucket_name=BUCKET_NAME, replace=True, encrypt=True)

        return counts_by_day

    # ───────────────────────────────
    # Build immutable per-day manifests (via S3 listing)
    # ───────────────────────────────
    @task
    def merge_day_counts(list_of_maps: List[Dict[str, int]]) -> Dict[str, int]:
        out: Dict[str, int] = {}
        for m in (list_of_maps or []):
            for k, v in (m or {}).items():
                out[k] = out.get(k, 0) + int(v or 0)
        return out

    @task
    def write_day_manifests_from_s3(trading_days: List[str], counts_by_day: Dict[str, int]) -> List[str]:
        """
        Build per-day manifests by LISTING S3 prefixes (no big XComs).
        Only write a manifest if the day has files.
        Writes to: raw/manifests/options/YYYY-MM-DD/manifest.txt
        """
        s3 = S3Hook()
        manifest_keys: List[str] = []

        for iso in sorted(trading_days):
            y, m, d = iso.split("-")
            prefix = f"raw/options/year={y}/month={m}/day={d}/"
            keys = s3.list_keys(bucket_name=BUCKET_NAME, prefix=prefix) or []
            keys = [k for k in keys if k and k.startswith(prefix)]
            if not keys:
                continue

            mkey = f"{DAY_MANIFEST_PREFIX}/{iso}/manifest.txt"
            body = "\n".join(sorted(set(keys))) + "\n"
            s3.load_string(body, key=mkey, bucket_name=BUCKET_NAME, replace=True, encrypt=True)
            manifest_keys.append(mkey)

        if not manifest_keys:
            raise AirflowSkipException("No non-empty per-day manifests produced.")
        return manifest_keys

    # ───────────────────────────────
    # Optional: update "latest" pointer and emit Dataset (opt-in)
    # ───────────────────────────────
    @task(outlets=[S3_OPTIONS_MANIFEST_DATASET])
    def maybe_update_latest_pointer(manifest_keys: List[str]) -> None:
        """
        Optionally update the mutable 'latest' POINTER so downstream loaders see a dataset event.
        Controlled by dag_run.conf/params.update_latest_pointer (default False).
        - If False: task SKIPs (no Dataset event; loaders won't trigger).
        - If True:  writes POINTER=<latest per-day manifest> and SUCCEEDS (emits Dataset).
        """
        ctx = get_current_context()
        do_update = bool(ctx["params"].get("update_latest_pointer"))

        if not do_update:
            # Skip on purpose so we do NOT emit an event or alter the pointer.
            raise AirflowSkipException("update_latest_pointer=False — not updating pointer.")

        if not manifest_keys:
            # Nothing to point to → skip (no event emission)
            raise AirflowSkipException("No per-day manifests; leaving latest pointer unchanged.")

        latest_mkey = sorted(manifest_keys)[-1]
        s3 = S3Hook()
        pointer_body = f"POINTER={latest_mkey}\n"
        s3.load_string(pointer_body, key=LATEST_POINTER_KEY, bucket_name=BUCKET_NAME,
                       replace=True, encrypt=True)
        print(f"✅ Updated latest pointer: s3://{BUCKET_NAME}/{LATEST_POINTER_KEY} → {latest_mkey}")

    # ───────────────────────────────
    # Wiring
    # ───────────────────────────────
    underlyings   = read_underlyings()
    trading_days  = compute_trading_dates()
    pairs         = make_pairs(underlyings, trading_days)

    discoveries        = discover_for_pair.expand(pair=pairs)
    batch_specs_nested = batch_contracts_for_pair.expand(discovery=discoveries)
    batch_specs_flat   = flatten_batch_specs(batch_specs_nested)
    chunked_specs      = chunk_batch_specs(batch_specs_flat)

    # Workers return tiny {day_iso: count} maps and write worklogs
    day_counts_list    = process_chunk.expand(specs=chunked_specs)
    counts_by_day      = merge_day_counts(day_counts_list)

    # Build immutable per-day manifests; optionally update pointer (opt-in)
    day_manifests      = write_day_manifests_from_s3(trading_days, counts_by_day)
    _ = maybe_update_latest_pointer(day_manifests)


# Instantiate
polygon_options_ingest_backfill_dag()
