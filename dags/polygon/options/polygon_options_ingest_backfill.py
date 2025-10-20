# dags/polygon/options/polygon_options_ingest_backfill.py
# =============================================================================
# Polygon Options Backfill (Ingestion)
# -----------------------------------------------------------------------------
# Updates applied:
#   1) S3 layout: raw/options/year=YYYY/month=MM/day=DD/underlying=<U>/contract=<C>.json.gz
#   2) Per-day manifests at raw/manifests/options/YYYY-MM-DD.txt + "latest" pointer
#   4) Filter contracts to those alive on target day (list_date ≤ day ≤ expiration_date)
# =============================================================================
from __future__ import annotations

import csv
import json
import os
import time
import gzip
from io import BytesIO
from typing import List, Dict, Any

import pendulum
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from datetime import timedelta

from airflow.decorators import dag, task
from airflow.exceptions import AirflowSkipException
from airflow.models.param import Param
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from dags.utils.polygon_datasets import S3_OPTIONS_MANIFEST_DATASET

# ────────────────────────────────────────────────────────────────────────────────
# Config / Constants
# ────────────────────────────────────────────────────────────────────────────────
POLYGON_CONTRACTS_URL = "https://api.polygon.io/v3/reference/options/contracts"
POLYGON_AGGS_URL_BASE = "https://api.polygon.io/v2/aggs/ticker"

BUCKET_NAME = os.getenv("BUCKET_NAME", "test")
DBT_PROJECT_DIR = os.getenv("DBT_PROJECT_DIR", "/usr/local/airflow/dbt")

REQUEST_TIMEOUT_SECS = int(os.getenv("HTTP_REQUEST_TIMEOUT_SECS", "60"))
USER_AGENT = os.getenv("HTTP_USER_AGENT", "stocks-elt/polygon-options-backfill (manual)")
API_POOL = os.getenv("API_POOL", "api_pool")

REQUEST_DELAY_SECONDS = float(os.getenv("POLYGON_REQUEST_DELAY_SECONDS", "0.25"))
CONTRACTS_BATCH_SIZE  = int(os.getenv("BACKFILL_CONTRACTS_BATCH_SIZE", "300"))
REPLACE_FILES        = os.getenv("BACKFILL_REPLACE", "false").lower() == "true"
MAP_CHUNK_SIZE       = int(os.getenv("BACKFILL_MAP_CHUNK_SIZE", "2000"))

# Allow toggling the date-alive filter if needed
DISABLE_CONTRACT_DATE_FILTER = os.getenv("POLYGON_DISABLE_CONTRACT_DATE_FILTER", "false").lower() in ("1","true","yes")

# ────────────────────────────────────────────────────────────────────────────────
# Helpers
# ────────────────────────────────────────────────────────────────────────────────
def _get_polygon_options_key() -> str:
    """
    Resolve the Polygon Options API key (connection → variable → env).
    Connection ID: polygon_options_api_key
    """
    # 1) Airflow Connection
    try:
        from airflow.hooks.base import BaseHook
        conn = BaseHook.get_connection("polygon_options_api_key")
        if conn:
            if conn.password and conn.password.strip():
                return conn.password.strip()
            extra = (conn.extra_dejson or {})
            v = extra.get("api_key")
            if isinstance(v, str) and v.strip():
                return v.strip()
    except Exception:
        pass

    # 2) Airflow Variable (string or tiny JSON)
    try:
        from airflow.models import Variable
        raw = Variable.get("polygon_options_api_key")
        if raw:
            raw = raw.strip()
            if raw.startswith("{"):
                import json as _json
                try:
                    obj = _json.loads(raw)
                    if isinstance(obj, dict):
                        for k in ("polygon_options_api_key", "api_key", "key", "value"):
                            vv = obj.get(k)
                            if isinstance(vv, str) and vv.strip():
                                return vv.strip()
                        if len(obj) == 1:
                            only = next(iter(obj.values()))
                            if isinstance(only, str) and only.strip():
                                return only.strip()
                except Exception:
                    pass
            return raw
    except Exception:
        pass

    # 3) ENV
    env = os.getenv("POLYGON_OPTIONS_API_KEY", "").strip()
    if env:
        return env

    raise RuntimeError(
        "Polygon Options API key not found. Provide via Airflow connection 'polygon_options_api_key', "
        "Variable 'polygon_options_api_key', or env POLYGON_OPTIONS_API_KEY."
    )

def _session() -> requests.Session:
    s = requests.Session()
    retries = Retry(
        total=6,
        backoff_factor=1.2,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=frozenset(["GET"]),
        raise_on_status=False,
    )
    s.mount("https://", HTTPAdapter(max_retries=retries, pool_connections=32, pool_maxsize=32))
    s.headers.update({"User-Agent": USER_AGENT})
    return s

def _rate_limited_get(sess: requests.Session, url: str, params: dict | None, max_tries: int = 6) -> requests.Response:
    backoff = 1.5
    delay = 1.0
    tries = 0
    while True:
        resp = sess.get(url, params=params, timeout=REQUEST_TIMEOUT_SECS)
        if resp.status_code == 429:
            retry_after = resp.headers.get("Retry-After")
            sleep_s = float(retry_after) if retry_after else delay
            time.sleep(sleep_s)
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
@dag(
    dag_id="polygon_options_ingest_backfill",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["ingestion", "polygon", "options", "backfill", "aws"],
    params={
        "start_date": Param(default="2025-10-09", type="string", description="Backfill start date (YYYY-MM-DD)"),
        "end_date":   Param(default="2025-10-10", type="string", description="Backfill end date (YYYY-MM-DD)"),
    },
    dagrun_timeout=timedelta(hours=36),
    max_active_runs=1,
)
def polygon_options_ingest_backfill_dag():

    @task
    def read_underlyings() -> List[str]:
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
        start = pendulum.parse(kwargs["params"]["start_date"])
        end   = pendulum.parse(kwargs["params"]["end_date"])
        if start > end:
            raise ValueError("start_date cannot be after end_date.")
        dates: List[str] = []
        cur = start
        while cur <= end:
            if cur.day_of_week not in (5, 6):  # Sat/Sun
                dates.append(cur.to_date_string())
            cur = cur.add(days=1)
        if not dates:
            raise AirflowSkipException("No trading dates in the given range.")
        return dates

    @task
    def make_pairs(underlyings: List[str], trading_days: List[str]) -> List[Dict[str, str]]:
        return [{"ticker": u, "target_date": d} for d in trading_days for u in underlyings]

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
        sess = _session()

        params = {
            "underlying_ticker": underlying,
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
            data = resp.json() or {}
            all_recs.extend([r for r in (data.get("results") or []) if r and r.get("ticker")])
            next_url = data.get("next_url")
            time.sleep(REQUEST_DELAY_SECONDS)

        # Update 4: filter to alive contracts for target_date
        filtered = [r for r in all_recs if _alive_on_day(r, target_date)]
        contracts = sorted({r["ticker"] for r in filtered})

        return {"underlying": underlying, "target_date": target_date, "contracts": contracts}

    @task
    def batch_contracts_for_pair(discovery: Dict[str, Any]) -> List[Dict[str, Any]]:
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
        if not batch_specs:
            return []
        sz = MAP_CHUNK_SIZE
        return [batch_specs[i:i+sz] for i in range(0, len(batch_specs), sz)]

    @task(retries=3, retry_delay=pendulum.duration(minutes=5), pool=API_POOL)
    def process_chunk(specs: List[Dict[str, Any]]) -> List[str]:
        if not specs:
            return []

        api_key = _get_polygon_options_key()
        sess = _session()
        s3 = S3Hook()

        written: List[str] = []
        for item in specs:
            underlying  = item["underlying"]
            target_date = item["target_date"]
            yyyy, mm, dd = target_date.split("-")
            for contract in (item.get("contract_batch") or []):
                key = f"raw/options/year={yyyy}/month={mm}/day={dd}/underlying={underlying}/contract={contract}.json.gz"

                if not REPLACE_FILES and s3.check_for_key(key, bucket_name=BUCKET_NAME):
                    continue

                url = f"{POLYGON_AGGS_URL_BASE}/{contract}/range/1/day/{target_date}/{target_date}"
                params = {"adjusted": "true", "apiKey": api_key}
                resp = _rate_limited_get(sess, url, params)
                if resp.status_code == 404:
                    continue
                resp.raise_for_status()

                obj = resp.json() or {}
                results = obj.get("results") or []
                if not results:
                    continue

                buf = BytesIO()
                with gzip.GzipFile(filename="", mode="wb", fileobj=buf) as gz:
                    gz.write(json.dumps(obj, separators=(",", ":")).encode("utf-8"))
                s3.load_bytes(buf.getvalue(), key=key, bucket_name=BUCKET_NAME, replace=True)
                written.append(key)

                time.sleep(REQUEST_DELAY_SECONDS)

        return written

    @task
    def flatten_str_lists(list_of_lists: List[List[str]]) -> List[str]:
        return [k for sub in (list_of_lists or []) for k in (sub or []) if k]

    @task
    def write_day_manifests(all_keys: List[str]) -> List[str]:
        """
        Write per-day manifests at raw/manifests/options/YYYY-MM-DD.txt
        based on keys actually written.
        """
        if not all_keys:
            return []

        by_day: dict[str, list[str]] = {}
        for k in all_keys:
            parts = (k or "").split("/")
            if len(parts) < 6 or parts[0] != "raw" or parts[1] != "options":
                continue
            y = parts[3].split("=")[-1]
            m = parts[4].split("=")[-1]
            d = parts[5].split("=")[-1]
            iso = f"{y}-{m}-{d}"
            by_day.setdefault(iso, []).append(k)

        s3 = S3Hook()
        manifest_keys: List[str] = []
        for iso, keys in sorted(by_day.items()):
            if not keys:
                continue
            mkey = f"raw/manifests/options/{iso}.txt"
            s3.load_string("\n".join(sorted(set(keys))) + "\n",
                           key=mkey, bucket_name=BUCKET_NAME, replace=True)
            manifest_keys.append(mkey)
        return manifest_keys

    @task(outlets=[S3_OPTIONS_MANIFEST_DATASET])
    def update_latest_pointer(manifest_keys: List[str]) -> None:
        if not manifest_keys:
            raise AirflowSkipException("No non-empty per-day manifests; skipping latest pointer update.")
        latest_mkey = sorted(manifest_keys)[-1]
        s3 = S3Hook()
        content = s3.read_key(key=latest_mkey, bucket_name=BUCKET_NAME) or ""
        if not content.strip():
            raise AirflowSkipException(f"Latest day manifest {latest_mkey} is empty.")
        latest_ptr = "raw/manifests/polygon_options_manifest_latest.txt"
        s3.load_string(content, key=latest_ptr, bucket_name=BUCKET_NAME, replace=True)

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
    written_lists      = process_chunk.expand(specs=chunked_specs)
    all_keys           = flatten_str_lists(written_lists)

    day_manifests = write_day_manifests(all_keys)
    update_latest_pointer(day_manifests)

# Instantiate
polygon_options_ingest_backfill_dag()
