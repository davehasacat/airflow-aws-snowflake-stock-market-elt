# dags/polygon/options/polygon_options_ingest_backfill.py
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
from airflow.models import Variable  # Secrets Manager-backed Variables

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
API_POOL = os.getenv("API_POOL", "api_pool")  # ensure this pool exists (airflow_settings.yaml)

# Rate limiting / batching
REQUEST_DELAY_SECONDS = float(os.getenv("POLYGON_REQUEST_DELAY_SECONDS", "0.25"))
CONTRACTS_BATCH_SIZE = int(os.getenv("BACKFILL_CONTRACTS_BATCH_SIZE", "300"))
REPLACE_FILES = os.getenv("BACKFILL_REPLACE", "false").lower() == "true"  # default: skip existing

# ────────────────────────────────────────────────────────────────────────────────
# Helpers
# ────────────────────────────────────────────────────────────────────────────────
def _get_polygon_options_key() -> str:
    # 1) JSON variable
    try:
        v = Variable.get("polygon_options_api_key", deserialize_json=True)
        if isinstance(v, dict):
            for k in ("polygon_options_api_key", "api_key", "key", "value"):
                s = v.get(k)
                if isinstance(s, str) and s.strip():
                    return s.strip()
            if len(v) == 1:
                s = next(iter(v.values()))
                if isinstance(s, str) and s.strip():
                    return s.strip()
    except Exception:
        pass
    # 2) Plain or JSON-ish string
    try:
        s = Variable.get("polygon_options_api_key")
        if s:
            s = s.strip()
            if s.startswith("{"):
                try:
                    obj = json.loads(s)
                    if isinstance(obj, dict):
                        for k in ("polygon_options_api_key", "api_key", "key", "value"):
                            v2 = obj.get(k)
                            if isinstance(v2, str) and v2.strip():
                                return v2.strip()
                        if len(obj) == 1:
                            v2 = next(iter(obj.values()))
                            if isinstance(v2, str) and v2.strip():
                                return v2.strip()
                except Exception:
                    pass
            return s
    except Exception:
        pass
    # 3) Env fallback
    env = os.getenv("POLYGON_OPTIONS_API_KEY", "").strip()
    if env:
        return env
    raise RuntimeError(
        "Polygon Options API key not found. Create secret 'airflow/variables/polygon_options_api_key' "
        "in AWS Secrets Manager (plain text), or set env POLYGON_OPTIONS_API_KEY."
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

# ────────────────────────────────────────────────────────────────────────────────
# DAG
# ────────────────────────────────────────────────────────────────────────────────
@dag(
    dag_id="polygon_options_ingest_backfill",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule=None,  # manual only
    catchup=False,
    tags=["ingestion", "polygon", "options", "backfill", "aws"],
    params={
        "start_date": Param(default="2023-10-01", type="string", description="Backfill start date (YYYY-MM-DD)"),
        "end_date":   Param(default="2025-10-01", type="string", description="Backfill end date (YYYY-MM-DD)"),
    },
    dagrun_timeout=timedelta(hours=36),
    max_active_runs=1,
)
def polygon_options_ingest_backfill_dag():
    """
    Optimized backfill without nested mapping:
      - Build flat (ticker, date) pairs → map over pairs
      - Discover contracts per pair
      - Batch contracts per pair
      - Fetch gzip bars per contract per date
      - Write a single manifest of all written keys
      - Raw-in/raw-out, no parsing
    """

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
        end = pendulum.parse(kwargs["params"]["end_date"])
        if start > end:
            raise ValueError("start_date cannot be after end_date.")
        dates: List[str] = []
        cur = start
        while cur <= end:
            if cur.day_of_week not in (5, 6):  # skip Sat/Sun
                dates.append(cur.to_date_string())
            cur = cur.add(days=1)
        if not dates:
            raise AirflowSkipException("No trading dates in the given range.")
        return dates

    @task
    def make_pairs(underlyings: List[str], trading_days: List[str]) -> List[Dict[str, str]]:
        """
        Produce a flat list of {'ticker': T, 'target_date': D} dicts to avoid nested mapping.
        """
        pairs: List[Dict[str, str]] = []
        for d in trading_days:
            for t in underlyings:
                pairs.append({"ticker": t, "target_date": d})
        return pairs

    @task(retries=3, retry_delay=pendulum.duration(minutes=5), pool=API_POOL)
    def discover_for_pair(pair: Dict[str, str]) -> Dict[str, Any]:
        """
        Discover contracts for (ticker, target_date).
        Returns: {'target_date': <date>, 'contracts': [tickers...]}
        """
        ticker = pair["ticker"]
        target_date = pair["target_date"]
        api_key = _get_polygon_options_key()
        sess = _session()

        params = {
            "underlying_ticker": ticker,
            "expiration_date.gte": target_date,
            "as_of": target_date,
            "limit": 1000,
            "apiKey": api_key,
        }
        contracts: List[str] = []

        resp = _rate_limited_get(sess, POLYGON_CONTRACTS_URL, params)
        resp.raise_for_status()
        data = resp.json()
        for r in data.get("results", []) or []:
            t = r.get("ticker")
            if t:
                contracts.append(t)

        next_url = data.get("next_url")
        while next_url:
            resp = _rate_limited_get(sess, next_url, {"apiKey": api_key})
            resp.raise_for_status()
            data = resp.json()
            for r in data.get("results", []) or []:
                t = r.get("ticker")
                if t:
                    contracts.append(t)
            next_url = data.get("next_url")
            time.sleep(REQUEST_DELAY_SECONDS)

        return {"target_date": target_date, "contracts": sorted(set(contracts))}

    @task
    def batch_contracts_for_pair(discovery: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Turn discovery -> list of batch dicts:
          [{'target_date': D, 'batch': [c1, c2, ...]}, ...]
        """
        target_date = discovery["target_date"]
        contracts: List[str] = discovery.get("contracts") or []
        if not contracts:
            return []
        batches = _batch(contracts, CONTRACTS_BATCH_SIZE)
        return [{"target_date": target_date, "batch": b} for b in batches]

    @task(retries=3, retry_delay=pendulum.duration(minutes=5), pool=API_POOL)
    def process_contract_batch(batch_info: Dict[str, Any]) -> List[str]:
        """
        Fetch daily bars for each contract in batch_info['batch'] for batch_info['target_date'].
        Write gzip JSON to:
          raw/options/<CONTRACT>/<YYYY-MM-DD>.json.gz
        Return list of written keys.
        """
        target_date = batch_info["target_date"]
        contract_batch: List[str] = batch_info.get("batch") or []
        if not contract_batch:
            return []

        api_key = _get_polygon_options_key()
        sess = _session()
        s3 = S3Hook()

        written: List[str] = []
        for contract in contract_batch:
            key = f"raw/options/{contract}/{target_date}.json.gz"

            if not REPLACE_FILES and s3.check_for_key(key, bucket_name=BUCKET_NAME):
                continue

            url = f"{POLYGON_AGGS_URL_BASE}/{contract}/range/1/day/{target_date}/{target_date}"
            params = {"adjusted": "true", "apiKey": api_key}
            resp = _rate_limited_get(sess, url, params)
            if resp.status_code == 404:
                continue
            resp.raise_for_status()
            obj = resp.json()
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

    @task(outlets=[S3_OPTIONS_MANIFEST_DATASET])
    def write_manifest(all_keys: List[str]) -> None:
        if not all_keys:
            raise AirflowSkipException("No files written; skipping manifest.")
        s3 = S3Hook()
        manifest_key = "raw/manifests/polygon_options_manifest_latest.txt"
        s3.load_string("\n".join(all_keys), key=manifest_key, bucket_name=BUCKET_NAME, replace=True)
        print(f"✅ Manifest updated with {len(all_keys)} keys at s3://{BUCKET_NAME}/{manifest_key}")

    # ───────────────────────────────
    # Wiring (no nested mapping)
    # ───────────────────────────────
    underlyings = read_underlyings()
    trading_days = compute_trading_dates()
    pairs = make_pairs(underlyings, trading_days)                          # list[dict{ticker, target_date}]

    discoveries = discover_for_pair.expand(pair=pairs)                     # mapped over pairs
    batch_specs = batch_contracts_for_pair.expand(discovery=discoveries)   # mapped over discoveries
    written_lists = process_contract_batch.expand(batch_info=batch_specs)  # mapped over batch specs
    all_keys = flatten_str_lists(written_lists)
    write_manifest(all_keys)

polygon_options_ingest_backfill_dag()
