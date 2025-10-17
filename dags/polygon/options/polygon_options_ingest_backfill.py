# dags/polygon/options/polygon_options_ingest_backfill.py
# =============================================================================
# Polygon Options Backfill (Ingestion)
# -----------------------------------------------------------------------------
# Goal:
#   Efficiently backfill daily options OHLCV (per-contract) from Polygon.io to S3.
#
# Key ideas:
#   1) Build flat (underlying, trade_date) pairs → map over pairs (no nested maps)
#   2) Discover option contracts valid on that date (pagination + retries)
#   3) Batch contracts to control API pressure
#   4) CHUNK batches to avoid Airflow max map length (tunable via env)
#   5) Process chunk internally: fetch day agg for each contract → gzip JSON → S3
#   6) Write a single S3 manifest enumerating all written object keys (Dataset outlet)
#
# Why this shape?
#   - Keeps mapping fan-out predictable (pairs + chunks)
#   - Uses strong HTTP retry strategy + light client-side rate limiting
#   - Avoids parsing/transforming raw payloads here (raw-in/raw-out)
#   - Produces a deterministic manifest to trigger downstream DAGs via Dataset
#
# Safety:
#   - Secrets come from Airflow Variables backed by AWS Secrets Manager
#   - Retries on transient HTTP (429/5xx) and task-level retries where appropriate
#   - Skips weekends automatically when computing trading dates
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
from airflow.models import Variable  # Secrets Manager-backed Variables

from dags.utils.polygon_datasets import S3_OPTIONS_MANIFEST_DATASET

# ────────────────────────────────────────────────────────────────────────────────
# Config / Constants (env-driven; safe defaults)
# ────────────────────────────────────────────────────────────────────────────────
POLYGON_CONTRACTS_URL = "https://api.polygon.io/v3/reference/options/contracts"
POLYGON_AGGS_URL_BASE = "https://api.polygon.io/v2/aggs/ticker"

BUCKET_NAME = os.getenv("BUCKET_NAME", "test")
DBT_PROJECT_DIR = os.getenv("DBT_PROJECT_DIR", "/usr/local/airflow/dbt")

REQUEST_TIMEOUT_SECS = int(os.getenv("HTTP_REQUEST_TIMEOUT_SECS", "60"))
USER_AGENT = os.getenv("HTTP_USER_AGENT", "stocks-elt/polygon-options-backfill (manual)")
API_POOL = os.getenv("API_POOL", "api_pool")  # protect Polygon APIs via Airflow Pool

# Rate limiting / batching
REQUEST_DELAY_SECONDS = float(os.getenv("POLYGON_REQUEST_DELAY_SECONDS", "0.25"))
CONTRACTS_BATCH_SIZE = int(os.getenv("BACKFILL_CONTRACTS_BATCH_SIZE", "300"))
REPLACE_FILES = os.getenv("BACKFILL_REPLACE", "false").lower() == "true"  # default: skip existing
# Cap mapping fan-out: chunk size controlling number of batch-specs per mapped task
MAP_CHUNK_SIZE = int(os.getenv("BACKFILL_MAP_CHUNK_SIZE", "2000"))

# ────────────────────────────────────────────────────────────────────────────────
# Helpers
# ────────────────────────────────────────────────────────────────────────────────
def _get_polygon_options_key() -> str:
    """
    Resolve the Polygon Options API key.

    Precedence:
      1) Airflow Variable 'polygon_options_api_key' (SecretsManager-backed)
         - Accepts raw string *or* small JSON dict like {"api_key": "..."}
      2) ENV POLYGON_OPTIONS_API_KEY (for local dev)
    """
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
    env = os.getenv("POLYGON_OPTIONS_API_KEY", "").strip()
    if env:
        return env
    raise RuntimeError(
        "Polygon Options API key not found. Create secret 'airflow/variables/polygon_options_api_key' "
        "or set env POLYGON_OPTIONS_API_KEY."
    )

def _session() -> requests.Session:
    """
    HTTP session with connection pooling + retry policy tuned for Polygon.

    - Retries 429/5xx with exponential backoff (server-friendly)
    - Connection pool sized for moderate parallelism
    """
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
    """Simple list chunker for batching contracts per date."""
    return [lst[i : i + n] for i in range(0, len(lst), n)]

# ────────────────────────────────────────────────────────────────────────────────
# DAG
# ────────────────────────────────────────────────────────────────────────────────
@dag(
    dag_id="polygon_options_ingest_backfill",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule=None,  # manual-only by design
    catchup=False,
    tags=["ingestion", "polygon", "options", "backfill", "aws"],
    params={
        # Inclusive date range (YYYY-MM-DD). Weekend days are skipped automatically.
        "start_date": Param(default="2025-10-09", type="string", description="Backfill start date (YYYY-MM-DD)"),
        "end_date":   Param(default="2025-10-10", type="string", description="Backfill end date (YYYY-MM-DD)"),
    },
    dagrun_timeout=timedelta(hours=36),
    max_active_runs=1,
)
def polygon_options_ingest_backfill_dag():
    """
    Optimized backfill strategy:
      - Underlyings from dbt seed (seeds/custom_tickers.csv)
      - Flat pairs (ticker x date) → discover contracts per pair
      - Batch + CHUNK to stay below max map length
      - Process each chunk task internally (looping): write raw JSON.gz to S3
      - Emit a single manifest file (Dataset outlet) for downstream triggers
    """

    @task
    def read_underlyings() -> List[str]:
        """
        Read `seeds/custom_tickers.csv` (dbt seed) and return uppercase symbols.
        Keeps the control-plane under version control (easy to update).
        """
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
        Compute inclusive date range, skipping Saturday/Sunday.
        (No market calendar dependency; simple and safe.)
        """
        start = pendulum.parse(kwargs["params"]["start_date"])
        end = pendulum.parse(kwargs["params"]["end_date"])
        if start > end:
            raise ValueError("start_date cannot be after end_date.")
        dates: List[str] = []
        cur = start
        while cur <= end:
            if cur.day_of_week not in (5, 6):  # 5=Sat, 6=Sun
                dates.append(cur.to_date_string())
            cur = cur.add(days=1)
        if not dates:
            raise AirflowSkipException("No trading dates in the given range.")
        return dates

    @task
    def make_pairs(underlyings: List[str], trading_days: List[str]) -> List[Dict[str, str]]:
        """Cartesian product (U x D) → [{'ticker': T, 'target_date': D}, ...]."""
        pairs: List[Dict[str, str]] = []
        for d in trading_days:
            for t in underlyings:
                pairs.append({"ticker": t, "target_date": d})
        return pairs

    @task(retries=3, retry_delay=pendulum.duration(minutes=5), pool=API_POOL)
    def discover_for_pair(pair: Dict[str, str]) -> Dict[str, Any]:
        """
        Discover contracts for a (ticker, target_date) using Polygon contracts API.

        Returns:
          {'target_date': <YYYY-MM-DD>, 'contracts': [<CONTRACT>, ...]}

        Notes:
          - Paginates via next_url
          - De-dupes + sorts tickers for stable downstream batches
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
            time.sleep(REQUEST_DELAY_SECONDS)  # play nice

        return {"target_date": target_date, "contracts": sorted(set(contracts))}

    @task
    def batch_contracts_for_pair(discovery: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Convert discovery into homogeneous batch-specs:
          [{'target_date': D, 'contract_batch': [c1, c2, ...]}, ...]

        Keeps downstream logic generic (each spec is processable on its own).
        """
        target_date = discovery["target_date"]
        contracts: List[str] = discovery.get("contracts") or []
        if not contracts:
            return []
        batches = _batch(contracts, CONTRACTS_BATCH_SIZE)
        return [{"target_date": target_date, "contract_batch": b} for b in batches]

    @task
    def flatten_batch_specs(nested: List[List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
        """Flatten list[list[dict]] → list[dict] for chunking and mapping."""
        return [d for sub in (nested or []) for d in (sub or [])]

    @task
    def chunk_batch_specs(batch_specs: List[Dict[str, Any]]) -> List[List[Dict[str, Any]]]:
        """
        Chunk the flat list of batch-specs to:
          - avoid large dynamic map explosions
          - spread work across tasks with predictable size
        """
        if not batch_specs:
            return []
        sz = MAP_CHUNK_SIZE
        return [batch_specs[i:i+sz] for i in range(0, len(batch_specs), sz)]

    @task(retries=3, retry_delay=pendulum.duration(minutes=5), pool=API_POOL)
    def process_chunk(specs: List[Dict[str, Any]]) -> List[str]:
        """
        Process a chunk of batch-specs (loop inside a single task for efficiency).

        For each contract in each spec:
          - GET day aggs for target_date
          - If results exist: gzip the raw JSON to S3 at:
              raw/options/<CONTRACT>/<YYYY-MM-DD>.json.gz

        Returns:
          - The list of S3 keys written for this chunk (for manifest assembly)

        Idempotency:
          - If BACKFILL_REPLACE=false, will skip existing keys (safe re-runs).
        """
        if not specs:
            return []

        api_key = _get_polygon_options_key()
        sess = _session()
        s3 = S3Hook()

        written: List[str] = []
        for item in specs:
            target_date = item["target_date"]
            contract_batch: List[str] = item.get("contract_batch") or []
            if not contract_batch:
                continue

            for contract in contract_batch:
                key = f"raw/options/{contract}/{target_date}.json.gz"

                if not REPLACE_FILES and s3.check_for_key(key, bucket_name=BUCKET_NAME):
                    # Already present; skip unless explicit replace
                    continue

                url = f"{POLYGON_AGGS_URL_BASE}/{contract}/range/1/day/{target_date}/{target_date}"
                params = {"adjusted": "true", "apiKey": api_key}
                resp = _rate_limited_get(sess, url, params)
                if resp.status_code == 404:
                    # Contract/date not found — benign for illiquid symbols
                    continue
                resp.raise_for_status()

                obj = resp.json()
                results = obj.get("results") or []
                if not results:
                    # Nothing for that day; skip
                    continue

                # Write raw JSON (no parsing) as gzipped bytes to S3
                buf = BytesIO()
                with gzip.GzipFile(filename="", mode="wb", fileobj=buf) as gz:
                    gz.write(json.dumps(obj, separators=(",", ":")).encode("utf-8"))
                s3.load_bytes(buf.getvalue(), key=key, bucket_name=BUCKET_NAME, replace=True)
                written.append(key)

                time.sleep(REQUEST_DELAY_SECONDS)  # light client throttle

        return written

    @task
    def flatten_str_lists(list_of_lists: List[List[str]]) -> List[str]:
        """Flatten list[list[str]] → list[str] and drop empties."""
        return [k for sub in (list_of_lists or []) for k in (sub or []) if k]

    @task(outlets=[S3_OPTIONS_MANIFEST_DATASET])
    def write_manifest(all_keys: List[str]) -> None:
        """
        Emit a single manifest enumerating all written keys.
        Downstream DAGs can use this Dataset to trigger dbt/loads.
        """
        if not all_keys:
            raise AirflowSkipException("No files written; skipping manifest.")
        s3 = S3Hook()
        manifest_key = "raw/manifests/polygon_options_manifest_latest.txt"
        s3.load_string("\n".join(all_keys), key=manifest_key, bucket_name=BUCKET_NAME, replace=True)
        print(f"✅ Manifest updated with {len(all_keys)} keys at s3://{BUCKET_NAME}/{manifest_key}")

    # ───────────────────────────────
    # Wiring / Dataflow
    # ───────────────────────────────
    underlyings = read_underlyings()
    trading_days = compute_trading_dates()
    pairs = make_pairs(underlyings, trading_days)  # list[{'ticker','target_date'}]

    discoveries = discover_for_pair.expand(pair=pairs)                             # map over pairs
    batch_specs_nested = batch_contracts_for_pair.expand(discovery=discoveries)    # map → list[list[dict]]
    batch_specs_flat = flatten_batch_specs(batch_specs_nested)                     # flatten → list[dict]
    chunked_specs = chunk_batch_specs(batch_specs_flat)                            # list[list[dict]] chunks
    written_lists = process_chunk.expand(specs=chunked_specs)                      # mapped per chunk
    all_keys = flatten_str_lists(written_lists)
    write_manifest(all_keys)

# Instantiate DAG
polygon_options_ingest_backfill_dag()
