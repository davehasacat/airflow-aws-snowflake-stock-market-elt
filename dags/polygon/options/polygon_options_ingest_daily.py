# dags/polygon/options/polygon_options_ingest_daily.py
from __future__ import annotations

import csv
import json
import os
import time
import gzip
from io import BytesIO
from typing import List, Optional

import pendulum
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from airflow.decorators import dag, task, task_group
from airflow.operators.python import get_current_context
from airflow.exceptions import AirflowSkipException
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models import Variable  # Secrets Manager-backed Variables

from dags.utils.polygon_datasets import S3_OPTIONS_MANIFEST_DATASET

# ────────────────────────────────────────────────────────────────────────────────
# Config / Constants
# ────────────────────────────────────────────────────────────────────────────────
POLYGON_CONTRACTS_URL = "https://api.polygon.io/v3/reference/options/contracts"
POLYGON_AGGS_URL_BASE = "https://api.polygon.io/v2/aggs/ticker"

BUCKET_NAME = os.getenv("BUCKET_NAME")  # expected: stock-market-elt
DBT_PROJECT_DIR = os.getenv("DBT_PROJECT_DIR", "/usr/local/airflow/dbt")

REQUEST_TIMEOUT_SECS = int(os.getenv("HTTP_REQUEST_TIMEOUT_SECS", "60"))
USER_AGENT = os.getenv("HTTP_USER_AGENT", "stocks-elt/polygon-options-daily (auto)")
API_POOL = "api_pool"  # ensure this pool exists (slots ~8)

# Batching knobs
CONTRACT_BATCH_SIZE = int(os.getenv("OPTIONS_DAILY_CONTRACT_BATCH_SIZE", "400"))
REQUEST_DELAY_SECONDS = float(os.getenv("POLYGON_REQUEST_DELAY_SECONDS", "0.25"))

# Idempotent writes (True = overwrite, False = skip if exists)
REPLACE_FILES = os.getenv("DAILY_REPLACE", "true").lower() == "true"

default_args = {
    "owner": "data-platform",
    "retries": 3,
    "retry_delay": pendulum.duration(minutes=5),
}

# Guardrail: ensure BUCKET_NAME is configured
if not BUCKET_NAME:
    raise RuntimeError("BUCKET_NAME env var is required (e.g., 'stock-market-elt').")

# ────────────────────────────────────────────────────────────────────────────────
# Helpers
# ────────────────────────────────────────────────────────────────────────────────
def _get_polygon_options_key() -> str:
    """
    Return a clean Polygon Options API key whether the Airflow Variable is stored as:
      - plain text: "abc123"
      - JSON object: {"polygon_options_api_key":"abc123"} (console/SM can do this)
    Falls back to env POLYGON_OPTIONS_API_KEY.
    """
    # 1) Try JSON variable
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

    # 2) Try plain-text variable (could itself be a JSON string)
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
                    # fall through to return s as-is
                    pass
            return s
    except Exception:
        pass

    # 3) Env fallback
    env = os.getenv("POLYGON_OPTIONS_API_KEY", "").strip()
    if env:
        return env

    raise RuntimeError(
        "Polygon Options API key not found. In AWS Secrets Manager set secret "
        "'airflow/variables/polygon_options_api_key' to the raw key string, "
        "or set POLYGON_OPTIONS_API_KEY in the environment."
    )

def _session() -> requests.Session:
    """HTTP session with connection pooling + retries for resilience and speed."""
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
    Robust GET with exponential backoff for 429/5xx and 'Retry-After' support.
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

# ────────────────────────────────────────────────────────────────────────────────
# DAG
# ────────────────────────────────────────────────────────────────────────────────
@dag(
    dag_id="polygon_options_ingest_daily",
    description="Discover option contracts by underlying, fetch daily aggregates per contract, write to S3 (gz), and build a manifest.",
    start_date=pendulum.now(tz="UTC"),
    schedule="0 0 * * 1-5",  # weekdays at 00:00 UTC
    catchup=True,
    default_args=default_args,
    dagrun_timeout=pendulum.duration(hours=12),
    tags=["ingestion", "polygon", "options", "daily", "aws"],
    max_active_runs=1,
)
def polygon_options_ingest_daily_dag():

    # ───────────────────────────────
    # Inputs
    # ───────────────────────────────
    @task
    def get_custom_tickers() -> List[str]:
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
        """
        Use logical_date so the daily schedule loads the previous **business** day.
        If the prior day is Sat/Sun, roll back to Friday.
        """
        ctx = get_current_context()
        d = ctx["logical_date"].subtract(days=1)  # prior calendar day
        while d.day_of_week in (5, 6):  # 5=Sat, 6=Sun
            d = d.subtract(days=1)
        return d.to_date_string()

    # ───────────────────────────────
    # Contract Discovery
    # ───────────────────────────────
    @task_group(group_id="discover_contracts")
    def tg_discover_contracts(custom_tickers: List[str], target_date: str):

        @task(retries=3, retry_delay=pendulum.duration(minutes=5), pool=API_POOL)
        def discover_for_ticker(ticker: str, target_date: str) -> List[str]:
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

            # First page
            resp = _rate_limited_get(sess, POLYGON_CONTRACTS_URL, params)
            resp.raise_for_status()
            data = resp.json()
            contracts.extend([c["ticker"] for c in (data.get("results") or []) if "ticker" in c])

            # Pagination
            next_url = data.get("next_url")
            while next_url:
                resp = _rate_limited_get(sess, next_url, {"apiKey": api_key})
                resp.raise_for_status()
                data = resp.json()
                contracts.extend([c["ticker"] for c in (data.get("results") or []) if "ticker" in c])
                next_url = data.get("next_url")
                time.sleep(REQUEST_DELAY_SECONDS)

            return sorted(set(contracts))

        @task
        def flatten(nested: List[List[str]]) -> List[str]:
            return [c for sub in (nested or []) for c in (sub or [])]

        discovered = (
            discover_for_ticker
            .partial(target_date=target_date)
            .expand(ticker=custom_tickers)
        )
        all_contracts = flatten(discovered)
        return all_contracts

    # ───────────────────────────────
    # Batching
    # ───────────────────────────────
    @task
    def batch_contracts(contracts: List[str]) -> List[List[str]]:
        if not contracts:
            raise AirflowSkipException("No contracts discovered for provided tickers.")
        sz = CONTRACT_BATCH_SIZE
        return [contracts[i:i+sz] for i in range(0, len(contracts), sz)]

    # ───────────────────────────────
    # Fetch & Store (per-batch)
    # ───────────────────────────────
    @task(retries=3, retry_delay=pendulum.duration(minutes=5), pool=API_POOL)
    def process_contract_batch(contract_batch: List[str], target_date: str) -> Optional[str]:
        """
        For each contract in the batch:
          - Fetch daily bar for target_date
          - Write one gzip JSON to S3: raw/options/<contract>/<YYYY-MM-DD>.json.gz
        Then write a per-batch manifest:
          - raw/manifests/runs/<dag_run_id>/batch_<idx>.txt
        Returns the per-batch manifest key (tiny XCom).
        """
        ctx = get_current_context()
        dag_run_id = ctx["dag_run"].run_id.replace(":", "_").replace("/", "_")
        batch_idx = ctx["ti"].map_index if ctx.get("ti") else 0

        api_key = _get_polygon_options_key()
        s3 = S3Hook()  # default AWS creds chain
        sess = _session()

        written_keys: List[str] = []
        for contract_ticker in contract_batch:
            url = f"{POLYGON_AGGS_URL_BASE}/{contract_ticker}/range/1/day/{target_date}/{target_date}"
            params = {"adjusted": "true", "apiKey": api_key}
            resp = _rate_limited_get(sess, url, params)

            if resp.status_code in (401, 403):
                # Likely entitlement or auth issue — log and skip
                try:
                    j = resp.json()
                    msg = j.get("error") or j.get("message") or str(j)
                except Exception:
                    msg = resp.text
                print(f"⚠️  {contract_ticker} {target_date} -> {resp.status_code}: {msg[:200]}")
                time.sleep(REQUEST_DELAY_SECONDS)
                continue

            if resp.status_code == 404:
                # No data for this contract/date
                time.sleep(REQUEST_DELAY_SECONDS)
                continue

            resp.raise_for_status()
            data = resp.json()
            results = data.get("results") or []
            if not results:
                # Empty results — nothing to write
                time.sleep(REQUEST_DELAY_SECONDS)
                continue

            # gzip JSON payload to reduce size & speed COPY
            buf = BytesIO()
            with gzip.GzipFile(filename="", mode="wb", fileobj=buf) as gz:
                gz.write(json.dumps(data, separators=(",", ":")).encode("utf-8"))
            gz_bytes = buf.getvalue()

            s3_key = f"raw/options/{contract_ticker}/{target_date}.json.gz"

            # Idempotency: optionally skip overwriting existing files
            if not REPLACE_FILES and s3.check_for_key(s3_key, bucket_name=BUCKET_NAME):
                time.sleep(REQUEST_DELAY_SECONDS)
                continue

            s3.load_bytes(bytes_data=gz_bytes, key=s3_key, bucket_name=BUCKET_NAME, replace=True)
            written_keys.append(s3_key)

            time.sleep(REQUEST_DELAY_SECONDS)

        if not written_keys:
            return None

        manifest_key = f"raw/manifests/runs/{dag_run_id}/batch_{batch_idx:04d}.txt"
        s3.load_string("\n".join(written_keys), key=manifest_key, bucket_name=BUCKET_NAME, replace=True)
        return manifest_key

    # ───────────────────────────────
    # Merge manifests → canonical latest manifest
    # ───────────────────────────────
    @task(outlets=[S3_OPTIONS_MANIFEST_DATASET])
    def merge_manifests(batch_manifest_keys: List[Optional[str]]) -> str:
        s3 = S3Hook()
        keys = [k for k in (batch_manifest_keys or []) if k]
        if not keys:
            raise AirflowSkipException("No batch manifests produced; nothing to merge.")

        all_lines: list[str] = []
        for mk in keys:
            content = s3.read_key(key=mk, bucket_name=BUCKET_NAME) or ""
            lines = [ln.strip() for ln in content.splitlines() if ln.strip()]
            all_lines.extend(lines)

        if not all_lines:
            raise AirflowSkipException("Merged manifest would be empty; aborting.")

        final_key = "raw/manifests/polygon_options_manifest_latest.txt"
        s3.load_string("\n".join(all_lines), key=final_key, bucket_name=BUCKET_NAME, replace=True)
        print(f"✅ Final manifest {final_key} with {len(all_lines)} files")
        return final_key

    # ───────────────────────────────
    # Wiring
    # ───────────────────────────────
    custom_tickers = get_custom_tickers()
    target_date = compute_target_date()
    all_contracts = tg_discover_contracts(custom_tickers, target_date)
    contract_batches = batch_contracts(all_contracts)
    batch_manifest_keys = process_contract_batch.partial(target_date=target_date).expand(contract_batch=contract_batches)
    _final_manifest = merge_manifests(batch_manifest_keys)


polygon_options_ingest_daily_dag()
