from __future__ import annotations
import pendulum
import os
import requests
import json
import csv
from airflow.decorators import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.exceptions import AirflowSkipException
from airflow.models.param import Param
from airflow.models import Variable  # read from Secrets Manager via Variables

from dags.utils.polygon_datasets import S3_STOCKS_MANIFEST_DATASET


def _get_polygon_stocks_key() -> str:
    """
    Return a clean Polygon Stocks API key whether the Airflow Variable is stored as:
      - plain text: "abc123"
      - JSON object: {"polygon_stocks_api_key":"abc123"} (console defaults sometimes do this)
    Falls back to env POLYGON_STOCKS_API_KEY.
    """
    # 1) Try JSON variable
    try:
        v = Variable.get("polygon_stocks_api_key", deserialize_json=True)
        if isinstance(v, dict):
            for k in ("polygon_stocks_api_key", "api_key", "key", "value"):
                s = v.get(k)
                if isinstance(s, str) and s.strip():
                    return s.strip()
            if len(v) == 1:
                s = next(iter(v.values()))
                if isinstance(s, str) and s.strip():
                    return s.strip()
    except Exception:
        pass

    # 2) Try plain-text variable (or JSON-ish string)
    try:
        s = Variable.get("polygon_stocks_api_key")
        if s:
            s = s.strip()
            if s.startswith("{"):
                try:
                    obj = json.loads(s)
                    if isinstance(obj, dict):
                        for k in ("polygon_stocks_api_key", "api_key", "key", "value"):
                            v = obj.get(k)
                            if isinstance(v, str) and v.strip():
                                return v.strip()
                        if len(obj) == 1:
                            v = next(iter(obj.values()))
                            if isinstance(v, str) and v.strip():
                                return v.strip()
                except Exception:
                    # fall through to return s as-is
                    pass
            return s
    except Exception:
        pass

    # 3) Env fallback
    env = os.getenv("POLYGON_STOCKS_API_KEY", "").strip()
    if env:
        return env

    raise RuntimeError(
        "Missing Polygon API key. In AWS Secrets Manager set secret "
        "'airflow/variables/polygon_stocks_api_key' to the raw key string, "
        "or set POLYGON_STOCKS_API_KEY in the environment."
    )


@dag(
    dag_id="polygon_stocks_ingest_backfill",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["ingestion", "polygon", "backfill", "aws"],
    params={
        "start_date": Param(default="2025-10-06", type="string", description="The start date for the backfill (YYYY-MM-DD)."),
        "end_date": Param(default="2025-10-11", type="string", description="The end date for the backfill (YYYY-MM-DD)."),
    },
)
def polygon_stocks_ingest_backfill_dag():
    """
    Backfills daily grouped OHLCV from Polygon for a date range,
    filtering to the tickers in dbt/seeds/custom_tickers.csv.

    S3 writes:
      - raw/stocks/{TICKER}/{YYYY-MM-DD}.json
      - raw/manifests/manifest_latest.txt
    """
    BUCKET_NAME = os.getenv("BUCKET_NAME")  # expected: 'stock-market-elt'
    DBT_PROJECT_DIR = os.getenv("DBT_PROJECT_DIR", "/usr/local/airflow/dbt")
    USER_AGENT = os.getenv("HTTP_USER_AGENT", "stocks-elt/polygon-stocks-backfill (manual)")

    @task
    def get_custom_tickers() -> list[str]:
        path = os.path.join(DBT_PROJECT_DIR, "seeds", "custom_tickers.csv")
        tickers: list[str] = []
        with open(path, mode="r", newline="") as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                t = (row.get("ticker") or "").strip().upper()
                if t:
                    tickers.append(t)
        return tickers

    @task
    def generate_date_range(**kwargs) -> list[str]:
        start = pendulum.parse(kwargs["params"]["start_date"])
        end = pendulum.parse(kwargs["params"]["end_date"])

        trading_dates: list[str] = []
        cur = start
        while cur <= end:
            # Skip weekends (Sat=5, Sun=6)
            if cur.day_of_week not in (5, 6):
                trading_dates.append(cur.to_date_string())
            cur = cur.add(days=1)
        return trading_dates

    @task(retries=3, retry_delay=pendulum.duration(minutes=10), pool="api_pool")
    def process_date(target_date: str, custom_tickers: list[str]) -> list[str]:
        # Use default AWS credentials chain (mounted ~/.aws)
        s3_hook = S3Hook()
        api_key = _get_polygon_stocks_key()

        custom_tickers_set = set(custom_tickers)
        url = f"https://api.polygon.io/v2/aggs/grouped/locale/us/market/stocks/{target_date}"
        params = {"adjusted": "true", "apiKey": api_key}
        headers = {"User-Agent": USER_AGENT}

        try:
            resp = requests.get(url, params=params, headers=headers, timeout=90)
            resp.raise_for_status()
            data = resp.json()
        except requests.exceptions.HTTPError as e:
            if e.response is not None and e.response.status_code == 404:
                print(f"No data found for {target_date} (likely a market holiday). Skipping.")
                return []
            raise
        except requests.exceptions.RequestException as e:
            raise e

        processed_s3_keys: list[str] = []
        if data.get("resultsCount", 0) > 0 and data.get("results"):
            for result in data["results"]:
                ticker = (result.get("T") or "").upper()
                if ticker in custom_tickers_set:
                    formatted_result = {
                        "ticker": ticker,
                        "queryCount": 1,
                        "resultsCount": 1,
                        "adjusted": True,
                        "results": [{
                            "v": result.get("v"),
                            "vw": result.get("vw"),
                            "o": result.get("o"),
                            "c": result.get("c"),
                            "h": result.get("h"),
                            "l": result.get("l"),
                            "t": result.get("t"),
                            "n": result.get("n"),
                        }],
                        "status": "OK",
                        "request_id": data.get("request_id"),
                    }
                    # s3://<bucket>/raw/stocks/<ticker>/<YYYY-MM-DD>.json
                    s3_key = f"raw/stocks/{ticker}/{target_date}.json"
                    s3_hook.load_string(
                        string_data=json.dumps(formatted_result),
                        key=s3_key,
                        bucket_name=BUCKET_NAME,
                        replace=True,
                    )
                    processed_s3_keys.append(s3_key)

        return processed_s3_keys

    @task
    def flatten_s3_key_list(nested_list: list[list[str]]) -> list[str]:
        return [key for sub in nested_list for key in sub if key]

    @task(outlets=[S3_STOCKS_MANIFEST_DATASET])
    def write_manifest_to_s3(s3_keys: list[str]):
        if not s3_keys:
            raise AirflowSkipException("No S3 keys were processed during the backfill.")
        manifest_content = "\n".join(s3_keys)
        s3_hook = S3Hook()
        manifest_key = "raw/manifests/manifest_latest.txt"
        s3_hook.load_string(string_data=manifest_content, key=manifest_key, bucket_name=BUCKET_NAME, replace=True)
        print(f"Backfill manifest file created: s3://{BUCKET_NAME}/{manifest_key}")

    # Flow
    custom_tickers = get_custom_tickers()
    date_range = generate_date_range()
    processed_nested = process_date.partial(custom_tickers=custom_tickers).expand(target_date=date_range)
    s3_keys_flat = flatten_s3_key_list(processed_nested)
    write_manifest_to_s3(s3_keys_flat)


polygon_stocks_ingest_backfill_dag()
