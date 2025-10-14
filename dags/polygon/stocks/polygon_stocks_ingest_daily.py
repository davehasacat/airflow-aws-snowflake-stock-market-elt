from __future__ import annotations
import pendulum
import os
import requests
import json
import csv

from airflow.decorators import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.exceptions import AirflowSkipException
from airflow.models import Variable  # read secrets from AWS SM via Airflow Variables

from dags.utils.polygon_datasets import S3_STOCKS_MANIFEST_DATASET


def _get_polygon_stocks_key() -> str:
    """
    Return a clean Polygon Stocks API key whether the Airflow Variable is stored as:
      - plain text: "abc123"
      - JSON object: {"polygon_stocks_api_key":"abc123"}
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
    dag_id="polygon_stocks_ingest_daily",
    start_date=pendulum.now(tz="UTC"),
    schedule="0 0 * * 1-5",   # run Mon–Fri at 00:00 UTC; targets previous trading day
    catchup=True,
    tags=["ingestion", "polygon", "daily", "aws"],
)
def polygon_stocks_ingest_daily_dag():
    BUCKET_NAME = os.getenv("BUCKET_NAME")  # expected: stock-market-elt
    DBT_PROJECT_DIR = os.getenv("DBT_PROJECT_DIR", "/usr/local/airflow/dbt")
    USER_AGENT = os.getenv("HTTP_USER_AGENT", "stocks-elt/polygon-stocks-daily (auto)")

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

    @task(retries=3, retry_delay=pendulum.duration(minutes=10), pool="api_pool")
    def get_grouped_daily_data_and_split(custom_tickers: list[str], **kwargs) -> list[str]:
        # Execution date is the schedule date; we pull the prior calendar day
        execution_date = kwargs["ds"]
        target_date = pendulum.parse(execution_date).subtract(days=1).to_date_string()

        api_key = _get_polygon_stocks_key()
        s3 = S3Hook()  # no aws_conn_id → use default AWS creds chain (mounted ~/.aws)
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
                print(f"No data found for {target_date} (likely a holiday). Skipping.")
                return []
            raise
        except requests.exceptions.RequestException as e:
            raise e

        processed_s3_keys: list[str] = []
        if data.get("resultsCount", 0) > 0 and data.get("results"):
            for result in data["results"]:
                ticker = (result.get("T") or "").upper()
                if ticker in custom_tickers_set:
                    payload = {
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
                    s3.load_string(
                        string_data=json.dumps(payload),
                        key=s3_key,
                        bucket_name=BUCKET_NAME,
                        replace=True,
                    )
                    processed_s3_keys.append(s3_key)

        return processed_s3_keys

    @task(outlets=[S3_STOCKS_MANIFEST_DATASET])
    def write_manifest_to_s3(s3_keys: list[str]):
        if not s3_keys:
            raise AirflowSkipException("No S3 keys were processed.")
        s3 = S3Hook()
        manifest_key = "raw/manifests/manifest_latest.txt"
        s3.load_string(
            string_data="\n".join(s3_keys),
            key=manifest_key,
            bucket_name=BUCKET_NAME,
            replace=True,
        )
        print(f"Manifest file updated: s3://{BUCKET_NAME}/{manifest_key}")

    custom_tickers = get_custom_tickers()
    s3_keys = get_grouped_daily_data_and_split(custom_tickers)
    write_manifest_to_s3(s3_keys)


polygon_stocks_ingest_daily_dag()
