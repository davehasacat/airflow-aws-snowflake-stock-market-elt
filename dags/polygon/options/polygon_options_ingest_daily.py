from __future__ import annotations
import pendulum
import os
import requests
import json
import csv
from datetime import timedelta
from airflow.decorators import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.hooks.base import BaseHook
from airflow.exceptions import AirflowSkipException
from dags.utils.polygon_datasets import S3_OPTIONS_MANIFEST_DATASET

POLYGON_CONTRACTS_URL = "https://api.polygon.io/v3/reference/options/contracts"
POLYGON_AGGS_URL = "https://api.polygon.io/v2/aggs/ticker"

@dag(
    dag_id="polygon_options_ingest_daily",
    start_date=pendulum.now(tz="UTC"),
    schedule="0 0 * * 1-5",
    catchup=True,
    tags=["ingestion", "polygon", "options", "historical", "aws"],
    dagrun_timeout=timedelta(hours=12),
)
def polygon_options_ingest_daily_dag():
    S3_CONN_ID = "aws_default"
    BUCKET_NAME = os.getenv("BUCKET_NAME")
    DBT_PROJECT_DIR = os.getenv("DBT_PROJECT_DIR", "/usr/local/airflow/dbt")

    @task
    def get_custom_tickers() -> list[str]:
        """Read custom tickers from dbt seeds."""
        path = os.path.join(DBT_PROJECT_DIR, "seeds", "custom_tickers.csv")
        tickers = []
        with open(path, mode="r") as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                tickers.append(row["ticker"])
        return tickers

    @task(retries=3, retry_delay=pendulum.duration(minutes=5), pool="api_pool")
    def discover_contracts_for_date(ticker: str, target_date: str) -> list[str]:
        """
        Uses Polygon's /v3/reference/options/contracts to list all contracts
        for a given underlying and expiration >= target_date.
        """
        conn = BaseHook.get_connection("polygon_options_api")
        api_key = conn.password

        params = {
            "underlying_ticker": ticker,
            "expiration_date.gte": target_date,
            "as_of": target_date,
            "limit": 1000,
            "apiKey": api_key,
        }

        contracts = []
        while True:
            resp = requests.get(POLYGON_CONTRACTS_URL, params=params)
            resp.raise_for_status()
            data = resp.json()

            for result in data.get("results", []):
                contracts.append(result["ticker"])

            # pagination
            next_url = data.get("next_url")
            if not next_url:
                break
            resp = requests.get(f"{next_url}&apiKey={api_key}")
            data = resp.json()

        return contracts

    @task(retries=3, retry_delay=pendulum.duration(minutes=5), pool="api_pool")
    def fetch_contract_bars(contract_ticker: str, target_date: str) -> str | None:
        """
        Fetch daily custom bars for a single options contract and store in S3.
        """
        s3_hook = S3Hook(aws_conn_id=S3_CONN_ID)
        conn = BaseHook.get_connection("polygon_options_api")
        api_key = conn.password

        url = f"{POLYGON_AGGS_URL}/{contract_ticker}/range/1/day/{target_date}/{target_date}"
        params = {"adjusted": "true", "apiKey": api_key}

        resp = requests.get(url, params=params)
        if resp.status_code == 404:
            # no data for this contract on that day
            return None
        resp.raise_for_status()
        data = resp.json()

        if not data.get("results"):
            return None

        json_string = json.dumps(data)
        s3_key = f"raw_data/options/{contract_ticker}_{target_date}.json"
        s3_hook.load_string(
            string_data=json_string,
            key=s3_key,
            bucket_name=BUCKET_NAME,
            replace=True,
        )
        return s3_key

    @task(outlets=[S3_OPTIONS_MANIFEST_DATASET])
    def create_manifest(s3_keys: list[str]):
        """Write manifest of ingested contract data."""
        keys = [k for k in s3_keys if k]
        if not keys:
            raise AirflowSkipException("No new files created; skipping manifest.")
        manifest_content = "\n".join(keys)
        s3_hook = S3Hook(aws_conn_id=S3_CONN_ID)
        manifest_key = "manifests/polygon_options_manifest_latest.txt"
        s3_hook.load_string(
            string_data=manifest_content,
            key=manifest_key,
            bucket_name=BUCKET_NAME,
            replace=True,
        )
        print(f"✅ Manifest created with {len(keys)} keys at s3://{BUCKET_NAME}/{manifest_key}")

    # ─── DAG FLOW ────────────────────────────────────────────────
    custom_tickers = get_custom_tickers()

    # Use Airflow Task Mapping to discover contracts for each ticker
    @task
    def get_target_date(execution_date: str) -> str:
        return pendulum.parse(execution_date).subtract(days=1).to_date_string()

    target_date = get_target_date("{{ ds }}")
    contracts_per_ticker = discover_contracts_for_date.expand(
        ticker=custom_tickers,
        target_date=[target_date] * len(custom_tickers),
    )

    # Flatten the list of lists into a single list
    @task
    def flatten_contracts(contracts_list: list[list[str]]) -> list[str]:
        return [c for sublist in contracts_list for c in sublist]

    all_contracts = flatten_contracts(contracts_per_ticker)

    s3_keys = fetch_contract_bars.expand(
        contract_ticker=all_contracts,
        target_date=[target_date] * len(all_contracts),
    )

    create_manifest(s3_keys)

polygon_options_ingest_daily_dag()
