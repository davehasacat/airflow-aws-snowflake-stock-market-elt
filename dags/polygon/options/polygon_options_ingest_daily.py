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

@dag(
    dag_id="polygon_options_ingest_daily",
    start_date=pendulum.now(tz="UTC"),
    schedule="0 0 * * 1-5",
    catchup=True,
    tags=["ingestion", "polygon", "options", "daily", "aws"],
    dagrun_timeout=timedelta(hours=12),
)
def polygon_options_ingest_daily_dag():
    S3_CONN_ID = "aws_default"
    BUCKET_NAME = os.getenv("BUCKET_NAME")
    DBT_PROJECT_DIR = os.getenv("DBT_PROJECT_DIR", "/usr/local/airflow/dbt")

    @task
    def get_custom_tickers() -> list[str]:
        """
        Retrieves a list of stock tickers from the `custom_tickers` seed table.
        """
        custom_tickers_path = os.path.join(DBT_PROJECT_DIR, "seeds", "custom_tickers.csv")
        tickers = []
        with open(custom_tickers_path, mode='r') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                tickers.append(row["ticker"])
        return tickers

    @task(pool="api_pool")
    def fetch_and_save_options_data(ticker: str, trade_date: str) -> list[str]:
        """
        For a single underlying ticker, fetches all relevant option contracts
        and saves their daily aggregate data for a specific trade date to S3.
        """
        conn = BaseHook.get_connection('polygon_options_api')
        api_key = conn.password
        if not api_key:
            raise ValueError("Polygon API key not found in connection 'polygon_options_api'.")

        s3_hook = S3Hook(aws_conn_id=S3_CONN_ID)
        saved_keys = []
        
        contracts_url = (
            f"https://api.polygon.io/v3/reference/options/contracts"
            f"?underlying_ticker={ticker}"
            f"&expiration_date.gte={trade_date}"
            f"&limit=1000&apiKey={api_key}"
        )
        
        while contracts_url:
            try:
                response = requests.get(contracts_url)
                response.raise_for_status()
                data = response.json()
                
                contracts = data.get("results", [])
                
                for contract in contracts:
                    options_ticker = contract["ticker"]
                    
                    bar_url = (
                        f"https://api.polygon.io/v2/aggs/ticker/{options_ticker}/range/1/day/{trade_date}/{trade_date}"
                        f"?apiKey={api_key}"
                    )
                    bar_response = requests.get(bar_url)
                    bar_response.raise_for_status()

                    bar_data = bar_response.json()
                    if bar_data.get("resultsCount") > 0:
                        s3_key = f"raw_data/options/{options_ticker}_{trade_date}.json"
                        s3_hook.load_string(
                            string_data=json.dumps(bar_data),
                            key=s3_key,
                            bucket_name=BUCKET_NAME,
                            replace=True,
                        )
                        saved_keys.append(s3_key)

                contracts_url = data.get("next_url")
                if contracts_url:
                    contracts_url += f"&apiKey={api_key}"

            except requests.exceptions.RequestException as e:
                print(f"Failed to fetch data for ticker {ticker}: {e}")
                break

        print(f"Saved {len(saved_keys)} option contract files for ticker {ticker}.")
        return saved_keys

    @task(outlets=[S3_OPTIONS_MANIFEST_DATASET])
    def create_manifest(s3_keys_per_ticker: list):
        """
        Flattens the list of S3 keys and writes them to the manifest file in S3.
        """
        flat_list = [key for sublist in s3_keys_per_ticker for key in sublist if key]
        
        if not flat_list:
            raise AirflowSkipException("No new files were created; skipping manifest generation.")
        
        manifest_content = "\n".join(flat_list)
        s3_hook = S3Hook(aws_conn_id=S3_CONN_ID)
        manifest_key = "manifests/polygon_options_manifest_latest.txt"

        s3_hook.load_string(
            string_data=manifest_content,
            key=manifest_key,
            bucket_name=BUCKET_NAME,
            replace=True,
        )
        print(f"Manifest created with {len(flat_list)} keys at s3://{BUCKET_NAME}/{manifest_key}")

    trade_date_str = "{{ ds }}"
    
    tickers_list = get_custom_tickers()
    s3_keys_list = fetch_and_save_options_data.partial(trade_date=trade_date_str).expand(ticker=tickers_list)
    create_manifest(s3_keys_list)

polygon_options_ingest_daily_dag()
