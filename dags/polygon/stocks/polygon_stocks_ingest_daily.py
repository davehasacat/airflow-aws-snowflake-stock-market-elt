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
from airflow.datasets import Dataset
from airflow.exceptions import AirflowSkipException

S3_POLYGG_OPTIONS_MANIFEST_DATASET = Dataset("s3://test/manifests/polygon_options_manifest_latest.txt")

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
    def get_tickers_from_seed() -> list[str]:
        """
        Retrieves a list of stock tickers from the `custom_tickers` dbt seed file.
        """
        custom_tickers_path = os.path.join(DBT_PROJECT_DIR, "seeds", "custom_tickers.csv")
        with open(custom_tickers_path, mode='r') as csvfile:
            reader = csv.DictReader(csvfile)
            tickers = [row["ticker"] for row in reader]
        
        if not tickers:
            raise AirflowSkipException("No tickers found in 'custom_tickers.csv'.")
            
        print(f"Retrieved {len(tickers)} tickers from the 'custom_tickers' seed.")
        return tickers

    @task(retries=3, retry_delay=pendulum.duration(minutes=10), pool="api_pool")
    def fetch_and_save_options_data(ticker: str, **kwargs) -> list[str]:
        """
        For a single underlying ticker, fetches all relevant option contracts
        and saves their daily aggregate data for a specific trade date to S3.
        """
        # Correctly calculate the target date for the data pull
        execution_date = kwargs["ds"]
        trade_date = pendulum.parse(execution_date).subtract(days=1).to_date_string()

        conn = BaseHook.get_connection('polygon_options_api')
        api_key = conn.password
        if not api_key:
            raise ValueError("Polygon Options API key not found in connection 'polygon_options_api'.")

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
                
                for contract in data.get("results", []):
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

            except requests.exceptions.HTTPError as e:
                # Add more specific error handling, similar to the stocks DAG
                if e.response.status_code == 404:
                    print(f"No contract data found for ticker {ticker} on {trade_date}. Skipping.")
                    break
                print(f"HTTP Error for ticker {ticker}: {e}. Skipping this ticker.")
                break # Exit loop for this ticker if a persistent API error occurs
            except requests.exceptions.RequestException as e:
                print(f"Network error while fetching data for {ticker}: {e}. Skipping this ticker.")
                break

        print(f"Saved {len(saved_keys)} option contract files for ticker {ticker}.")
        return saved_keys

    @task(outlets=[S3_POLYGON_OPTIONS_MANIFEST_DATASET])
    def create_manifest(s3_keys_per_ticker: list):
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

    tickers_list = get_tickers_from_seed()
    s3_keys_list = fetch_and_save_options_data.expand(ticker=tickers_list)
    create_manifest(s3_keys_list)

polygon_options_ingest_daily_dag()
