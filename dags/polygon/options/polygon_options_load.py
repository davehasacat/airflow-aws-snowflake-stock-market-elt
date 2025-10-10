from __future__ import annotations
import pendulum
import os
from datetime import timedelta
from airflow.decorators import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.exceptions import AirflowSkipException
from airflow.datasets import Dataset

# Define the Datasets for data-driven scheduling
S3_POLYGON_OPTIONS_MANIFEST_DATASET = Dataset("s3://test/manifests/polygon_options_manifest_latest.txt")
SNOWFLAKE_DWH_POLYGON_OPTIONS_RAW_DATASET = Dataset("snowflake://stocks_elt_db/public/source_polygon_options_bars_daily")

@dag(
    dag_id="polygon_options_load",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule=[S3_POLYGON_OPTIONS_MANIFEST_DATASET],
    catchup=False,
    tags=["load", "polygon", "options", "snowflake"],
    dagrun_timeout=timedelta(hours=4),
)
def polygon_options_load_dag():
    """
    This DAG loads raw JSON options data from S3 into Snowflake using the
    COPY INTO command. It is triggered by the completion of an options ingest DAG.
    """
    S3_CONN_ID = os.getenv("S3_CONN_ID", "minio_s3")
    SNOWFLAKE_CONN_ID = "snowflake_default"
    BUCKET_NAME = os.getenv("BUCKET_NAME", "test")
    SNOWFLAKE_TABLE = "source_polygon_options_bars_daily"

    @task
    def create_snowflake_table():
        """Creates the target options table in Snowflake if it doesn't exist."""
        snowflake_hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {SNOWFLAKE_TABLE} (
            option_symbol TEXT,
            trade_date DATE,
            underlying_ticker TEXT,
            expiration_date DATE,
            strike_price NUMERIC(19, 4),
            option_type VARCHAR(4),
            open NUMERIC(19, 4),
            high NUMERIC(19, 4),
            low NUMERIC(19, 4),
            close NUMERIC(19, 4),
            volume BIGINT,
            vwap NUMERIC(19, 4),
            transactions BIGINT,
            inserted_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP()
        );
        """
        snowflake_hook.run(create_table_sql)
        print(f"Ensured table {SNOWFLAKE_TABLE} exists.")

    @task
    def get_s3_keys_from_manifest() -> list[str]:
        """Reads the options manifest file from S3 to get the list of new JSON files."""
        s3_hook = S3Hook(aws_conn_id=S3_CONN_ID)
        manifest_key = "manifests/polygon_options_manifest_latest.txt"
        if not s3_hook.check_for_key(manifest_key, bucket_name=BUCKET_NAME):
            raise FileNotFoundError(f"Manifest file not found: {manifest_key}")
        
        manifest_content = s3_hook.read_key(key=manifest_key, bucket_name=BUCKET_NAME)
        s3_keys = [key for key in manifest_content.strip().splitlines() if key]
        if not s3_keys:
            raise AirflowSkipException("Manifest is empty. No new files to process.")
        
        print(f"Found {len(s3_keys)} new options files to load from manifest.")
        return s3_keys

    @task(outlets=[SNOWFLAKE_DWH_POLYGON_OPTIONS_RAW_DATASET])
    def load_data_to_snowflake(s3_keys: list[str]):
        """
        Executes a COPY INTO command to load data from the specified S3 keys
        into the target Snowflake table.
        """
        snowflake_hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)

        # --- CORRECTED SECTION ---
        # The curly braces in the regex are now doubled (e.g., {{4}}) to escape them in the f-string.
        copy_sql = f"""
        COPY INTO {SNOWFLAKE_TABLE} (
            option_symbol, trade_date, underlying_ticker, expiration_date, strike_price, option_type,
            open, high, low, close, volume, vwap, transactions
        )
        FROM (
            SELECT
                $1:ticker::TEXT,
                TO_DATE(REGEXP_SUBSTR(METADATA$FILENAME, '([0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}})')),
                REGEXP_SUBSTR($1:ticker, 'O:([A-Z\.]+)', 1, 1, 'e', 1),
                TO_DATE(REGEXP_SUBSTR($1:ticker, '(\\\\d{{6}})', 1, 1, 'e'), 'YYMMDD'),
                (REGEXP_SUBSTR($1:ticker, '(\\\\d{{8}}$)', 1, 1, 'e'))::NUMBER / 1000,
                IFF(REGEXP_SUBSTR($1:ticker, '([CP])', 1, 1, 'e') = 'C', 'call', 'put'),
                $1:results[0]:o::FLOAT,
                $1:results[0]:h::FLOAT,
                $1:results[0]:l::FLOAT,
                $1:results[0]:c::FLOAT,
                $1:results[0]:v::BIGINT,
                $1:results[0]:vw::FLOAT,
                $1:results[0]:n::BIGINT
            FROM @s3_stage
        )
        FILES = ({', '.join(f"'{key}'" for key in s3_keys)})
        FILE_FORMAT = (TYPE = 'JSON');
        """
        
        snowflake_hook.run(copy_sql)
        print(f"Successfully loaded data from {len(s3_keys)} files into {SNOWFLAKE_TABLE}.")

    # --- Task Flow ---
    table_created = create_snowflake_table()
    s3_keys = get_s3_keys_from_manifest()
    
    load_op = load_data_to_snowflake(s3_keys)
    table_created >> load_op

polygon_options_load_dag()
