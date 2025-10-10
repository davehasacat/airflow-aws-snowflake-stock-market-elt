from __future__ import annotations

import os
import pendulum

from airflow.decorators import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig

# --- DAG Configuration ---
DBT_PROJECT_DIR = os.getenv("DBT_PROJECT_DIR")
S3_CONN_ID = os.getenv("S3_CONN_ID", "minio_s3")
SNOWFLAKE_CONN_ID = "snowflake_default"
BUCKET_NAME = os.getenv("BUCKET_NAME", "test")

@dag(
    dag_id="utils_connection_test",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["test", "minio", "snowflake", "dbt"],
    doc_md="""
    ### Full Stack Connection Test DAG

    This DAG tests the connections to MinIO S3 and Snowflake, and also
    verifies that dbt can successfully connect and parse a source.
    """,
)
def utils_connection_test_dag():
    """
    A DAG to test connections to S3, Snowflake, and dbt.
    """

    @task
    def test_minio_connection():
        """Checks the Minio S3 connection."""
        s3_hook = S3Hook(aws_conn_id=S3_CONN_ID)
        s3_hook.check_for_bucket(bucket_name=BUCKET_NAME)
        print(f"Minio connection to bucket '{BUCKET_NAME}' successful.")

    @task
    def test_snowflake_connection():
        """Checks the Snowflake DWH connection."""
        snowflake_hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        snowflake_hook.get_first("SELECT 1;")
        print("Snowflake connection successful.")

    test_dbt_connection = DbtTaskGroup(
        group_id="test_dbt_connection",
        project_config=ProjectConfig(
            dbt_project_path=DBT_PROJECT_DIR
        ),
        profile_config=ProfileConfig(
            profile_name="stock_market_elt",
            target_name="dev",
            profiles_yml_filepath=os.path.join(DBT_PROJECT_DIR, "profiles.yml"),
        ),
        operator_args={"select": "source:public.source_polygon_stock_bars_daily"},
    )

    # --- CORRECTED SECTION ---
    # Instantiate the tasks by calling the decorated functions
    minio_test = test_minio_connection()
    snowflake_test = test_snowflake_connection()

    # Define task dependencies using the instantiated task objects
    [minio_test, snowflake_test] >> test_dbt_connection

utils_connection_test_dag()
