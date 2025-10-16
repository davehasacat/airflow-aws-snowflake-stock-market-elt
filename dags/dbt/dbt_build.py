from __future__ import annotations
import os

import pendulum
from airflow.decorators import dag
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.profiles import SnowflakeUserPasswordProfileMapping
from dags.utils.utils import send_failure_email


DBT_PROJECT_DIR = os.getenv("DBT_PROJECT_DIR", "/usr/local/airflow/dbt")
DBT_EXECUTABLE_PATH = os.getenv("DBT_EXECUTABLE_PATH", "/usr/local/airflow/dbt_venv/bin/dbt")

@dag(
    dag_id="dbt_build",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    schedule="0 3 * * 1-5",
    catchup=False,
    tags=["dbt", "build"],
    default_args={"on_failure_callback": send_failure_email},
    doc_md="""
    ### dbt Transformation DAG for Stock Market Data
    Uses Cosmos profile mapping to materialize a **temporary** dbt profile from the
    `snowflake_default` Airflow connection (Secrets Manager-backed). No profiles.yml required.
    """,
)

def dbt_build_dag():
    dbt_build = DbtTaskGroup(
        group_id="dbt_build_all_models",
        project_config=ProjectConfig(dbt_project_path=DBT_PROJECT_DIR),

        # ✅ No profiles.yml: derive from Airflow connection at runtime
        profile_config=ProfileConfig(
            profile_name="stock_market_elt",    # logical name used by Cosmos; doesn't need to exist on disk
            target_name="dev",
            profile_mapping=SnowflakeUserPasswordProfileMapping(
                conn_id="snowflake_default",
                # Optional: if you want to override/force values instead of reading from connection extras:
                # profile_args={
                #     "database": "STOCKS_ELT_DB",
                #     "schema": "PUBLIC",
                #     "warehouse": "STOCKS_ELT_WH",
                #     "role": "STOCKS_ELT_ROLE",
                # }
            ),
       ),
        execution_config=ExecutionConfig(dbt_executable_path=DBT_EXECUTABLE_PATH),
        operator_args={"dbt_cmd": "build"},
  )

dbt_build_dag()
