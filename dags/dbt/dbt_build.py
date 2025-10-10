from __future__ import annotations
import os
import pendulum
from airflow.decorators import dag
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig
from dags.utils.utils import send_failure_email

# --- dbt Configuration ---
DBT_PROJECT_DIR = os.getenv("DBT_PROJECT_DIR")
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
    This DAG runs daily on a fixed schedule. It runs the `dbt build`
    command to execute all dbt models and tests, transforming the raw data
    into analytics-ready marts.
    """,
)
def dbt_build_dag():
    """
    This DAG uses DbtTaskGroup to execute dbt models on a schedule.
    """
    dbt_build = DbtTaskGroup(
        group_id="dbt_build_all_models",
        project_config=ProjectConfig(dbt_project_path=DBT_PROJECT_DIR),
        profile_config=ProfileConfig(
            profile_name="stock_market_elt",
            target_name="dev",
            profiles_yml_filepath=os.path.join(DBT_PROJECT_DIR, "profiles.yml"),
        ),
        execution_config=ExecutionConfig(dbt_executable_path=DBT_EXECUTABLE_PATH),
        operator_args={"dbt_cmd": "build"},
    )

dbt_build_dag()
