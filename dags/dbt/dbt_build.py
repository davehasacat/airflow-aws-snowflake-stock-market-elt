from __future__ import annotations
import os
import json
from datetime import timedelta

import pendulum
from airflow.decorators import dag
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.profiles import SnowflakeUserPasswordProfileMapping
from dags.utils.utils import send_failure_email


# ───────────────────────────────────────────────
# Helper functions for env vars
# ───────────────────────────────────────────────
def _env_str(name: str, default: str) -> str:
    v = os.getenv(name)
    return default if v is None or not str(v).strip() else str(v).strip()

def _env_int(name: str, default: int) -> int:
    v = os.getenv(name)
    if v is None:
        return default
    try:
        return int(str(v).strip()) if str(v).strip() else default
    except ValueError:
        return default


# ───────────────────────────────────────────────
# Config
# ───────────────────────────────────────────────
DBT_PROJECT_DIR = _env_str("DBT_PROJECT_DIR", "/usr/local/airflow/dbt")
DBT_EXECUTABLE_PATH = _env_str("DBT_EXECUTABLE_PATH", "/usr/local/airflow/dbt_venv/bin/dbt")
DBT_THREADS = _env_int("DBT_THREADS", 4)
DBT_TARGET_PATH = _env_str("DBT_TARGET_PATH", "/usr/local/airflow/dbt_target")

DBT_SELECT = _env_str("DBT_BUILD_SELECT", "")
DBT_EXCLUDE = _env_str("DBT_BUILD_EXCLUDE", "")
DBT_FULL_REFRESH = _env_str("DBT_FULL_REFRESH", "").lower() in {"1", "true", "yes"}

DBT_VARS: dict = {}  # optional --vars payload

MANIFEST_PATH = _env_str("DBT_MANIFEST_PATH", os.path.join(DBT_TARGET_PATH, "manifest.json"))
USE_MANIFEST = os.path.exists(MANIFEST_PATH)


# ───────────────────────────────────────────────
# DAG Definition
# ───────────────────────────────────────────────
@dag(
    dag_id="dbt_build",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    schedule="0 3 * * 1-5",
    catchup=False,
    max_active_runs=1,
    tags=["dbt", "build"],
    default_args={"on_failure_callback": send_failure_email},
    doc_md="""
    **dbt_build DAG**

    Runs `dbt build` using Cosmos with a runtime profile
    derived from the `snowflake_default` Airflow connection
    (stored in AWS Secrets Manager).
    """,
)
def dbt_build_dag():
    project_cfg = ProjectConfig(
        dbt_project_path=DBT_PROJECT_DIR,
        env_vars={"DBT_TARGET_PATH": DBT_TARGET_PATH},
        manifest_path=MANIFEST_PATH if USE_MANIFEST else None,
    )

    profile_cfg = ProfileConfig(
        profile_name="stock_market_elt",
        target_name="dev",
        profile_mapping=SnowflakeUserPasswordProfileMapping(
            conn_id="snowflake_default",
        ),
    )

    exec_cfg = ExecutionConfig(dbt_executable_path=DBT_EXECUTABLE_PATH)

    def _cmd() -> str:
        parts = ["build"]
        if DBT_SELECT:
            parts += ["--select", DBT_SELECT]
        if DBT_EXCLUDE:
            parts += ["--exclude", DBT_EXCLUDE]
        if DBT_VARS:
            parts += ["--vars", json.dumps(DBT_VARS)]
        if DBT_FULL_REFRESH:
            parts.append("--full-refresh")
        return " ".join(parts)

    dbt_build = DbtTaskGroup(
        group_id="dbt_build",
        project_config=project_cfg,
        profile_config=profile_cfg,
        execution_config=exec_cfg,
        operator_args={
            "dbt_cmd": _cmd(),
            "retries": 2,
            "retry_delay": timedelta(minutes=5),
            "append_env": {"DBT_THREADS": str(DBT_THREADS)},
        },
    )

    dbt_build


dbt_build_dag()
