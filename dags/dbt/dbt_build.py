from __future__ import annotations
import os
import json
from datetime import timedelta

import pendulum
from airflow.decorators import dag
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.profiles import SnowflakeUserPasswordProfileMapping
from dags.utils.utils import send_failure_email


# ────────────────────────────────────────────────────────────────────────────────
# Env helpers
# ────────────────────────────────────────────────────────────────────────────────
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


# ────────────────────────────────────────────────────────────────────────────────
# Config
# ────────────────────────────────────────────────────────────────────────────────
DBT_PROJECT_DIR = _env_str("DBT_PROJECT_DIR", "/usr/local/airflow/dbt")
DBT_EXECUTABLE_PATH = _env_str("DBT_EXECUTABLE_PATH", "/usr/local/airflow/dbt_venv/bin/dbt")
DBT_THREADS = _env_int("DBT_THREADS", 4)
DBT_TARGET_PATH = _env_str("DBT_TARGET_PATH", "/usr/local/airflow/dbt_target")

DBT_SELECT = _env_str("DBT_BUILD_SELECT", "")
DBT_EXCLUDE = _env_str("DBT_BUILD_EXCLUDE", "")
DBT_FULL_REFRESH = _env_str("DBT_FULL_REFRESH", "").lower() in {"1", "true", "yes"}

DBT_VARS: dict = {
    # optional: project vars passed via --vars JSON
}

# (Optional) prebuilt manifest path — safe to keep, Cosmos will ignore it if not using manifest mode
MANIFEST_PATH = _env_str("DBT_MANIFEST_PATH", os.path.join(DBT_TARGET_PATH, "manifest.json"))
USE_MANIFEST = os.path.exists(MANIFEST_PATH)


@dag(
    dag_id="dbt_build",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    schedule="0 3 * * 1-5",
    catchup=False,
    max_active_runs=1,
    tags=["dbt", "build"],
    default_args={"on_failure_callback": send_failure_email},
    doc_md="""
    Runs seeds → run → snapshots → tests using Cosmos with a runtime profile
    from `snowflake_default` (AWS Secrets Manager).
    """,
)
def dbt_build_dag():

    project_cfg = ProjectConfig(
        dbt_project_path=DBT_PROJECT_DIR,
        env_vars={"DBT_TARGET_PATH": DBT_TARGET_PATH},
        # manifest_path can stay; it’s ignored if Cosmos isn’t in manifest mode
        manifest_path=MANIFEST_PATH if USE_MANIFEST else None,
    )

    profile_cfg = ProfileConfig(
        profile_name="stock_market_elt",
        target_name="dev",
        profile_mapping=SnowflakeUserPasswordProfileMapping(
            conn_id="snowflake_default",
        ),
    )

    base_exec_cfg = ExecutionConfig(dbt_executable_path=DBT_EXECUTABLE_PATH)

    def _cmd(base: str, include_full_refresh: bool = False) -> str:
        parts = [base]
        if DBT_SELECT:
            parts += ["--select", DBT_SELECT]
        if DBT_EXCLUDE:
            parts += ["--exclude", DBT_EXCLUDE]
        if DBT_VARS:
            parts += ["--vars", json.dumps(DBT_VARS)]
        if include_full_refresh and DBT_FULL_REFRESH:
            parts.append("--full-refresh")
        return " ".join(parts)

    dbt_seed = DbtTaskGroup(
        group_id="dbt_seed",
        project_config=project_cfg,
        profile_config=profile_cfg,
        execution_config=base_exec_cfg,
        operator_args={
            "dbt_cmd": _cmd("seed"),
            "retries": 2,
            "retry_delay": timedelta(minutes=5),
            "append_env": {"DBT_THREADS": str(DBT_THREADS)},
        },
    )

    dbt_run = DbtTaskGroup(
        group_id="dbt_run",
        project_config=project_cfg,
        profile_config=profile_cfg,
        execution_config=base_exec_cfg,
        operator_args={
            "dbt_cmd": _cmd("run", include_full_refresh=True),
            "retries": 2,
            "retry_delay": timedelta(minutes=5),
            "append_env": {"DBT_THREADS": str(DBT_THREADS)},
        },
    )

    dbt_snapshot = DbtTaskGroup(
        group_id="dbt_snapshot",
        project_config=project_cfg,
        profile_config=profile_cfg,
        execution_config=base_exec_cfg,
        operator_args={
            "dbt_cmd": _cmd("snapshot"),
            "retries": 2,
            "retry_delay": timedelta(minutes=5),
            "append_env": {"DBT_THREADS": str(DBT_THREADS)},
        },
    )

    dbt_test = DbtTaskGroup(
        group_id="dbt_test",
        project_config=project_cfg,
        profile_config=profile_cfg,
        execution_config=base_exec_cfg,
        operator_args={
            "dbt_cmd": _cmd("test"),
            "retries": 2,
            "retry_delay": timedelta(minutes=5),
            "append_env": {"DBT_THREADS": str(DBT_THREADS)},
        },
    )

    dbt_seed >> dbt_run >> dbt_snapshot >> dbt_test


dbt_build_dag()
