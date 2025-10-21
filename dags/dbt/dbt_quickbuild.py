# dags/dbt/dbt_quickbuild.py
# =============================================================================
# DBT Quick Build DAG
# -----------------------------------------------------------------------------
# Purpose:
#   Run `dbt build` end-to-end quickly for dev/testing. Designed to execute
#   inside your Astro Airflow container using the same virtualenv as dbt_core.
#   No full-refresh, no retries, minimal logging — perfect for short feedback loops.
# =============================================================================

from __future__ import annotations
from datetime import datetime

from airflow.decorators import dag, task
import subprocess
import os

# Default Airflow environment vars for dbt
DBT_PROJECT_DIR = os.getenv("DBT_PROJECT_DIR", "/usr/local/airflow/dbt")
DBT_PROFILES_DIR = "/tmp/dbt_profiles"
DBT_EXECUTABLE = os.getenv("DBT_EXECUTABLE_PATH", "/usr/local/airflow/dbt_venv/bin/dbt")


@dag(
    dag_id="dbt_quickbuild",
    description="Run dbt build quickly without full-refresh (dev only)",
    start_date=datetime(2024, 1, 1),
    schedule=None,  # manual trigger only
    catchup=False,
    tags=["dbt", "dev", "quickbuild"],
)
def dbt_quickbuild():
    @task
    def run_dbt_build():
        cmd = [
            DBT_EXECUTABLE,
            "build",
            "--target", "dev",
            "--profiles-dir", DBT_PROFILES_DIR,
            "--no-use-colors",
        ]
        print(f"Running: {' '.join(cmd)}")
        subprocess.run(cmd, cwd=DBT_PROJECT_DIR, check=True)

    run_dbt_build()


dbt_quickbuild()
