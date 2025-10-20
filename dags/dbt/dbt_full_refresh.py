# dags/dbt/dbt_full_refresh.py
# =============================================================================
# DBT Full Refresh DAG
# -----------------------------------------------------------------------------
# Purpose:
#   Executes `dbt build --full-refresh` for all models.
#   Use this only when schema changes, backfills, or scorched-earth resets occur.
#   This will drop and fully rebuild incremental models — heavier than quickbuild.
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
    dag_id="dbt_full_refresh",
    description="Run dbt build with --full-refresh for all models (manual)",
    start_date=datetime(2024, 1, 1),
    schedule=None,  # manual trigger only
    catchup=False,
    tags=["dbt", "maintenance", "full-refresh"],
)
def dbt_full_refresh():
    @task
    def run_dbt_full_refresh():
        cmd = [
            DBT_EXECUTABLE,
            "build",
            "--target", "dev",
            "--profiles-dir", DBT_PROFILES_DIR,
            "--full-refresh",
            "--no-use-colors",
        ]
        print(f"Running: {' '.join(cmd)}")
        subprocess.run(cmd, cwd=DBT_PROJECT_DIR, check=True)

    run_dbt_full_refresh()


dbt_full_refresh()
