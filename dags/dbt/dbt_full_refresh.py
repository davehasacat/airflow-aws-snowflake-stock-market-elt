# dags/dbt/dbt_full_refresh.py
# =============================================================================
# DBT Full Refresh DAG (minimal)
# Runs: dbt build --full-refresh
# =============================================================================

from __future__ import annotations
from datetime import datetime
import os
import subprocess

from airflow.decorators import dag, task

# Optional: use DBT_EXECUTABLE_PATH if set; otherwise rely on "dbt" on PATH
DBT_EXECUTABLE = os.getenv("DBT_EXECUTABLE_PATH", "dbt")
# Optional: run from your project directory if you prefer
DBT_PROJECT_DIR = os.getenv("DBT_PROJECT_DIR", "/usr/local/airflow/dbt")

@dag(
    dag_id="dbt_full_refresh",
    description="Run `dbt build --full-refresh` (manual)",
    start_date=datetime(2024, 1, 1),
    schedule=None,   # manual trigger only
    catchup=False,
    tags=["dbt", "full-refresh"],
)
def dbt_full_refresh():
    @task
    def run():
        cmd = [DBT_EXECUTABLE, "build", "--full-refresh"]
        print("Running:", " ".join(cmd))
        subprocess.run(cmd, cwd=DBT_PROJECT_DIR, check=True)

    run()

dbt_full_refresh()
