from __future__ import annotations
import os
import pendulum
import json
from airflow.decorators import dag, task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig

# --- dbt Configuration ---
DBT_PROJECT_DIR = os.getenv("DBT_PROJECT_DIR", "/usr/local/airflow/dbt")
DBT_EXECUTABLE_PATH = os.getenv("DBT_EXECUTABLE_PATH", "/usr/local/airflow/dbt_venv/bin/dbt")
SNOWFLAKE_CONN_ID = "snowflake_default"


@dag(
    dag_id="dbt_test",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    schedule="0 2 * * *",
    catchup=False,
    tags=["dbt", "monitoring", "snowflake"],
)
def dbt_test_dag():
    """
    This DAG runs dbt tests and loads the results to a Snowflake table.
    """
    run_dbt_tests = DbtTaskGroup(
    )

    @task
    def parse_and_load_test_results():
        """
        Parses the dbt run_results.json artifact and loads the test
        results into a dedicated table in Snowflake.
        """

        snowflake_hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        
        # Create table if it doesn't exist
        snowflake_hook.run("""
            CREATE TABLE IF NOT EXISTS public.dbt_test_results (
                test_id TEXT PRIMARY KEY,
                test_name TEXT,
                status VARCHAR(10),
                failures INTEGER,
                execution_time FLOAT,
                run_generated_at TIMESTAMPTZ,
                loaded_at TIMESTAMPTZ DEFAULT NOW()
            );
        """)

        # --- REVISED SECTION ---
        # Use parameter binding for safe and robust upserting.
        # This approach is not vulnerable to SQL injection.
        
        # First, create a temporary table to stage the new results
        snowflake_hook.run("CREATE OR REPLACE TEMPORARY TABLE dbt_test_results_staging LIKE public.dbt_test_results;")
        
        # Insert new results into the staging table
        # The snowflake-connector-python supports inserting a list of tuples
        rows_to_insert = [
            (
                res["test_id"],
                res["test_name"],
                res["status"],
                res["failures"],
                res["execution_time"],
                res["run_generated_at"],
            )
            for res in test_results
        ]
        
        # Build the INSERT statement with the correct number of placeholders
        insert_sql = f"INSERT INTO dbt_test_results_staging (test_id, test_name, status, failures, execution_time, run_generated_at) VALUES (%s, %s, %s, %s, %s, %s)"
        snowflake_hook.run(insert_sql, parameters=rows_to_insert)

        # Now, merge from the staging table into the final table
        merge_sql = """
            MERGE INTO public.dbt_test_results t
            USING dbt_test_results_staging s
            ON t.test_id = s.test_id
            WHEN MATCHED THEN
                UPDATE SET
                    t.status = s.status,
                    t.failures = s.failures,
                    t.execution_time = s.execution_time,
                    t.run_generated_at = s.run_generated_at,
                    t.loaded_at = NOW()
            WHEN NOT MATCHED THEN
                INSERT (test_id, test_name, status, failures, execution_time, run_generated_at)
                VALUES (s.test_id, s.test_name, s.status, s.failures, s.execution_time, s.run_generated_at);
        """
        snowflake_hook.run(merge_sql)
        print(f"Successfully loaded {len(test_results)} test results.")

    run_dbt_tests >> parse_and_load_test_results()

dbt_test_dag()
