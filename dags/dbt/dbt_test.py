from __future__ import annotations
import os
import pendulum
import json
from airflow.decorators import dag, task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig

# --- dbt Configuration ---
DBT_PROJECT_DIR = os.getenv("DBT_PROJECT_DIR", "/usr/local/airflow/dbt")
SNOWFLAKE_CONN_ID = "snowflake_default"


@dag(
    dag_id="dbt_test",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    schedule="0 2 * * *",
    catchup=False,
    tags=["dbt", "monitoring", "snowflake"],
    doc_md="""
    ### dbt Test Runner and Parser DAG

    This DAG runs `dbt test` and then uses a Python task to parse the
    `run_results.json` artifact and load the results into a table
    in the data warehouse for monitoring.
    """,
)
def dbt_test_dag():
    """
    This DAG runs dbt tests and loads the results to a Snowflake table.
    """
    run_dbt_tests = DbtTaskGroup(
        group_id="run_dbt_tests",
        project_config=ProjectConfig(
            dbt_project_path=DBT_PROJECT_DIR,
        ),
        profile_config=ProfileConfig(
            profile_name="stock_market_elt",
            target_name="dev",
            profiles_yml_filepath=os.path.join(DBT_PROJECT_DIR, "profiles.yml"),
        ),
        operator_args={"cmd": "test"}
    )

    @task
    def parse_and_load_test_results():
        """
        Parses the dbt run_results.json artifact and loads the test
        results into a dedicated table in Snowflake using a robust,
        staging table approach.
        """
        run_results_path = os.path.join(DBT_PROJECT_DIR, "target/run_results.json")
        
        try:
            with open(run_results_path, "r") as f:
                data = json.load(f)
        except FileNotFoundError:
            print(f"Could not find run_results.json at {run_results_path}. Skipping.")
            return

        test_results = []
        for result in data.get("results", []):
            if result["unique_id"].startswith("test."):
                test_results.append({
                    "test_id": result["unique_id"],
                    "test_name": result["unique_id"].split(".")[-1],
                    "status": result["status"],
                    "failures": result.get("failures", 0),
                    "execution_time": result["execution_time"],
                    "run_generated_at": data["metadata"]["generated_at"]
                })

        if not test_results:
            print("No test results found in the artifact. Skipping load.")
            return

        snowflake_hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        
        # Create the target table if it doesn't exist
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

        # Create a temporary table to stage the new results
        snowflake_hook.run("CREATE OR REPLACE TEMPORARY TABLE dbt_test_results_staging LIKE public.dbt_test_results;")
        
        # Prepare rows for insertion
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
        
        # Insert new results into the staging table using parameter binding for safety
        insert_sql = "INSERT INTO dbt_test_results_staging (test_id, test_name, status, failures, execution_time, run_generated_at) VALUES (%s, %s, %s, %s, %s, %s)"
        snowflake_hook.run(insert_sql, parameters=rows_to_insert)

        # Merge from the staging table into the final table
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

    # Set the task dependency
    run_dbt_tests >> parse_and_load_test_results()

dbt_test_dag()
