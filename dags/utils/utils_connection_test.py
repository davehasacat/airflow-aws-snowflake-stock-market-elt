from __future__ import annotations

import os, uuid, json
import pendulum
import boto3

from airflow.decorators import dag, task
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.models import Variable

from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.profiles import SnowflakeUserPasswordProfileMapping

DBT_PROJECT_DIR = os.getenv("DBT_PROJECT_DIR")
DBT_EXECUTABLE_PATH = os.getenv("DBT_EXECUTABLE_PATH", "/usr/local/airflow/dbt_venv/bin/dbt")
AWS_REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-2")

def resolve_bucket_name() -> str:
    """
    Prefer Secrets Manager-backed Airflow Variable `s3_data_bucket`.
    Accepts either a plain string ("stock-market-elt") or a JSON object like {"s3_data_bucket":"stock-market-elt"}.
    Falls back to BUCKET_NAME from env.
    """
    try:
        raw = Variable.get("s3_data_bucket")
        try:
            maybe = json.loads(raw)
            if isinstance(maybe, dict) and "s3_data_bucket" in maybe:
                return str(maybe["s3_data_bucket"])
        except Exception:
            if raw:
                return raw
    except Exception:
        pass
    return os.getenv("BUCKET_NAME", "")

BUCKET_NAME = resolve_bucket_name()
SNOWFLAKE_CONN_ID = "snowflake_default"

@dag(
    dag_id="utils_connection_test",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["test", "aws", "s3", "snowflake", "dbt"],
    doc_md="""
    ### Full Stack Connection Test DAG
    Validates:
    - AWS via STS and an S3 round-trip (put/get/delete) using **profile-only** creds.
    - Snowflake via a `current_*` identity query (connection from Secrets Manager).
    - dbt connectivity for a representative source.
    """,
)
def utils_connection_test_dag():
    @task
    def test_aws_connection_and_s3_roundtrip():
        assert BUCKET_NAME, "Bucket name not set. Ensure Variable 's3_data_bucket' (plain string) or env BUCKET_NAME."
        sts = boto3.client("sts", region_name=AWS_REGION)
        ident = sts.get_caller_identity()
        print({"aws_account": ident["Account"], "aws_arn": ident["Arn"], "region": AWS_REGION})

        s3 = boto3.client("s3", region_name=AWS_REGION)
        key = f"tmp/airflow_roundtrip_{uuid.uuid4().hex}.txt"
        data = f"roundtrip-ok account={ident['Account']} arn={ident['Arn']}"
        s3.put_object(Bucket=BUCKET_NAME, Key=key, Body=data.encode("utf-8"))
        got = s3.get_object(Bucket=BUCKET_NAME, Key=key)["Body"].read().decode("utf-8")
        s3.delete_object(Bucket=BUCKET_NAME, Key=key)

        print({"bucket": BUCKET_NAME, "key": key, "wrote": data, "read": got})
        assert got == data, "S3 round-trip content mismatch"

    test_snowflake_connection = SnowflakeOperator(
        task_id="test_snowflake_connection",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql="""
            select
              current_user()      as user,
              current_role()      as role,
              current_warehouse() as warehouse,
              current_database()  as database,
              current_schema()    as schema;
        """,
        do_xcom_push=False,
    )

    test_dbt_connection = DbtTaskGroup(
        group_id="test_dbt_connection",
        project_config=ProjectConfig(dbt_project_path=DBT_PROJECT_DIR),
        profile_config=ProfileConfig(
            profile_name="stock_market_elt",
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
        operator_args={"select": "source:public.source_polygon_stock_bars_daily"},
    )

    aws_test = test_aws_connection_and_s3_roundtrip()
    [aws_test, test_snowflake_connection] >> test_dbt_connection

utils_connection_test_dag()
