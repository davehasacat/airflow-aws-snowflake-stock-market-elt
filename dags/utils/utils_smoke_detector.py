# dags/utils/utils_smoke_detector.py
from __future__ import annotations

import json
import os
import shutil
import subprocess
from typing import Optional

import pendulum
from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

# boto3 is available in Astronomer runtime
import boto3
from botocore.exceptions import BotoCoreError, ClientError


@dag(
    dag_id="utils_smoke_detector",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    schedule=None,            # run manually or trigger on deploy
    catchup=False,
    max_active_runs=1,
    tags=["utils", "smoke", "infra"],
    default_args={"retries": 0},
    doc_md="""
    # Utils: Smoke Detector
    Quick end-to-end health check for environment wiring:
    AWS creds → S3 bucket access, Snowflake connection & stage, dbt binary, and essential Airflow Variables.
    """,
)
def utils_smoke_detector():

    @task
    def show_env():
        env = {
            "AWS_PROFILE": os.getenv("AWS_PROFILE"),
            "AWS_DEFAULT_REGION": os.getenv("AWS_DEFAULT_REGION"),
            "AWS_SDK_LOAD_CONFIG": os.getenv("AWS_SDK_LOAD_CONFIG"),
            "BUCKET_NAME": os.getenv("BUCKET_NAME"),
            "DBT_EXECUTABLE_PATH": os.getenv("DBT_EXECUTABLE_PATH"),
            "DBT_THREADS": os.getenv("DBT_THREADS"),
            "DBT_TARGET_PATH": os.getenv("DBT_TARGET_PATH"),
            "HTTP_REQUEST_TIMEOUT_SECS": os.getenv("HTTP_REQUEST_TIMEOUT_SECS"),
            "POLYGON_REQUEST_DELAY_SECONDS": os.getenv("POLYGON_REQUEST_DELAY_SECONDS"),
            "API_POOL": os.getenv("API_POOL"),
        }
        print("🔎 Effective env (key settings):\n" + json.dumps(env, indent=2))
        missing = [k for k, v in env.items() if v in (None, "")]
        if missing:
            print(f"⚠️ Missing envs (may be okay if unused): {missing}")
        return env

    @task
    def aws_identity_and_s3_probe(env: dict):
        region = env.get("AWS_DEFAULT_REGION") or "us-east-1"
        bucket = env.get("BUCKET_NAME")
        if not bucket:
            raise RuntimeError("BUCKET_NAME env var is required for S3 probe.")

        try:
            sts = boto3.client("sts", region_name=region)
            ident = sts.get_caller_identity()
            print(f"✅ AWS STS identity: Account={ident.get('Account')} ARN={ident.get('Arn')}")
        except (BotoCoreError, ClientError) as e:
            raise RuntimeError(f"❌ STS get-caller-identity failed: {e}") from e

        try:
            s3 = boto3.client("s3", region_name=region)
            # Head bucket to validate existence & access
            s3.head_bucket(Bucket=bucket)
            # List a few keys under raw/ to validate ListObjects permission
            resp = s3.list_objects_v2(Bucket=bucket, Prefix="raw/", MaxKeys=5)
            count = resp.get("KeyCount", 0)
            sample = [x["Key"] for x in resp.get("Contents", [])][:5] if count else []
            print(f"✅ S3 probe ok: s3://{bucket}/raw/ (KeyCount={count}, sample={sample})")
        except (BotoCoreError, ClientError) as e:
            raise RuntimeError(f"❌ S3 probe failed for bucket '{bucket}': {e}") from e

    @task
    def airflow_secrets_presence_check():
        """Confirm expected Airflow artifacts exist (without leaking secrets)."""
        # Connection
        try:
            conn = BaseHook.get_connection("snowflake_default")
            extras = conn.extra_dejson or {}
            db = extras.get("database")
            schema = extras.get("schema")
            stage = extras.get("stage", "s3_stage")
            print(f"✅ Airflow connection 'snowflake_default' found (db={db}, schema={schema}, stage={stage})")
        except Exception as e:
            raise RuntimeError("❌ Airflow connection 'snowflake_default' not found or unreadable.") from e

        # Variables (do not print values)
        missing_vars = []
        for var_name in ["polygon_stocks_api_key", "polygon_options_api_key"]:
            try:
                _ = Variable.get(var_name)
                print(f"✅ Airflow Variable '{var_name}' present")
            except Exception:
                missing_vars.append(var_name)

        if missing_vars:
            raise RuntimeError(f"❌ Missing required Airflow Variables: {missing_vars}")

        return {"db": db, "schema": schema, "stage": extras.get("stage", "s3_stage")}

    @task
    def snowflake_probe(conn_info: dict):
        hook = SnowflakeHook(snowflake_conn_id="snowflake_default")

        # Verify context basics
        ctx_sql = "select current_role(), current_warehouse(), current_database(), current_schema()"
        try:
            rows = hook.get_first(ctx_sql)
            print(f"✅ Snowflake context: role={rows[0]}, wh={rows[1]}, db={rows[2]}, schema={rows[3]}")
        except Exception as e:
            raise RuntimeError(f"❌ Snowflake context query failed: {e}") from e

        # Verify stage is present (DESC STAGE expects no '@')
        db = conn_info.get("db")
        schema = conn_info.get("schema")
        stage_name = conn_info.get("stage") or "s3_stage"
        if not (db and schema and stage_name):
            raise RuntimeError("❌ Missing database/schema/stage in Snowflake connection extras.")

        fq_stage_no_at = f"{db}.{schema}.{stage_name}"
        try:
            hook.run(f"DESC STAGE {fq_stage_no_at}")
            print(f"✅ Snowflake stage exists: {fq_stage_no_at}")
        except Exception as e:
            raise RuntimeError(f"❌ Snowflake stage not accessible: {fq_stage_no_at} — {e}") from e

    @task
    def dbt_probe(env: dict):
        exe = env.get("DBT_EXECUTABLE_PATH") or "/usr/local/airflow/dbt_venv/bin/dbt"
        if not os.path.isabs(exe):
            # also allow PATH lookup
            resolved = shutil.which(exe)
        else:
            resolved = exe if os.path.exists(exe) else None

        if not resolved:
            raise RuntimeError(f"❌ dbt executable not found at '{exe}' (and not in PATH).")

        try:
            out = subprocess.check_output([resolved, "--version"], stderr=subprocess.STDOUT, text=True, timeout=60)
            print("✅ dbt --version\n" + out.strip())
        except subprocess.CalledProcessError as e:
            raise RuntimeError(f"❌ dbt --version failed:\n{e.output}") from e

    # Flow
    env = show_env()
    aws_identity_and_s3_probe(env)
    conn_info = airflow_secrets_presence_check()
    snowflake_probe(conn_info)
    dbt_probe(env)


utils_smoke_detector()
