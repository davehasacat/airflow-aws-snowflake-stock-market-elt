# dags/utils/utils_smoke_detector.py
# =====================================================================
# Utils: Smoke Detector DAG
# ---------------------------------------------------------------------
# Purpose:
#   Quick, end-to-end infrastructure health check:
#     - AWS credentials & S3 access
#     - Airflow Pools, Variables, and Snowflake connection presence
#     - Snowflake context + external stage reachability
#     - dbt binary resolution / version
#
# Notes:
#   - This is intentionally fast and safe: it reads metadata, does a
#     small LIST on S3/Snowflake stage, and avoids printing secrets.
#   - It’s perfect to trigger after clean builds or environment resets.
#   - Fails hard on missing prerequisites so you get immediate signal.
# =====================================================================

from __future__ import annotations

import json
import os
import shutil
import subprocess
from typing import Optional

import pendulum
from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from airflow.models import Variable, Pool
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

# boto3 ships with Astronomer Runtime; used for AWS STS + S3 probes
import boto3
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError


# Conservative, robust client config for fast feedback
AWS_CONFIG = Config(
    connect_timeout=5,
    read_timeout=20,
    retries={"max_attempts": 3, "mode": "standard"},
)


@dag(
    dag_id="utils_smoke_detector",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    schedule=None,            # manual runs only; trigger on deploy or reset
    catchup=False,
    max_active_runs=1,
    tags=["utils", "smoke", "infra"],
    default_args={"retries": 0},
    doc_md="""
    # Utils: Smoke Detector
    Quick end-to-end health check for environment wiring:
    **AWS creds** → **S3** bucket access, **Snowflake** connection & stage, **dbt** binary,
    and essential **Airflow** Variables/Pool.
    """,
)
def utils_smoke_detector():

    @task
    def show_env():
        """
        Snapshot key env vars and fail-fast on BUCKET_NAME.
        Avoids dumping the entire environment (keeps logs clean & safe).
        """
        env = {
            "AWS_PROFILE": os.getenv("AWS_PROFILE"),
            "AWS_DEFAULT_REGION": os.getenv("AWS_DEFAULT_REGION"),
            "AWS_SDK_LOAD_CONFIG": os.getenv("AWS_SDK_LOAD_CONFIG"),
            "BUCKET_NAME": os.getenv("BUCKET_NAME"),
            "DBT_EXECUTABLE_PATH": os.getenv("DBT_EXECUTABLE_PATH"),
            "DBT_THREADS": os.getenv("DBT_THREADS"),
            "DBT_TARGET_PATH": os.getenv("DBT_TARGET_PATH"),  # optional
            "HTTP_REQUEST_TIMEOUT_SECS": os.getenv("HTTP_REQUEST_TIMEOUT_SECS"),
            "POLYGON_REQUEST_DELAY_SECONDS": os.getenv("POLYGON_REQUEST_DELAY_SECONDS"),
            "API_POOL": os.getenv("API_POOL"),
        }
        print("[ENV] 🔎 Effective env (key settings):\n" + json.dumps(env, indent=2))
        missing = [k for k, v in env.items() if v in (None, "")]
        if missing:
            print(f"[ENV] ⚠️ Missing envs (may be okay if unused): {missing}")

        # Hard requirement: many checks depend on this
        if not env.get("BUCKET_NAME"):
            raise RuntimeError("❌ BUCKET_NAME env var is required (e.g., 'stock-market-elt').")
        return env

    @task
    def aws_identity_and_s3_probe(env: dict):
        """
        Verify AWS identity (STS) and basic S3 access:
          - head_bucket (existence + perms)
          - list_objects_v2 on 'raw/' prefix (read perm sanity)
        """
        region = env.get("AWS_DEFAULT_REGION") or "us-east-1"
        bucket = env.get("BUCKET_NAME")

        # STS identity probe
        try:
            sts = boto3.client("sts", region_name=region, config=AWS_CONFIG)
            ident = sts.get_caller_identity()
            arn = ident.get("Arn", "")
            principal = arn.split("/")[-1] if "/" in arn else arn
            print(f"[AWS] ✅ STS identity: Account={ident.get('Account')} Principal={principal}")
        except (BotoCoreError, ClientError) as e:
            raise RuntimeError(f"[AWS] ❌ STS get-caller-identity failed: {e}") from e

        # S3 existence + list probe
        try:
            s3 = boto3.client("s3", region_name=region, config=AWS_CONFIG)
            s3.head_bucket(Bucket=bucket)  # lightweight existence/permission check

            resp = s3.list_objects_v2(Bucket=bucket, Prefix="raw/", MaxKeys=5)
            count = resp.get("KeyCount", 0)
            sample = [x["Key"] for x in resp.get("Contents", [])][:5] if count else []
            print(f"[S3] ✅ s3://{bucket}/raw/ list ok (KeyCount={count}, sample={sample})")
            if count == 0:
                print(f"[S3] ⚠️ Prefix empty: s3://{bucket}/raw/ — create it or verify IAM write perms.")
        except (BotoCoreError, ClientError) as e:
            raise RuntimeError(f"[S3] ❌ Probe failed for bucket '{bucket}': {e}") from e

    @task
    def airflow_secrets_presence_check():
        """
        Confirm required Airflow assets:
          - Pool presence (api_pool by default)
          - Connection 'snowflake_default' exists and has expected extras
          - Required Variables exist (without showing values)
        """
        # Pool presence (shared rate-limiting for API-heavy tasks)
        pool_name = os.getenv("API_POOL", "api_pool")
        try:
            Pool.get_pool(pool_name)
            print(f"[AF] ✅ Airflow pool '{pool_name}' present")
        except Exception:
            raise RuntimeError(
                f"[AF] ❌ Required Airflow pool '{pool_name}' not found. "
                f"Create via UI or airflow_settings.yaml"
            )

        # Connection (do not print secrets)
        try:
            conn = BaseHook.get_connection("snowflake_default")
            extras = conn.extra_dejson or {}
            db = extras.get("database")
            schema = extras.get("schema")
            stage = extras.get("stage", "s3_stage")
            print(f"[AF] ✅ Conn 'snowflake_default' (db={db}, schema={schema}, stage={stage})")
        except Exception as e:
            raise RuntimeError("[AF] ❌ Connection 'snowflake_default' not found or unreadable.") from e

        # Variables (names only)
        missing_vars = []
        for var_name in ["polygon_stocks_api_key", "polygon_options_api_key"]:
            try:
                _ = Variable.get(var_name)
                print(f"[AF] ✅ Airflow Variable '{var_name}' present")
            except Exception:
                missing_vars.append(var_name)

        if missing_vars:
            raise RuntimeError(f"[AF] ❌ Missing required Airflow Variables: {missing_vars}")

        return {"db": db, "schema": schema, "stage": stage}

    @task
    def snowflake_probe(conn_info: dict):
        """
        Check Snowflake context and stage reachability:
          - current_role/warehouse/database/schema
          - stage existence via DESC STAGE <db>.<schema>.<stage>
          - external location reachability via LIST @<stage>
        Gotcha:
          - DESC STAGE expects NO leading '@'
          - LIST requires '@' prefix
        """
        hook = SnowflakeHook(snowflake_conn_id="snowflake_default")

        # Context sanity check
        ctx_sql = "select current_role(), current_warehouse(), current_database(), current_schema()"
        try:
            rows = hook.get_first(ctx_sql)
            print(f"[SF] ✅ Context: role={rows[0]}, wh={rows[1]}, db={rows[2]}, schema={rows[3]}")
        except Exception as e:
            raise RuntimeError(f"[SF] ❌ Context query failed: {e}") from e

        # Stage checks
        db = conn_info.get("db")
        schema = conn_info.get("schema")
        stage_name = conn_info.get("stage") or "s3_stage"
        if not (db and schema and stage_name):
            raise RuntimeError("[SF] ❌ Missing database/schema/stage in Snowflake connection extras.")

        fq_stage_no_at = f"{db}.{schema}.{stage_name}"
        try:
            # Verify the stage object itself exists
            hook.run(f"DESC STAGE {fq_stage_no_at}")
            print(f"[SF] ✅ Stage exists: {fq_stage_no_at}")

            # Verify the external location is reachable (S3/GCS/Azure) via LIST
            hook.run(f"LIST @{fq_stage_no_at} PATTERN='.*'")

            # Pull a single row from LIST output for basic proof of life
            row = hook.get_first("SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())) LIMIT 1")
            if row:
                print(f"[SF] ✅ Stage LIST ok (reachable), sample row: {row}")
            else:
                print(f"[SF] ⚠️ Stage LIST returned no rows (stage reachable but empty?)")

        except Exception as e:
            raise RuntimeError(f"[SF] ❌ Stage not accessible: {fq_stage_no_at} — {e}") from e

    @task
    def dbt_probe(env: dict):
        """
        Resolve dbt binary and print version.
        - Honors DBT_EXECUTABLE_PATH when set; otherwise, uses default venv path.
        - Falls back to PATH lookup if a non-absolute name is provided.
        """
        exe = env.get("DBT_EXECUTABLE_PATH") or "/usr/local/airflow/dbt_venv/bin/dbt"
        if not os.path.isabs(exe):
            resolved = shutil.which(exe)  # allow 'dbt' if in PATH
        else:
            resolved = exe if os.path.exists(exe) else None

        if not resolved:
            raise RuntimeError(f"[DBT] ❌ dbt executable not found at '{exe}' (and not in PATH).")

        print(f"[DBT] 🔧 Resolving dbt executable: requested='{exe}', resolved='{resolved}'")
        try:
            out = subprocess.check_output(
                [resolved, "--version"],
                stderr=subprocess.STDOUT,
                text=True,
                timeout=45,
            )
            print("[DBT] ✅ dbt --version\n" + out.strip())
        except subprocess.CalledProcessError as e:
            raise RuntimeError(f"[DBT] ❌ dbt --version failed:\n{e.output}") from e

    # ── Orchestration / Flow ──────────────────────────────────────────
    env = show_env()
    aws_identity_and_s3_probe(env)
    conn_info = airflow_secrets_presence_check()
    snowflake_probe(conn_info)
    dbt_probe(env)


# Instantiate the DAG
utils_smoke_detector()
