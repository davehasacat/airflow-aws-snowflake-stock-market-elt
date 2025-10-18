# dags/utils/utils_secrets_probe.py
# =====================================================================
# Utils: Secrets Probe DAG
# ---------------------------------------------------------------------
# Purpose:
#   Focused test of Airflow ↔ AWS Secrets Manager integration.
#   Confirms backend wiring, Variables & Connections retrieval,
#   and optional IAM GetSecretValue access (no secret values logged).
# =====================================================================

from __future__ import annotations

import json
import os
from typing import List

import pendulum
from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.configuration import conf

import boto3
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError

AWS_CONFIG = Config(
    connect_timeout=5,
    read_timeout=20,
    retries={"max_attempts": 3, "mode": "standard"},
)

REQUIRED_VARIABLES: List[str] = [
    "s3_data_bucket",
    "polygon_stocks_api_key",
    "polygon_options_api_key",
]

REQUIRED_CONNECTIONS: List[str] = [
    "aws_default",
    "snowflake_default",
    "polygon_options_api",
]

OPTIONAL_DIRECT_SECRET_ENV = "TEST_SECRET_NAME"


@dag(
    dag_id="utils_secrets_probe",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["utils", "smoke", "infra", "secrets"],
    default_args={"retries": 0},
    doc_md="""
    # Utils: Secrets Probe
    Verifies Airflow ↔ AWS Secrets Manager:
    - Backend configured and active
    - Variables + Connections resolvable (no secret values logged)
    - Optional direct GetSecretValue check
    - Basic AWS identity + S3 read probe
    """,
)
def utils_secrets_probe():

    @task
    def show_env_and_backend():
        env = {
            "AWS_PROFILE": os.getenv("AWS_PROFILE"),
            "AWS_DEFAULT_REGION": os.getenv("AWS_DEFAULT_REGION"),
            "BUCKET_NAME": os.getenv("BUCKET_NAME"),
            OPTIONAL_DIRECT_SECRET_ENV: os.getenv(OPTIONAL_DIRECT_SECRET_ENV),
        }
        print("[ENV] 🔎 Key env:\n" + json.dumps(env, indent=2))

        backend_class = conf.get("secrets", "backend", fallback=None)
        backend_kwargs = conf.get("secrets", "backend_kwargs", fallback=None)

        print(f"[AF] 🔧 Secrets backend class: {backend_class}")
        print(f"[AF] 🔧 Secrets backend kwargs: {backend_kwargs}")

        if not backend_class or "aws.secrets.secrets_manager.SecretsManagerBackend" not in (backend_class or ""):
            raise RuntimeError(
                "[AF] ❌ Secrets backend is not AWS SecretsManagerBackend. "
                "Check AIRFLOW__SECRETS__BACKEND and AIRFLOW__SECRETS__BACKEND_KWARGS."
            )

        if not env.get("BUCKET_NAME"):
            raise RuntimeError("❌ BUCKET_NAME env var is required for the S3 probe (e.g., 'stock-market-elt').")

        return env

    @task
    def airflow_variables_probe():
        missing = []
        for name in REQUIRED_VARIABLES:
            try:
                _ = Variable.get(name)
                print(f"[AF] ✅ Variable present: '{name}'")
            except Exception:
                missing.append(name)

        if missing:
            raise RuntimeError(f"[AF] ❌ Missing Airflow Variables (via secrets backend): {missing}")

    @task
    def airflow_connections_probe():
        missing = []
        for conn_id in REQUIRED_CONNECTIONS:
            try:
                conn = BaseHook.get_connection(conn_id)
                safe_bits = {
                    "conn_id": conn.conn_id,
                    "conn_type": conn.conn_type,
                    "host": bool(conn.host),
                    "schema": bool(conn.schema),
                    "extra_present": bool(conn.extra),
                }
                print(f"[AF] ✅ Connection present: {json.dumps(safe_bits)}")
            except Exception:
                missing.append(conn_id)

        if missing:
            raise RuntimeError(f"[AF] ❌ Missing Airflow Connections (via secrets backend): {missing}")

    @task
    def optional_direct_boto3_secret_check(env: dict):
        name = env.get(OPTIONAL_DIRECT_SECRET_ENV)
        if not name:
            print(f"[AWS] ℹ️ Skipping direct GetSecretValue; set {OPTIONAL_DIRECT_SECRET_ENV} to enable.")
            return

        region = env.get("AWS_DEFAULT_REGION") or "us-east-2"
        try:
            sm = boto3.client("secretsmanager", region_name=region, config=AWS_CONFIG)
            resp = sm.get_secret_value(SecretId=name)
            ver = resp.get("VersionStages", [])
            print(f"[AWS] ✅ GetSecretValue ok for '{name}' (stages={ver}, value length hidden)")
        except (BotoCoreError, ClientError) as e:
            raise RuntimeError(f"[AWS] ❌ GetSecretValue failed for '{name}': {e}") from e

    @task
    def aws_identity_and_s3_probe(env: dict):
        region = env.get("AWS_DEFAULT_REGION") or "us-east-2"
        bucket = env.get("BUCKET_NAME")

        # STS
        try:
            sts = boto3.client("sts", region_name=region, config=AWS_CONFIG)
            ident = sts.get_caller_identity()
            arn = ident.get("Arn", "")
            principal = arn.split("/")[-1] if "/" in arn else arn
            print(f"[AWS] ✅ STS identity: Account={ident.get('Account')} Principal={principal}")
        except (BotoCoreError, ClientError) as e:
            raise RuntimeError(f"[AWS] ❌ STS get-caller-identity failed: {e}") from e

        # S3
        try:
            s3 = boto3.client("s3", region_name=region, config=AWS_CONFIG)
            s3.head_bucket(Bucket=bucket)
            resp = s3.list_objects_v2(Bucket=bucket, Prefix="raw/", MaxKeys=5)
            count = resp.get("KeyCount", 0)
            sample = [x["Key"] for x in resp.get("Contents", [])][:5] if count else []
            print(f"[S3] ✅ s3://{bucket}/raw/ list ok (KeyCount={count}, sample={sample})")
            if count == 0:
                print(f"[S3] ⚠️ Prefix empty: s3://{bucket}/raw/")
        except (BotoCoreError, ClientError) as e:
            raise RuntimeError(f"[S3] ❌ Probe failed for bucket '{bucket}': {e}") from e

    # ── Flow ──────────────────────────────────────────────────────────
    env = show_env_and_backend()
    airflow_variables_probe()
    airflow_connections_probe()
    optional_direct_boto3_secret_check(env)
    aws_identity_and_s3_probe(env)


utils_secrets_probe()
