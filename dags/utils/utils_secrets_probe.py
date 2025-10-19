# dags/utils/utils_secrets_probe.py
# =====================================================================
# Utils: Secrets Probe DAG
# ---------------------------------------------------------------------
# Purpose:
#   Focused test of Airflow ↔ AWS Secrets Manager integration.
#   Confirms backend wiring, Variable (bucket only) & Connections
#   (Polygon API keys), validates no API keys remain in Variables,
#   runs Polygon Stocks API smoke test, optional GetSecretValue IAM check,
#   then S3 probe.
# =====================================================================

from __future__ import annotations

import json
import os
from typing import List, Optional

import pendulum
from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.configuration import conf

import boto3
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


AWS_CONFIG = Config(
    connect_timeout=5,
    read_timeout=20,
    retries={"max_attempts": 3, "mode": "standard"},
)

# Variables: only bucket lives here
REQUIRED_VARIABLES: List[str] = [
    "s3_data_bucket",
]

# Connections: include Polygon API keys as connections (per your secret names)
REQUIRED_CONNECTIONS: List[str] = [
    "aws_default",
    "snowflake_default",
    "polygon_stocks_api_key",
    "polygon_options_api_key",
]

OPTIONAL_DIRECT_SECRET_ENV = "TEST_SECRET_NAME"


def _requests_session() -> requests.Session:
    s = requests.Session()
    retry = Retry(
        total=3,
        backoff_factor=0.5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    return s


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
    Verifies Airflow ↔ AWS Secrets Manager and Polygon:
    - Backend configured and active
    - Variable resolvable (s3_data_bucket as raw string)
    - Guardrail: throws if Polygon API keys exist as Variables
    - Required Connections resolvable (aws_default, snowflake_default, polygon_*_api_key)
    - Polygon Stocks API tiny fetch (AAPL)
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
    def guardrail_no_api_keys_in_variables():
        """
        Fail fast if Polygon API keys are still stored as Variables.
        Explains Variables vs Connections difference and how to fix.
        """
        misplaced = []
        for bad in ["polygon_stocks_api_key", "polygon_options_api_key"]:
            try:
                Variable.get(bad)
                misplaced.append(bad)
            except Exception:
                pass

        if misplaced:
            raise RuntimeError(
                "[GUARDRAIL] ❌ API keys must be stored as Airflow Connections, not Variables.\n"
                "• Variables (airflow/variables/*) are raw strings — good for simple config like 's3_data_bucket'.\n"
                "• Connections (airflow/connections/*) are JSON objects that map to Airflow's Connection model "
                "(conn_type/schema/host/login/password/extra). Put API keys there.\n\n"
                f"Found Variables that should be Connections: {misplaced}\n\n"
                "Create these Secrets Manager entries as Plaintext JSON (note stringified `extra`):\n"
                "— airflow/connections/polygon_stocks_api_key\n"
                "{\n"
                '  "conn_type": "http",\n'
                '  "schema": "https",\n'
                '  "host": "api.polygon.io",\n'
                '  "extra": "{\\"api_key\\": \\"YOUR_STOCKS_API_KEY\\"}"\n'
                "}\n"
                "— airflow/connections/polygon_options_api_key\n"
                "{\n"
                '  "conn_type": "http",\n'
                '  "schema": "https",\n'
                '  "host": "api.polygon.io",\n'
                '  "extra": "{\\"api_key\\": \\"YOUR_OPTIONS_API_KEY\\"}"\n'
                "}\n"
                "Then delete the Variables with those names."
            )

        print("[GUARDRAIL] ✅ No misplaced API keys in Variables.")

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
    def polygon_stocks_smoke_probe():
        """
        Use connection 'polygon_stocks_api_key' to fetch a tiny sample from Polygon Stocks API.
        Extracts key from password or extras.api_key; logs masked preview only.
        """
        def _sanitize(k: Optional[str]) -> str:
            if not k:
                return ""
            k = k.strip()
            if (k.startswith('"') and k.endswith('"')) or (k.startswith("'") and k.endswith("'")):
                k = k[1:-1].strip()
            return k

        try:
            conn = BaseHook.get_connection("polygon_stocks_api_key")
        except Exception as e:
            raise RuntimeError(
                "[POLY] ❌ Connection 'polygon_stocks_api_key' not found. "
                "Create Secrets Manager secret 'airflow/connections/polygon_stocks_api_key' with JSON including extra.api_key."
            ) from e

        api_key = _sanitize(conn.password) or _sanitize((conn.extra_dejson or {}).get("api_key", ""))
        if not api_key:
            raise RuntimeError(
                "[POLY] ❌ 'polygon_stocks_api_key' has no API key in password or extra.api_key. "
                "Fix the connection JSON in Secrets Manager."
            )

        masked = f"{api_key[:4]}...{api_key[-4:]} (len={len(api_key)})"
        print(f"[POLY] 🔎 Using sanitized key: {masked}")

        s = _requests_session()
        ticker = "AAPL"
        base = "https://api.polygon.io/v2/aggs/ticker"
        today = pendulum.today("UTC")
        tried = []

        for i in range(1, 8):
            d = today.subtract(days=i).format("YYYY-MM-DD")
            url = f"{base}/{ticker}/range/1/day/{d}/{d}"
            params = {"adjusted": "true", "limit": 1, "apiKey": api_key}
            tried.append(d)
            try:
                resp = s.get(url, params=params, timeout=15)
            except requests.RequestException as e:
                if i == 7:
                    raise RuntimeError(f"[POLY] ❌ Network error: {e}") from e
                continue

            q = resp.request.url.split("?")[0]
            print(f"[POLY] → GET {q} (date={d}) status={resp.status_code}")

            if resp.status_code in (401, 403):
                raise RuntimeError(
                    f"[POLY] ❌ Unauthorized/Forbidden (HTTP {resp.status_code}). "
                    "Likely wrong/expired key or whitespace/quotes in secret."
                )
            if resp.status_code == 429:
                raise RuntimeError("[POLY] ❌ Rate limited by Polygon (429).")
            if resp.status_code >= 500:
                if i == 7:
                    raise RuntimeError(f"[POLY] ❌ Server error {resp.status_code}: {resp.text[:200]}")
                continue
            if resp.status_code != 200:
                raise RuntimeError(f"[POLY] ❌ HTTP {resp.status_code}: {resp.text[:200]}")

            try:
                data = resp.json()
            except Exception:
                raise RuntimeError(f"[POLY] ❌ Non-JSON response: {resp.text[:200]}")

            results = data.get("results") or []
            if results:
                r = results[0]
                o, h, l, c, v = r.get("o"), r.get("h"), r.get("l"), r.get("c"), r.get("v")
                print(f"[POLY] ✅ {ticker} {d} agg OK (o={o}, h={h}, l={l}, c={c}, v={v})")
                return {"ticker": ticker, "date": d}

        raise RuntimeError(f"[POLY] ❌ No data found for {ticker} on dates tried: {tried}")

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
    guardrail_no_api_keys_in_variables()    # ⛔ throws if API keys still in Variables
    airflow_connections_probe()
    polygon_stocks_smoke_probe()
    optional_direct_boto3_secret_check(env)
    aws_identity_and_s3_probe(env)


utils_secrets_probe()
