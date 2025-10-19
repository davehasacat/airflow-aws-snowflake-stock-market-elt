#!/usr/bin/env bash
set -euo pipefail

: "${SNOWFLAKE_CONN_SECRET_NAME:=airflow/connections/snowflake_default}"
: "${DBT_PROFILE_NAME:=stock_market_elt}"          # matches dbt_project.yml's `profile:`
: "${DBT_DIR:=/usr/local/airflow/dbt}"
: "${DBT_PROFILES_DIR:=${DBT_DIR}}"

mkdir -p "${DBT_DIR}"

# Fetch the Airflow Snowflake connection JSON from Secrets Manager
secret_json="$(aws secretsmanager get-secret-value \
  --secret-id "${SNOWFLAKE_CONN_SECRET_NAME}" \
  --query 'SecretString' --output text)"

# Use Python (available in Astro runtime) to parse JSON and emit a dbt profiles.yml
python - <<'PY' "${secret_json}" "${DBT_PROFILE_NAME}" "${DBT_DIR}"
import os, sys, json, yaml, pathlib

secret = json.loads(sys.argv[1])
profile_name = sys.argv[2]
dbt_dir = pathlib.Path(sys.argv[3])

conn_type = secret.get("conn_type")
login = secret.get("login")
passphrase = secret.get("password")
extra = secret.get("extra", {}) or {}

key_path = extra.get("private_key_file")
account = extra.get("account")
warehouse = extra.get("warehouse")
role = extra.get("role")
database = extra.get("database")
schema = extra.get("schema")
keepalive = bool(extra.get("client_session_keep_alive", True))

missing = []
for k, v in {
    "login": login, "password(passphrase)": passphrase, "private_key_file": key_path,
    "account": account, "warehouse": warehouse, "role": role, "database": database, "schema": schema
}.items():
    if not v:
        missing.append(k)
if conn_type != "snowflake":
    missing.append("conn_type=snowflake")

if missing:
    raise SystemExit(f"Snowflake connection secret missing fields: {', '.join(missing)}")

profiles = {
    profile_name: {
        "target": "dev",
        "outputs": {
            "dev": {
                "type": "snowflake",
                "account": account,
                "user": login,
                "role": role,
                "database": database,
                "warehouse": warehouse,
                "schema": schema,
                "private_key_path": key_path,
                "private_key_passphrase": passphrase,
                "client_session_keep_alive": keepalive,
            }
        },
    }
}

dbt_dir.mkdir(parents=True, exist_ok=True)
with open(dbt_dir / "profiles.yml", "w", encoding="utf-8") as f:
    yaml.safe_dump(profiles, f, sort_keys=False)
PY

# Quick sanity: ensure the private key exists (mounted via docker-compose)
key_path="$(python - <<'PY'
import yaml, sys, pathlib
p = yaml.safe_load(open("/usr/local/airflow/dbt/profiles.yml"))
out = next(iter(p.values()))["outputs"]["dev"]["private_key_path"]
print(out)
PY
)"
if [ ! -f "$key_path" ]; then
  echo "ERROR: Private key file not found at ${key_path}. Check your ./keys mount and filename." >&2
  exit 1
fi

# Validate dbt structure (non-destructive)
export DBT_PROFILES_DIR="${DBT_PROFILES_DIR}"
export DBT_PROJECT_DIR="${DBT_DIR}"
dbt parse --project-dir "${DBT_DIR}" --profiles-dir "${DBT_PROFILES_DIR}" >/dev/null

echo "[dbt-bootstrap] profiles.yml rendered and validated."
