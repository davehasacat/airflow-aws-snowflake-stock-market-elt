#!/usr/bin/env bash
# Renders dbt profiles.yml from the Airflow Snowflake connection stored in
# AWS Secrets Manager (JSON at secret-id: $SNOWFLAKE_CONN_SECRET_NAME).
# Fails fast with actionable errors. Safe to run repeatedly (idempotent).

set -euo pipefail

# ────────────────────────────────────────────────────────────────────────────────
# Config (override via .env / docker-compose)
# ────────────────────────────────────────────────────────────────────────────────
: "${SNOWFLAKE_CONN_SECRET_NAME:=airflow/connections/snowflake_default}"
: "${DBT_PROFILE_NAME:=stock_market_elt}"           # must match dbt_project.yml: profile:
: "${DBT_DIR:=/usr/local/airflow/dbt}"
: "${DBT_PROFILES_DIR:=${DBT_DIR}}"
: "${DBT_PROJECT_DIR:=${DBT_DIR}}"
: "${DBT_BOOTSTRAP_VALIDATE:=1}"                    # set to 0 to skip dbt parse
: "${DBT_BIN:=/usr/local/airflow/dbt_venv/bin/dbt}" # prefer venv path; can override

# AWS CLI retry behavior (reduces transient failures)
export AWS_RETRY_MODE=standard
export AWS_MAX_ATTEMPTS=3

# Optional CLI timeouts (in seconds) – safe defaults
AWS_CONNECT_TIMEOUT="${AWS_CONNECT_TIMEOUT:-5}"
AWS_READ_TIMEOUT="${AWS_READ_TIMEOUT:-20}"

# ────────────────────────────────────────────────────────────────────────────────
# Helpers
# ────────────────────────────────────────────────────────────────────────────────
log() { printf '[dbt-bootstrap] %s\n' "$*" >&2; }
die() { printf '[dbt-bootstrap:ERROR] %s\n' "$*" >&2; exit 1; }

require_bin() {
  command -v "$1" >/dev/null 2>&1 || die "Missing required binary: $1"
}

# Resolve a working dbt binary without relying on PATH
resolve_dbt_bin() {
  local primary="${DBT_BIN:-/usr/local/airflow/dbt_venv/bin/dbt}"
  if [ -x "$primary" ]; then
    echo "$primary"; return 0
  fi
  if [ -x "/usr/local/bin/dbt" ]; then
    echo "/usr/local/bin/dbt"; return 0
  fi
  # Last resort: PATH
  if command -v dbt >/dev/null 2>&1; then
    command -v dbt; return 0
  fi
  return 1
}

# ────────────────────────────────────────────────────────────────────────────────
# Pre-flight checks
# ────────────────────────────────────────────────────────────────────────────────
require_bin aws
require_bin python

DBT_BIN_RESOLVED="$(resolve_dbt_bin)" || die "dbt executable not found at ${DBT_BIN} or /usr/local/bin/dbt, and not in PATH."
log "Using dbt binary: ${DBT_BIN_RESOLVED}"
"${DBT_BIN_RESOLVED}" --version >/dev/null 2>&1 || die "dbt is not executable or failed to run."

mkdir -p "${DBT_DIR}"

# Ensure AWS credentials are visible (best-effort, just to aid debugging)
if ! aws sts get-caller-identity --output text \
      --cli-connect-timeout "${AWS_CONNECT_TIMEOUT}" \
      --cli-read-timeout "${AWS_READ_TIMEOUT}" >/dev/null 2>&1; then
  die "AWS credentials not available to the container (cannot call STS). Check your ~/.aws mount and env vars."
fi

log "Fetching Snowflake connection from Secrets Manager: ${SNOWFLAKE_CONN_SECRET_NAME}"

# ────────────────────────────────────────────────────────────────────────────────
# Fetch secret JSON from AWS Secrets Manager (with simple retry)
# ────────────────────────────────────────────────────────────────────────────────
get_secret() {
  aws secretsmanager get-secret-value \
    --secret-id "${SNOWFLAKE_CONN_SECRET_NAME}" \
    --query 'SecretString' \
    --output text \
    --no-cli-pager \
    --cli-connect-timeout "${AWS_CONNECT_TIMEOUT}" \
    --cli-read-timeout "${AWS_READ_TIMEOUT}"
}

SECRET_JSON=""
for i in 1 2 3; do
  set +e
  SECRET_JSON="$(get_secret 2>/tmp/dbt_bootstrap_aws_err.log)"
  rc=$?
  set -e
  if [ $rc -eq 0 ] && [ -n "${SECRET_JSON}" ]; then
    break
  fi
  log "Retry $i/3: could not read secret (rc=$rc)."
  sleep $((i * 2))
done

if [ -z "${SECRET_JSON}" ]; then
  log "AWS error output:"
  sed -e 's/^/[aws-cli] /' /tmp/dbt_bootstrap_aws_err.log || true
  die "Failed to retrieve secret after 3 attempts."
fi

# ────────────────────────────────────────────────────────────────────────────────
# Transform Airflow connection JSON -> dbt profiles.yml (atomic write)
# ────────────────────────────────────────────────────────────────────────────────
TMP_PROFILES="$(mktemp "${DBT_DIR}/profiles.yml.tmp.XXXXXX")"
python - "$SECRET_JSON" "$DBT_PROFILE_NAME" "$TMP_PROFILES" <<'PYCODE'
import json, sys, yaml, pathlib

secret_str, profile_name, tmp_path = sys.argv[1], sys.argv[2], sys.argv[3]
try:
    secret = json.loads(secret_str)
except Exception as e:
    raise SystemExit(f"Secret is not valid JSON: {e}")

conn_type = secret.get("conn_type")
login = secret.get("login")
passphrase = secret.get("password")
extra = secret.get("extra") or {}

account   = extra.get("account")
warehouse = extra.get("warehouse")
role      = extra.get("role")
database  = extra.get("database")
schema    = extra.get("schema")
key_path  = extra.get("private_key_file")
keepalive = bool(extra.get("client_session_keep_alive", True))

missing = []
if conn_type != "snowflake":
    missing.append("conn_type must be 'snowflake'")
for k, v in {
    "login": login,
    "password(passphrase)": passphrase,
    "extra.account": account,
    "extra.warehouse": warehouse,
    "extra.role": role,
    "extra.database": database,
    "extra.schema": schema,
    "extra.private_key_file": key_path,
}.items():
    if not v:
        missing.append(k)

if missing:
    raise SystemExit("Secret missing fields: " + ", ".join(missing))

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

p = pathlib.Path(tmp_path)
p.parent.mkdir(parents=True, exist_ok=True)
with p.open("w", encoding="utf-8") as f:
    yaml.safe_dump(profiles, f, sort_keys=False)
PYCODE

# Move into place atomically, and lock down perms
chmod 600 "${TMP_PROFILES}" || true
mv -f "${TMP_PROFILES}" "${DBT_PROFILES_DIR%/}/profiles.yml"

log "profiles.yml rendered to ${DBT_PROFILES_DIR%/}/profiles.yml"

# ────────────────────────────────────────────────────────────────────────────────
# Sanity check: private key presence
# ────────────────────────────────────────────────────────────────────────────────
KEY_PATH="$(python - <<'PY'
import yaml, sys
p = yaml.safe_load(open(sys.argv[1], encoding="utf-8"))
k = next(iter(p.values()))["outputs"]["dev"]["private_key_path"]
print(k)
PY
"${DBT_PROFILES_DIR%/}/profiles.yml")"

if [ ! -f "${KEY_PATH}" ]; then
  die "Private key file not found at ${KEY_PATH}. Check your './keys' volume mount and filename."
fi

# ────────────────────────────────────────────────────────────────────────────────
# Optional validation (non-destructive)
# ────────────────────────────────────────────────────────────────────────────────
if [ "${DBT_BOOTSTRAP_VALIDATE}" = "1" ]; then
  log "Validating dbt project with dbt parse…"
  DBT_PROFILES_DIR="${DBT_PROFILES_DIR}" \
  DBT_PROJECT_DIR="${DBT_PROJECT_DIR}" \
  "${DBT_BIN_RESOLVED}" parse --quiet \
    --project-dir "${DBT_PROJECT_DIR}" \
    --profiles-dir "${DBT_PROFILES_DIR}"
  log "Validation successful."
else
  log "Skipping dbt parse (DBT_BOOTSTRAP_VALIDATE!=1)."
fi

log "Done."
