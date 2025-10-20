#!/usr/bin/env bash
set -euo pipefail

: "${DBT_BOOTSTRAP_ENABLED:=1}"   # set to 0 to skip bootstrap entirely
: "${DBT_BOOTSTRAP_STRICT:=1}"    # 1 = fail container on bootstrap error; 0 = warn and continue

run_bootstrap() {
  echo "[entrypoint] Running dbt bootstrap..."
  if /usr/local/bin/dbt_bootstrap.sh; then
    echo "[entrypoint] dbt bootstrap completed."
    return 0
  else
    if [ "${DBT_BOOTSTRAP_STRICT}" = "1" ]; then
      echo "[entrypoint] dbt bootstrap FAILED (strict=1). Exiting." >&2
      return 1
    else
      echo "[entrypoint] dbt bootstrap FAILED (strict=0). Continuing anyway." >&2
      return 0
    fi
  fi
}

if [ "${DBT_BOOTSTRAP_ENABLED}" = "1" ]; then
  LOCK="/tmp/dbt_bootstrap.lock"
  if command -v flock >/dev/null 2>&1; then
    # Only one service performs bootstrap; others wait up to 30s
    flock -w 30 "${LOCK}" bash -lc run_bootstrap
  else
    # Fallback lock using noclobber
    if ( set -o noclobber; : > "${LOCK}" ) 2>/dev/null; then
      trap 'rm -f "${LOCK}"' EXIT
      run_bootstrap
    else
      echo "[entrypoint] Another process is bootstrapping; waiting up to 30s..."
      for i in $(seq 1 30); do
        [ -f "${LOCK}" ] && sleep 1 || break
      done
    fi
  fi
else
  echo "[entrypoint] DBT_BOOTSTRAP_ENABLED=0 → skipping bootstrap."
fi

# Hand off to Astronomer’s stock entrypoint
exec /entrypoint "$@"
