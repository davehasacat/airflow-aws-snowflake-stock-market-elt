#!/usr/bin/env bash
set -euo pipefail
# Enable xtrace when DBT_BOOTSTRAP_DEBUG=1
[ "${DBT_BOOTSTRAP_DEBUG:-0}" = "1" ] && set -x

: "${DBT_BOOTSTRAP_ENABLED:=1}"   # 0 = skip bootstrap entirely
: "${DBT_BOOTSTRAP_STRICT:=1}"    # 1 = fail container on bootstrap error; 0 = warn & continue

LOCK="/tmp/dbt_bootstrap.lock"
DONE="/tmp/dbt_bootstrap.done"

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
  # If someone already finished bootstrap, skip.
  if [ -f "${DONE}" ]; then
    echo "[entrypoint] Bootstrap already completed earlier (found ${DONE})."
  else
    if command -v flock >/dev/null 2>&1; then
      # Try to acquire lock; if not acquired, just wait for completion
      {
        if flock -n 9; then
          # We hold the lock: perform bootstrap
          run_bootstrap && touch "${DONE}"
        else
          echo "[entrypoint] Another process is bootstrapping; waiting for completion..."
          # Wait until DONE appears (or lock released and DONE appears shortly after)
          for i in $(seq 1 300); do  # wait up to ~300s
            [ -f "${DONE}" ] && break
            sleep 1
          done
          [ -f "${DONE}" ] || echo "[entrypoint] Waited for bootstrap but ${DONE} not found; continuing."
        fi
      } 9>"${LOCK}"
    else
      # Fallback lock using noclobber
      if ( set -o noclobber; : > "${LOCK}" ) 2>/dev/null; then
        trap 'rm -f "${LOCK}"' EXIT
        run_bootstrap && touch "${DONE}"
      else
        echo "[entrypoint] Another process is bootstrapping; waiting for completion..."
        for i in $(seq 1 300); do
          [ -f "${DONE}" ] && break
          sleep 1
        done
        [ -f "${DONE}" ] || echo "[entrypoint] Waited for bootstrap but ${DONE} not found; continuing."
      fi
    fi
  fi
else
  echo "[entrypoint] DBT_BOOTSTRAP_ENABLED=0 → skipping bootstrap."
fi

# Hand off to Astronomer’s stock entrypoint (keeps base init)
if [ ! -x /entrypoint ]; then
  echo "[entrypoint] ERROR: /entrypoint not found or not executable." >&2
  ls -l / || true
  exit 127
fi
exec /entrypoint "$@"
