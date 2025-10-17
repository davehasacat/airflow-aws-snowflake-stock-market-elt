# dags/dbt/dbt_build.py
# =====================================================================
# dbt Build DAG
# ---------------------------------------------------------------------
# Purpose:
#   - Render a secure dbt profiles.yml at runtime from Airflow's
#     `snowflake_default` connection (sourced from AWS Secrets Manager)
#   - Optionally prime dbt with prior state (manifest.json) from S3
#   - Run: dbt debug → dbt deps → (optional) dbt source freshness → dbt build
#   - Upload core dbt artifacts (manifest.json, run_results.json, dbt.log) to S3
#
# Notes:
#   - This DAG is designed to be idempotent and safe for local + CI usage.
#   - We always pass `--profiles-dir` explicitly to dbt to avoid ambiguity.
#   - State mode uses the latest artifact path under s3://<BUCKET>/dbt/artifacts/<timestamp>/manifest.json
# =====================================================================

from __future__ import annotations

import os
import json
import shutil
import subprocess
from pathlib import Path
from typing import Optional

import pendulum
from airflow.decorators import dag, task, task_group
from airflow.models import Variable
from airflow.hooks.base import BaseHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

# Candidate locations to auto-discover dbt project when DBT_PROJECT_DIR is not set
DBT_DEFAULT_PATHS = [
    "/usr/local/airflow",
    "/usr/local/airflow/dbt",
    "/usr/local/airflow/dags/dbt",
]

# S3 prefix used when publishing/reading dbt artifacts for stateful builds
# Final layout: s3://<bucket>/dbt/artifacts/<YYYYMMDD_HHmmss>/{manifest.json,run_results.json}
ARTIFACTS_PREFIX = "dbt/artifacts"

# ────────────────────────────────────────────────────────────────────────────────
# Helpers: resolution, parsing, subprocess
# ────────────────────────────────────────────────────────────────────────────────
def _resolve_dbt_exe() -> str:
    """
    Resolve the dbt executable path.
    Default: /usr/local/airflow/dbt_venv/bin/dbt (installed in Dockerfile)
    Falls back to PATH lookup if a relative/executable name was provided.
    """
    exe = os.getenv("DBT_EXECUTABLE_PATH") or "/usr/local/airflow/dbt_venv/bin/dbt"
    if not os.path.isabs(exe):
        resolved = shutil.which(exe)
    else:
        resolved = exe if os.path.exists(exe) else None
    if not resolved:
        raise RuntimeError(f"[DBT] dbt executable not found at '{exe}' (and not in PATH).")
    return resolved

def _resolve_project_dir() -> str:
    """
    Resolve the dbt project directory containing dbt_project.yml.
    Priority:
      1) DBT_PROJECT_DIR (env)
      2) Common paths in DBT_DEFAULT_PATHS
      3) Current working directory
    """
    override = os.getenv("DBT_PROJECT_DIR")
    if override:
        if (Path(override) / "dbt_project.yml").exists():
            return str(Path(override).resolve())
        raise RuntimeError(f"[DBT] DBT_PROJECT_DIR set to '{override}' but dbt_project.yml not found there.")
    for base in DBT_DEFAULT_PATHS:
        if (Path(base) / "dbt_project.yml").exists():
            return str(Path(base).resolve())
    if Path("dbt_project.yml").exists():
        return str(Path(".").resolve())
    raise RuntimeError(
        "[DBT] Could not locate 'dbt_project.yml'. "
        "Place your dbt project at /usr/local/airflow or set DBT_PROJECT_DIR."
    )

def _normalize_target_value(val) -> Optional[str]:
    """
    Normalize dbt target value from:
      - plain string
      - JSON string (e.g., {"dbt_target":"ci"})
      - dicts with keys {dbt_target|target|value}
    Returns a cleaned string or None.
    """
    if val is None:
        return None
    if isinstance(val, dict):
        for k in ("dbt_target", "target", "value"):
            v = val.get(k)
            if isinstance(v, str) and v.strip():
                return v.strip()
        return None
    if isinstance(val, str):
        s_clean = val.strip()
        if not s_clean:
            return None
        # Allow JSON-like strings for Variables set via UI/CLI
        if s_clean.startswith("{") and s_clean.endswith("}"):
            try:
                obj = json.loads(s_clean)
                return _normalize_target_value(obj)
            except Exception:
                return None
        return s_clean
    return str(val).strip() or None

def _resolve_target() -> str:
    """
    Resolve dbt target:
      1) DBT_TARGET env var
      2) Airflow Variable 'dbt_target' (JSON or string)
      3) default 'ci'
    """
    env_target = os.getenv("DBT_TARGET")
    if env_target and env_target.strip():
        return env_target.strip()
    # Variable may be stored as raw string or JSON
    for deserialize in (True, False):
        try:
            val = Variable.get("dbt_target", deserialize_json=deserialize)
            tgt = _normalize_target_value(val)
            if tgt:
                return tgt
        except Exception:
            pass
    return "ci"

def _get_bool(name_env: str, name_var: str, default: bool = False) -> bool:
    """
    Uniformly read a boolean from Env or Airflow Variable.
    Accepted truthy: "1", "true", "t", "yes", "y" (case-insensitive).
    """
    raw = os.getenv(name_env)
    if raw is None:
        try:
            raw = Variable.get(name_var)
        except Exception:
            raw = None
    if raw is None:
        return default
    return str(raw).strip().lower() in ("1", "true", "t", "yes", "y")

def _get_str(name_env: str, name_var: str, default: Optional[str] = None) -> Optional[str]:
    """
    Uniformly read a string from Env or Airflow Variable with a default.
    """
    val = os.getenv(name_env)
    if val is None:
        try:
            val = Variable.get(name_var)
        except Exception:
            val = None
    return val if (val and str(val).strip()) else default

def _get_json(name_env: str, name_var: str) -> Optional[dict]:
    """
    Uniformly read a JSON dict from Env or Airflow Variable.
    Throws with a helpful error if JSON is malformed.
    """
    raw = _get_str(name_env, name_var)
    if not raw:
        return None
    try:
        return json.loads(raw)
    except Exception:
        raise RuntimeError(f"[DBT] {name_env}/{name_var} must be valid JSON (got: {raw!r})")

def _git_available() -> bool:
    """
    Check if `git` is available (dbt deps may pull Git-based packages).
    """
    return shutil.which("git") is not None

def _run(cmd: list[str], cwd: str, extra_env: dict | None = None, timeout_sec: int = 3600):
    """
    Run a shell command with merged environment, piping stdout/stderr to logs.
    Raises with full dbt output for easier debugging on failure.
    """
    env = os.environ.copy()
    if extra_env:
        env.update({k: v for k, v in extra_env.items() if v is not None})
    try:
        out = subprocess.check_output(
            cmd, cwd=cwd, text=True, stderr=subprocess.STDOUT, timeout=timeout_sec, env=env
        )
        print(out.strip())
        return out
    except subprocess.CalledProcessError as e:
        print("── dbt command failed ──")
        print("CWD:", cwd)
        print("Command:", " ".join(cmd))
        print("Output:\n", e.output)
        raise

# ────────────────────────────────────────────────────────────────────────────────
# profiles.yml rendering (Snowflake) from Airflow connection
# ────────────────────────────────────────────────────────────────────────────────
def _render_profiles_yml_from_airflow_conn(profiles_dir: Path, target: str) -> Path:
    """
    Render a minimal profiles.yml at runtime using Airflow's `snowflake_default` connection.
    - Reads creds/extras from AWS Secrets Manager via Airflow Secrets Backend.
    - Avoids baking secrets into the Docker image.
    - Writes to profiles_dir/profiles.yml and returns the file path.
    """
    conn = BaseHook.get_connection("snowflake_default")
    extras = conn.extra_dejson or {}
    user = conn.login or extras.get("user")
    password = conn.password or extras.get("password")
    account = extras.get("account") or extras.get("extra__snowflake__account")
    role = extras.get("role") or extras.get("extra__snowflake__role")
    warehouse = extras.get("warehouse") or extras.get("extra__snowflake__warehouse")
    database = extras.get("database") or extras.get("extra__snowflake__database")
    schema = extras.get("schema") or extras.get("extra__snowflake__schema") or "PUBLIC"
    authenticator = extras.get("authenticator")

    # Guard against misconfigured connections
    missing = [k for k, v in {
        "user": user, "password": password, "account": account,
        "role": role, "warehouse": warehouse, "database": database, "schema": schema
    }.items() if not v]
    if missing:
        raise RuntimeError(
            f"[DBT] snowflake_default missing required fields: {missing}. "
            "Fill Airflow connection extras (account/warehouse/database/role/schema) and password."
        )

    profiles_dir.mkdir(parents=True, exist_ok=True)
    profiles_yml = profiles_dir / "profiles.yml"
    content = {
        "stock_market_elt": {
            "target": target,
            "outputs": {
                target: {
                    "type": "snowflake",
                    "account": account,
                    "user": user,
                    "password": password,
                    "role": role,
                    "database": database,
                    "warehouse": warehouse,
                    "schema": schema,
                    "threads": int(os.getenv("DBT_THREADS", "4")),
                    # Optional SSO/OAuth authenticator if set in extras
                    **({"authenticator": authenticator} if authenticator else {}),
                    "client_session_keep_alive": False,
                }
            }
        }
    }

    # Tiny YAML dump to avoid bringing in PyYAML here
    def _dump_yaml(d, indent=0):
        lines = []
        for k, v in d.items():
            if isinstance(v, dict):
                lines.append("  " * indent + f"{k}:")
                lines.extend(_dump_yaml(v, indent + 1))
            else:
                sval = "true" if isinstance(v, bool) and v else "false" if isinstance(v, bool) else str(v)
                lines.append("  " * indent + f"{k}: {sval}")
        return lines

    profiles_yml.write_text("\n".join(_dump_yaml(content)) + "\n", encoding="utf-8")
    print(f"[DBT] Wrote profiles.yml to {profiles_yml} (from Airflow connection 'snowflake_default').")
    return profiles_yml

# ────────────────────────────────────────────────────────────────────────────────
# S3 Artifacts & State
# ────────────────────────────────────────────────────────────────────────────────
def _latest_manifest_to_state_dir(bucket: str) -> Optional[Path]:
    """
    Locate the latest manifest.json in S3 and prepare a local --state directory.
    - Lists keys under s3://<bucket>/<ARTIFACTS_PREFIX>/
    - Picks the newest <timestamp>/manifest.json
    - Downloads into a temp file under /tmp/dbt_state/
    - Renames that file to /tmp/dbt_state/manifest.json (dbt expects that exact name)

    IMPORTANT:
    - Airflow's S3Hook.download_file expects `local_path` to be a DIRECTORY.
      Do NOT pass a file path (e.g., '/tmp/dbt_state/manifest.json'); pass '/tmp/dbt_state'.
    """
    s3 = S3Hook()
    keys = s3.list_keys(bucket_name=bucket, prefix=f"{ARTIFACTS_PREFIX}/")
    if not keys:
        print("[S3] No prior artifacts found; skipping state.")
        return None

    manifest_keys = [k for k in keys if k.endswith("/manifest.json")]
    if not manifest_keys:
        print("[S3] No manifest.json found under artifacts; skipping state.")
        return None

    latest_key = sorted(manifest_keys)[-1]
    state_dir = Path("/tmp/dbt_state")
    state_dir.mkdir(parents=True, exist_ok=True)

    # Download *into the directory*; newer providers may return the new file path
    tmp_downloaded = s3.download_file(
        key=latest_key,
        bucket_name=bucket,
        local_path=str(state_dir)  # DIRECTORY, not file path
    )

    # Resolve the actual downloaded file path (provider-dependent)
    if tmp_downloaded and isinstance(tmp_downloaded, str):
        downloaded_path = Path(tmp_downloaded)
    else:
        # Fallback: find the newest airflow_tmp_* file created in state_dir
        candidates = sorted(
            state_dir.glob("airflow_tmp_*"),
            key=lambda p: p.stat().st_mtime,
            reverse=True,
        )
        if not candidates:
            raise RuntimeError("[S3] Download did not produce a file in state_dir.")
        downloaded_path = candidates[0]

    # Normalize the file name to "manifest.json" at the root of --state
    dest = state_dir / "manifest.json"
    downloaded_path.replace(dest)

    print(f"[S3] Downloaded prior manifest to {dest} from s3://{bucket}/{latest_key}")
    return state_dir

# ────────────────────────────────────────────────────────────────────────────────
# DAG
# ────────────────────────────────────────────────────────────────────────────────
@dag(
    dag_id="dbt_build",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    schedule=None,                 # Manual trigger by default
    catchup=False,                 # No backfills for ad-hoc builds
    max_active_runs=1,             # Protect Snowflake & vendor quotas
    tags=["dbt", "build"],
    default_args={"retries": 0},   # Fail fast; dbt logs are clear when it fails
    doc_md="""
    # dbt build
    - Renders a secure dbt `profiles.yml` from Airflow `snowflake_default`
    - Optional **stateful builds** using the latest manifest from S3
    - Runs: `dbt debug` → `dbt deps` → (optional) `dbt source freshness` → `dbt build`
    - Uploads: `manifest.json`, `run_results.json`, and `logs/dbt.log` to S3
    """,
)
def dbt_build():

    @task
    def prepare():
        """
        Resolve paths, target, and runtime controls.
        Pulls tuning knobs from Env or Airflow Variables:
          - DBT_FULL_REFRESH / dbt_full_refresh
          - DBT_SELECT / dbt_select
          - DBT_EXCLUDE / dbt_exclude
          - DBT_VARS_JSON / dbt_vars_json
          - DBT_USE_STATE / dbt_use_state
        """
        exe = _resolve_dbt_exe()
        project_dir = _resolve_project_dir()
        target = _resolve_target()
        bucket = os.getenv("BUCKET_NAME")
        if not bucket:
            raise RuntimeError("BUCKET_NAME env var is required.")
        # runtime knobs (env or Variables)
        full_refresh = _get_bool("DBT_FULL_REFRESH", "dbt_full_refresh", False)
        select = _get_str("DBT_SELECT", "dbt_select")  # e.g. "state:modified+"
        exclude = _get_str("DBT_EXCLUDE", "dbt_exclude")
        vars_json = _get_json("DBT_VARS_JSON", "dbt_vars_json")
        use_state = _get_bool("DBT_USE_STATE", "dbt_use_state", True)

        print(f"[DBT] exe={exe}")
        print(f"[DBT] project_dir={project_dir}")
        print(f"[DBT] target={target}")
        print(f"[DBT] threads={os.getenv('DBT_THREADS') or '(dbt default)'}")
        print(f"[S3] bucket={bucket}")
        print(f"[DBT] full_refresh={full_refresh} select={select!r} exclude={exclude!r} use_state={use_state}")

        return {
            "exe": exe,
            "project_dir": project_dir,
            "target": target,
            "bucket": bucket,
            "full_refresh": full_refresh,
            "select": select,
            "exclude": exclude,
            "vars_json": vars_json,
            "use_state": use_state,
            "git_ok": _git_available(),
        }

    @task
    def render_profiles(cfg: dict):
        """
        Write a temp profiles.yml at runtime based on `snowflake_default`.
        Returns cfg extended with 'profiles_dir'.
        """
        profiles_dir = Path("/tmp/dbt_profiles")
        _ = _render_profiles_yml_from_airflow_conn(profiles_dir, cfg["target"])
        print(f"[DBT] Using DBT_PROFILES_DIR={profiles_dir}")
        # Optional: we resume WH in a separate task to parallelize waiting.
        return {**cfg, "profiles_dir": str(profiles_dir)}

    @task
    def resume_warehouse():
        """
        Best-effort resume of the currently active Snowflake warehouse.
        Safe to ignore exceptions (e.g., insufficient privileges).
        """
        hook = SnowflakeHook(snowflake_conn_id="snowflake_default")
        try:
            wh = hook.get_first("select current_warehouse()")[0]
            if wh:
                hook.run(f"alter warehouse {wh} resume if suspended")
                print(f"[SF] Warehouse resumed (if suspended): {wh}")
        except Exception as e:
            print(f"[SF] Skipping warehouse resume: {e}")

    @task
    def prepare_state(cfg: dict):
        """
        If enabled, download latest manifest.json from S3 and return a --state directory.
        Otherwise return cfg with state_dir=None.
        """
        state_dir = None
        if cfg.get("use_state"):
            state_dir = _latest_manifest_to_state_dir(cfg["bucket"])
        return {**cfg, "state_dir": str(state_dir) if state_dir else None}

    @task_group
    def dbt_steps(cfg: dict):
        """
        Group the dbt steps so they’re easy to read in the Airflow graph view.
        All dbt invocations:
          - pass --profiles-dir explicitly
          - honor select/exclude/state/full-refresh/vars when set
        """

        def _env(cfg: dict) -> dict:
            # Keep env minimal; most flags are passed via CLI to avoid surprises
            return {
                "DBT_PROFILES_DIR": cfg["profiles_dir"],
                "DBT_TARGET": cfg["target"],
            }

        def _base_cmd(cfg: dict, subcmd: list[str]) -> list[str]:
            """
            Compose a dbt command for a given subcommand (e.g., ["build"]).
            Always pass explicit --profiles-dir for deterministic behavior.
            """
            cmd = [
                cfg["exe"], *subcmd, "--target", cfg["target"],
                "--profiles-dir", cfg["profiles_dir"], "--no-use-colors"
            ]
            # Selection
            if cfg.get("select"):
                cmd += ["--select", cfg["select"]]
            if cfg.get("exclude"):
                cmd += ["--exclude", cfg["exclude"]]
            # State
            if cfg.get("state_dir"):
                cmd += ["--state", cfg["state_dir"]]
            # Vars
            if cfg.get("vars_json"):
                cmd += ["--vars", json.dumps(cfg["vars_json"])]
            # Full refresh
            if cfg.get("full_refresh") and subcmd[0] in ("run", "build"):
                cmd.append("--full-refresh")
            return cmd

        @task
        def debug(cfg: dict):
            """Quick connectivity + config check (fast fail)."""
            _run(_base_cmd(cfg, ["debug"]), cfg["project_dir"], extra_env=_env(cfg), timeout_sec=600)

        @task
        def deps(cfg: dict):
            """Install dbt packages if git is available; otherwise skip cleanly."""
            if not cfg.get("git_ok", False):
                print("[DBT] Skipping `dbt deps` because `git` is not available in PATH.")
                return
            _run([cfg["exe"], "deps", "--profiles-dir", cfg["profiles_dir"]], cfg["project_dir"], extra_env=_env(cfg), timeout_sec=1200)

        @task
        def freshness(cfg: dict):
            """Optional source freshness; non-fatal if it fails."""
            try:
                _run(_base_cmd(cfg, ["source", "freshness"]), cfg["project_dir"], extra_env=_env(cfg), timeout_sec=1800)
            except subprocess.CalledProcessError:
                print("[DBT] source freshness failed (continuing). See logs above for details.")

        @task
        def build(cfg: dict):
            """Main transformation run: seeds/snapshots/models/tests in dependency order."""
            _run(_base_cmd(cfg, ["build"]), cfg["project_dir"], extra_env=_env(cfg), timeout_sec=7200)

        # Order: debug → deps → freshness → build
        debug(cfg) >> deps(cfg) >> freshness(cfg) >> build(cfg)

    @task
    def publish_artifacts(cfg: dict):
        """
        Upload artifacts to S3 for observability and future state builds.
        Files uploaded (if present):
          - target/manifest.json
          - target/run_results.json
          - logs/dbt.log
        """
        project_dir = cfg["project_dir"]; bucket = cfg["bucket"]
        ts_folder = pendulum.now("UTC").format("YYYYMMDD_HHmmss")
        base_key = f"{ARTIFACTS_PREFIX}/{ts_folder}/"

        target_dir = Path(project_dir) / "target"
        logs_dir = Path(project_dir) / "logs"
        files = [
            target_dir / "manifest.json",
            target_dir / "run_results.json",
            logs_dir / "dbt.log",
        ]
        if not any(p.exists() for p in files):
            print("[DBT] No artifacts found; skipping upload.")
            return

        s3 = S3Hook()
        for f in files:
            if f.exists():
                key = base_key + f.name
                s3.load_file(filename=str(f), key=key, bucket_name=bucket, replace=True)
                print(f"[S3] Uploaded s3://{bucket}/{key}")

    # DAG wiring
    cfg = prepare()
    cfg2 = render_profiles(cfg)
    cfg3 = prepare_state(cfg2)
    resume_warehouse() >> dbt_steps(cfg3) >> publish_artifacts(cfg3)

# Instantiate the DAG
dbt_build()
