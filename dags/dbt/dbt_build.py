# dags/dbt/dbt_build.py
from __future__ import annotations

import os
import json
import shutil
import subprocess
from pathlib import Path
from textwrap import dedent

import pendulum
from airflow.decorators import dag, task, task_group
from airflow.models import Variable
from airflow.hooks.base import BaseHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

DBT_DEFAULT_PATHS = [
    "/usr/local/airflow",          # repo root (most common)
    "/usr/local/airflow/dbt",
    "/usr/local/airflow/dags/dbt",
]

def _resolve_dbt_exe() -> str:
    exe = os.getenv("DBT_EXECUTABLE_PATH") or "/usr/local/airflow/dbt_venv/bin/dbt"
    if not os.path.isabs(exe):
        resolved = shutil.which(exe)
    else:
        resolved = exe if os.path.exists(exe) else None
    if not resolved:
        raise RuntimeError(f"[DBT] dbt executable not found at '{exe}' (and not in PATH).")
    return resolved

def _resolve_project_dir() -> str:
    override = os.getenv("DBT_PROJECT_DIR")
    if override:
        if (Path(override) / "dbt_project.yml").exists():
            return str(Path(override).resolve())
        raise RuntimeError(f"[DBT] DBT_PROJECT_DIR set to '{override}' but dbt_project.yml not found there.")
    for base in DBT_DEFAULT_PATHS:
        candidate = Path(base) / "dbt_project.yml"
        if candidate.exists():
            return str(Path(base).resolve())
    if Path("dbt_project.yml").exists():
        return str(Path(".").resolve())
    raise RuntimeError(
        "[DBT] Could not locate 'dbt_project.yml'. "
        "Place your dbt project at /usr/local/airflow or set DBT_PROJECT_DIR."
    )

def _normalize_target_value(val) -> str | None:
    if val is None:
        return None
    if isinstance(val, dict):
        for k in ("dbt_target", "target", "value"):
            v = val.get(k)
            if isinstance(v, str) and v.strip():
                return v.strip()
        return None
    if isinstance(val, str):
        s = val.strip()
        if not s:
            return None
        if s.startswith("{") and s.endswith("}"):
            try:
                obj = json.loads(s)
                return _normalize_target_value(obj)
            except Exception:
                return None
        return s
    return str(val).strip() or None

def _resolve_target() -> str:
    env_target = os.getenv("DBT_TARGET")
    if env_target and env_target.strip():
        return env_target.strip()
    var_val = None
    try:
        var_val = Variable.get("dbt_target", deserialize_json=True)
    except Exception:
        try:
            var_val = Variable.get("dbt_target")
        except Exception:
            var_val = None
    tgt = _normalize_target_value(var_val)
    if tgt:
        return tgt
    return "ci"  # fallback—your current profile showed only 'ci' earlier

def _git_available() -> bool:
    return shutil.which("git") is not None

def _run(cmd: list[str], cwd: str, extra_env: dict | None = None):
    env = os.environ.copy()
    if extra_env:
        env.update({k: v for k, v in extra_env.items() if v is not None})
    try:
        out = subprocess.check_output(
            cmd, cwd=cwd, text=True, stderr=subprocess.STDOUT, timeout=1200, env=env
        )
        print(out.strip())
        return out
    except subprocess.CalledProcessError as e:
        print("── dbt command failed ──")
        print("Command:", " ".join(cmd))
        print("Output:\n", e.output)
        raise

def _render_profiles_yml_from_airflow_conn(profiles_dir: Path, target: str) -> Path:
    """
    Create a minimal dbt profiles.yml for Snowflake using the Airflow connection 'snowflake_default'.
    This avoids hardcoding secrets into the image. File is written to profiles_dir/profiles.yml.
    """
    conn = BaseHook.get_connection("snowflake_default")
    extras = conn.extra_dejson or {}
    user = conn.login or extras.get("user")
    password = conn.password or extras.get("password")
    account = extras.get("account")  # expected in Snowflake conn extras
    role = extras.get("role") or extras.get("extra__snowflake__role")
    warehouse = extras.get("warehouse") or extras.get("extra__snowflake__warehouse")
    database = extras.get("database") or extras.get("extra__snowflake__database")
    schema = extras.get("schema") or extras.get("extra__snowflake__schema") or "PUBLIC"
    authenticator = extras.get("authenticator")  # optional: externalbrowser / oauth / etc.

    # Basic validation
    missing = [k for k, v in {
        "user": user, "password": password, "account": account,
        "role": role, "warehouse": warehouse, "database": database, "schema": schema
    }.items() if not v]
    if missing:
        raise RuntimeError(f"[DBT] snowflake_default missing required fields: {missing}. "
                           "Fill the Airflow connection extras (account/warehouse/database/role/schema) and password.")

    profiles_dir.mkdir(parents=True, exist_ok=True)
    profiles_yml = profiles_dir / "profiles.yml"

    # Build YAML (keep it tiny; dbt 1.10 defaults are fine)
    content = {
        "stock_market_elt": {  # must match your dbt_project.yml 'profile:'
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
                    # Optional extras below
                    **({"authenticator": authenticator} if authenticator else {}),
                    "client_session_keep_alive": False,
                }
            }
        }
    }

    # Manual YAML dump (simple & dependency-free)
    def _dump_yaml(d, indent=0):
        lines = []
        for k, v in d.items():
            if isinstance(v, dict):
                lines.append("  " * indent + f"{k}:")
                lines.extend(_dump_yaml(v, indent + 1))
            else:
                if isinstance(v, bool):
                    sval = "true" if v else "false"
                else:
                    sval = str(v)
                lines.append("  " * indent + f"{k}: {sval}")
        return lines

    text = "\n".join(_dump_yaml(content)) + "\n"
    profiles_yml.write_text(text, encoding="utf-8")

    # Do NOT print the file content (contains password). Just confirm path.
    print(f"[DBT] Wrote profiles.yml to {profiles_yml} (using Airflow connection 'snowflake_default').")
    return profiles_yml

@dag(
    dag_id="dbt_build",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["dbt", "build"],
    default_args={"retries": 0},
    doc_md="""
    # dbt build
    Renders a secure dbt `profiles.yml` from Airflow's `snowflake_default`, then runs:
    `dbt debug` → `dbt deps` → (optional) `dbt source freshness` → `dbt build`,
    and uploads `manifest.json` + `run_results.json` to S3.
    """,
)
def dbt_build():

    @task
    def prepare():
        exe = _resolve_dbt_exe()
        project_dir = _resolve_project_dir()
        target = _resolve_target()
        bucket = os.getenv("BUCKET_NAME")
        if not bucket:
            raise RuntimeError("BUCKET_NAME env var is required.")
        print(f"[DBT] exe={exe}")
        print(f"[DBT] project_dir={project_dir}")
        print(f"[DBT] target(resolved)={target}")
        print(f"[DBT] threads={os.getenv('DBT_THREADS') or '(dbt default)'}")
        print(f"[S3] bucket={bucket}")
        return {
            "exe": exe,
            "project_dir": project_dir,
            "target": target,
            "bucket": bucket,
            "git_ok": _git_available(),
        }

    @task
    def render_profiles(cfg: dict):
        # Create a private profiles dir and set DBT_PROFILES_DIR for subsequent commands
        profiles_dir = Path("/tmp/dbt_profiles")
        _ = _render_profiles_yml_from_airflow_conn(profiles_dir, cfg["target"])
        print(f"[DBT] Using DBT_PROFILES_DIR={profiles_dir}")
        return {**cfg, "profiles_dir": str(profiles_dir)}

    @task
    def resume_warehouse():
        # Optional: wake the WH
        hook = SnowflakeHook(snowflake_conn_id="snowflake_default")
        try:
            wh = hook.get_first("select current_warehouse()")[0]
            if wh:
                hook.run(f"alter warehouse {wh} resume if suspended")
                print(f"[SF] Warehouse resumed (if suspended): {wh}")
        except Exception as e:
            print(f"[SF] Skipping warehouse resume: {e}")

    @task_group
    def dbt_steps(cfg: dict):

        def _env(cfg: dict) -> dict:
            return {
                "DBT_PROFILES_DIR": cfg["profiles_dir"],
                "DBT_TARGET": cfg["target"],  # helpful if you run dbt without --target elsewhere
            }

        @task
        def debug(cfg: dict):
            exe = cfg["exe"]; project_dir = cfg["project_dir"]; target = cfg["target"]
            cmd = [exe, "debug", "--target", target, "--no-use-colors"]
            _run(cmd, project_dir, extra_env=_env(cfg))

        @task
        def deps(cfg: dict):
            exe = cfg["exe"]; project_dir = cfg["project_dir"]
            if not cfg.get("git_ok", False):
                print("[DBT] Skipping `dbt deps` because `git` is not available in PATH.")
                return
            cmd = [exe, "deps"]
            _run(cmd, project_dir, extra_env=_env(cfg))

        @task
        def freshness(cfg: dict):
            exe = cfg["exe"]; project_dir = cfg["project_dir"]; target = cfg["target"]
            try:
                cmd = [exe, "source", "freshness", "--target", target, "--no-use-colors"]
                _run(cmd, project_dir, extra_env=_env(cfg))
            except subprocess.CalledProcessError:
                print("[DBT] source freshness failed (continuing). See logs above for details.")

        @task
        def build(cfg: dict):
            exe = cfg["exe"]; project_dir = cfg["project_dir"]; target = cfg["target"]
            cmd = [exe, "build", "--target", target, "--no-use-colors"]
            _run(cmd, project_dir, extra_env=_env(cfg))

        debug(cfg) >> deps(cfg) >> freshness(cfg) >> build(cfg)

    @task
    def publish_artifacts(cfg: dict):
        project_dir = cfg["project_dir"]; bucket = cfg["bucket"]
        target_dir = Path(project_dir) / "target"
        manifest = target_dir / "manifest.json"
        run_results = target_dir / "run_results.json"

        missing = [p.name for p in [manifest, run_results] if not p.exists()]
        if missing:
            print(f"[DBT] Artifacts missing (skipping upload): {missing}")
            return

        s3 = S3Hook()
        base_key = f"dbt/artifacts/{pendulum.now('UTC').format('YYYYMMDD_HHmmss')}/"
        for f in [manifest, run_results]:
            key = base_key + f.name
            s3.load_file(filename=str(f), key=key, bucket_name=bucket, replace=True)
            print(f"[S3] Uploaded s3://{bucket}/{key}")

    cfg = prepare()
    cfg2 = render_profiles(cfg)
    resume_warehouse() >> dbt_steps(cfg2) >> publish_artifacts(cfg2)

dbt_build()
