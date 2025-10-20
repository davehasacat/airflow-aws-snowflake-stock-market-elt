# dags/dbt/dbt_build.py
# =====================================================================
# dbt Build DAG — key-pair aware (uses conn.password as key passphrase)
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

DBT_DEFAULT_PATHS = [
    "/usr/local/airflow",
    "/usr/local/airflow/dbt",
    "/usr/local/airflow/dags/dbt",
]

ARTIFACTS_PREFIX = "dbt/artifacts"

# ────────────────────────────────────────────────────────────────────────────────
# Helpers
# ────────────────────────────────────────────────────────────────────────────────
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
        if (Path(base) / "dbt_project.yml").exists():
            return str(Path(base).resolve())
    if Path("dbt_project.yml").exists():
        return str(Path(".").resolve())
    raise RuntimeError(
        "[DBT] Could not locate 'dbt_project.yml'. "
        "Place your dbt project at /usr/local/airflow or set DBT_PROJECT_DIR."
    )

def _normalize_target_value(val) -> Optional[str]:
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
    val = os.getenv(name_env)
    if val is None:
        try:
            val = Variable.get(name_var)
        except Exception:
            val = None
    return val if (val and str(val).strip()) else default

def _get_json(name_env: str, name_var: str) -> Optional[dict]:
    raw = _get_str(name_env, name_var)
    if not raw:
        return None
    try:
        return json.loads(raw)
    except Exception:
        raise RuntimeError(f"[DBT] {name_env}/{name_var} must be valid JSON (got: {raw!r})")

def _git_available() -> bool:
    return shutil.which("git") is not None

def _run(cmd: list[str], cwd: str, extra_env: dict | None = None, timeout_sec: int = 3600):
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
# profiles.yml rendering (Snowflake) — key-pair aware
# ────────────────────────────────────────────────────────────────────────────────
def _render_profiles_yml_from_airflow_conn(profiles_dir: Path, target: str) -> Path:
    """
    Render profiles.yml using Airflow `snowflake_default`.
    - Uses key-pair auth when `extra.private_key_file` (or `private_key_path`) is set.
    - Takes the private key passphrase from **conn.password** (preferred),
      falling back to `extra.private_key_passphrase` if present.
    - Falls back to password auth only if no key is provided.
    """
    conn = BaseHook.get_connection("snowflake_default")
    extras = conn.extra_dejson or {}

    # Base fields
    user = conn.login or extras.get("user")
    account = extras.get("account") or extras.get("extra__snowflake__account")
    role = extras.get("role") or extras.get("extra__snowflake__role")
    warehouse = extras.get("warehouse") or extras.get("extra__snowflake__warehouse")
    database = extras.get("database") or extras.get("extra__snowflake__database")
    schema = extras.get("schema") or extras.get("extra__snowflake__schema") or "PUBLIC"
    keep_alive = bool(extras.get("client_session_keep_alive", False))

    # Auth options
    pk_path = extras.get("private_key_file") or extras.get("private_key_path")
    pk_pass = (conn.password or "") or extras.get("private_key_passphrase")  # conn.password holds passphrase
    password = conn.password if not pk_path else None  # only use as password when no key provided

    # Validate required
    missing_base = [k for k, v in {
        "user": user, "account": account, "role": role,
        "warehouse": warehouse, "database": database, "schema": schema
    }.items() if not v]
    if missing_base:
        raise RuntimeError(f"[DBT] snowflake_default missing fields: {missing_base}")

    # Decide auth mode
    use_keypair = bool(pk_path)
    if use_keypair:
        if not Path(pk_path).exists():
            raise RuntimeError(f"[DBT] private key file not found at '{pk_path}'. Mount it into the container.")
        print("[DBT] Using key-pair authentication (private_key_path).")
    elif password:
        print("[DBT] Using password authentication.")
    else:
        raise RuntimeError("[DBT] No password and no private_key_file/private_key_path provided.")

    # Compose output profile
    output: dict = {
        "type": "snowflake",
        "account": account,
        "user": user,
        "role": role,
        "database": database,
        "warehouse": warehouse,
        "schema": schema,
        "threads": int(os.getenv("DBT_THREADS", "4")),
        "client_session_keep_alive": keep_alive,
    }

    if use_keypair:
        output["private_key_path"] = pk_path
        if pk_pass:
            output["private_key_passphrase"] = pk_pass
    else:
        output["password"] = password  # only when not using keypair

    content = {"stock_market_elt": {"target": target, "outputs": {target: output}}}

    # Tiny YAML dump
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

    profiles_dir.mkdir(parents=True, exist_ok=True)
    profiles_yml = profiles_dir / "profiles.yml"
    profiles_yml.write_text("\n".join(_dump_yaml(content)) + "\n", encoding="utf-8")
    print(f"[DBT] Wrote profiles.yml to {profiles_yml} (keypair={use_keypair}).")
    return profiles_yml

# ────────────────────────────────────────────────────────────────────────────────
# S3 Artifacts & State
# ────────────────────────────────────────────────────────────────────────────────
def _latest_manifest_to_state_dir(bucket: str) -> Optional[Path]:
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
    state_dir = Path("/tmp/dbt_state"); state_dir.mkdir(parents=True, exist_ok=True)
    tmp_downloaded = s3.download_file(key=latest_key, bucket_name=bucket, local_path=str(state_dir))
    if tmp_downloaded and isinstance(tmp_downloaded, str):
        downloaded_path = Path(tmp_downloaded)
    else:
        from pathlib import Path as _P
        candidates = sorted(_P(state_dir).glob("airflow_tmp_*"), key=lambda p: p.stat().st_mtime, reverse=True)
        if not candidates:
            raise RuntimeError("[S3] Download did not produce a file in state_dir.")
        downloaded_path = candidates[0]
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
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["dbt", "build"],
    default_args={"retries": 0},
    doc_md="""
    # dbt build
    - Renders a secure dbt `profiles.yml` from Airflow `snowflake_default`
    - Uses **key-pair auth** when available (passphrase from **conn.password**)
    - Optional stateful builds using the latest manifest from S3
    - Runs: `dbt debug` → `dbt deps` → (optional) `dbt source freshness` → `dbt build`
    - Uploads: `manifest.json`, `run_results.json`, and `logs/dbt.log` to S3
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
        full_refresh = _get_bool("DBT_FULL_REFRESH", "dbt_full_refresh", False)
        select = _get_str("DBT_SELECT", "dbt_select")
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
        profiles_dir = Path("/tmp/dbt_profiles")
        _ = _render_profiles_yml_from_airflow_conn(profiles_dir, cfg["target"])
        print(f"[DBT] Using DBT_PROFILES_DIR={profiles_dir}")
        return {**cfg, "profiles_dir": str(profiles_dir)}

    @task
    def resume_warehouse():
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
        state_dir = None
        if cfg.get("use_state"):
            state_dir = _latest_manifest_to_state_dir(cfg["bucket"])
        return {**cfg, "state_dir": str(state_dir) if state_dir else None}

    @task_group
    def dbt_steps(cfg: dict):

        def _env(cfg: dict) -> dict:
            return {"DBT_PROFILES_DIR": cfg["profiles_dir"], "DBT_TARGET": cfg["target"]}

        def _base_cmd(cfg: dict, subcmd: list[str]) -> list[str]:
            cmd = [
                cfg["exe"], *subcmd, "--target", cfg["target"],
                "--profiles-dir", cfg["profiles_dir"], "--no-use-colors"
            ]
            if cfg.get("select"):
                cmd += ["--select", cfg["select"]]
            if cfg.get("exclude"):
                cmd += ["--exclude", cfg["exclude"]]
            if cfg.get("state_dir"):
                cmd += ["--state", cfg["state_dir"]]
            if cfg.get("vars_json"):
                cmd += ["--vars", json.dumps(cfg["vars_json"])]
            if cfg.get("full_refresh") and subcmd[0] in ("run", "build"):
                cmd.append("--full-refresh")
            return cmd

        @task
        def debug(cfg: dict):
            _run(_base_cmd(cfg, ["debug"]), cfg["project_dir"], extra_env=_env(cfg), timeout_sec=600)

        @task
        def deps(cfg: dict):
            if not cfg.get("git_ok", False):
                print("[DBT] Skipping `dbt deps` because `git` is not available in PATH.")
                return
            _run([cfg["exe"], "deps", "--profiles-dir", cfg["profiles_dir"]],
                 cfg["project_dir"], extra_env=_env(cfg), timeout_sec=1200)

        @task
        def freshness(cfg: dict):
            try:
                _run(_base_cmd(cfg, ["source", "freshness"]),
                     cfg["project_dir"], extra_env=_env(cfg), timeout_sec=1800)
            except subprocess.CalledProcessError:
                print("[DBT] source freshness failed (continuing). See logs above for details.")

        @task
        def build(cfg: dict):
            _run(_base_cmd(cfg, ["build"]), cfg["project_dir"], extra_env=_env(cfg), timeout_sec=7200)

        debug(cfg) >> deps(cfg) >> freshness(cfg) >> build(cfg)

    @task
    def publish_artifacts(cfg: dict):
        project_dir = cfg["project_dir"]; bucket = cfg["bucket"]
        ts_folder = pendulum.now("UTC").format("YYYYMMDD_HHmmss")
        base_key = f"{ARTIFACTS_PREFIX}/{ts_folder}/"
        target_dir = Path(project_dir) / "target"
        logs_dir = Path(project_dir) / "logs"
        files = [target_dir / "manifest.json", target_dir / "run_results.json", logs_dir / "dbt.log"]
        if not any(p.exists() for p in files):
            print("[DBT] No artifacts found; skipping upload.")
            return
        s3 = S3Hook()
        for f in files:
            if f.exists():
                key = base_key + f.name
                s3.load_file(filename=str(f), key=key, bucket_name=bucket, replace=True)
                print(f"[S3] Uploaded s3://{bucket}/{key}")

    cfg = prepare()
    cfg2 = render_profiles(cfg)
    cfg3 = prepare_state(cfg2)
    resume_warehouse() >> dbt_steps(cfg3) >> publish_artifacts(cfg3)

dbt_build()
