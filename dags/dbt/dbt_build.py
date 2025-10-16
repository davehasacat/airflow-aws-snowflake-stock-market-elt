# dags/dbt/dbt_build.py
from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path

import pendulum
from airflow.decorators import dag, task, task_group
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

# Common locations inside the Astronomer/Airflow container where your dbt project may live
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
    # Prefer an explicit env override if you set one
    override = os.getenv("DBT_PROJECT_DIR")
    if override:
        if (Path(override) / "dbt_project.yml").exists():
            return str(Path(override).resolve())
        raise RuntimeError(f"[DBT] DBT_PROJECT_DIR set to '{override}' but dbt_project.yml not found there.")

    # Otherwise, find a dbt_project.yml in common spots
    for base in DBT_DEFAULT_PATHS:
        candidate = Path(base) / "dbt_project.yml"
        if candidate.exists():
            return str(Path(base).resolve())
    # fallback to CWD if it contains dbt_project.yml
    if Path("dbt_project.yml").exists():
        return str(Path(".").resolve())
    raise RuntimeError(
        "[DBT] Could not locate 'dbt_project.yml'. "
        "Place your dbt project at /usr/local/airflow (recommended) or set DBT_PROJECT_DIR."
    )

def _run(cmd: list[str], cwd: str):
    # 20-minute cap should be plenty for moderate projects; tune as needed
    out = subprocess.check_output(cmd, cwd=cwd, text=True, stderr=subprocess.STDOUT, timeout=1200)
    print(out.strip())
    return out

@dag(
    dag_id="dbt_build",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    schedule=None,            # run on-demand
    catchup=False,
    max_active_runs=1,
    tags=["dbt", "build"],
    default_args={"retries": 0},
    doc_md="""
    # dbt build
    Runs `dbt debug` → `dbt deps` → (optional) `dbt source freshness` → `dbt build`,
    then uploads artifacts (`manifest.json`, `run_results.json`) to S3.
    """,
)
def dbt_build():

    @task
    def prepare():
        exe = _resolve_dbt_exe()
        project_dir = _resolve_project_dir()
        target = Variable.get("dbt_target", default_var="dev")
        bucket = os.getenv("BUCKET_NAME")
        if not bucket:
            raise RuntimeError("BUCKET_NAME env var is required.")
        print(f"[DBT] exe={exe}")
        print(f"[DBT] project_dir={project_dir}")
        print(f"[DBT] target={target}")
        print(f"[DBT] threads={os.getenv('DBT_THREADS') or '(dbt default)'}")
        print(f"[S3] bucket={bucket}")
        return {"exe": exe, "project_dir": project_dir, "target": target, "bucket": bucket}

    @task
    def resume_warehouse():
        # Optional but helpful: ensure WH is awake before dbt to avoid cold-start latency
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
        exe = cfg["exe"]
        project_dir = cfg["project_dir"]
        target = cfg["target"]

        @task
        def debug():
            cmd = [exe, "debug", "--target", target, "--no-use-colors", "--quiet"]
            _run(cmd, project_dir)

        @task
        def deps():
            cmd = [exe, "deps"]
            _run(cmd, project_dir)

        @task
        def freshness():
            # Non-fatal: continue even if freshness not configured for sources yet
            try:
                cmd = [exe, "source", "freshness", "--target", target, "--no-use-colors"]
                _run(cmd, project_dir)
            except subprocess.CalledProcessError as e:
                print("[DBT] source freshness failed (continuing). Output:\n" + e.output)

        @task
        def build():
            # dbt will read DBT_THREADS from env automatically if set
            # You can add selection, e.g., ['--select', 'tag:core']
            cmd = [exe, "build", "--target", target, "--no-use-colors"]
            _run(cmd, project_dir)

        debug() >> deps() >> freshness() >> build()

    @task
    def publish_artifacts(cfg: dict):
        project_dir = cfg["project_dir"]
        bucket = cfg["bucket"]
        target_dir = Path(project_dir) / "target"
        manifest = target_dir / "manifest.json"
        run_results = target_dir / "run_results.json"

        missing = [p.name for p in [manifest, run_results] if not p.exists()]
        if missing:
            print(f"[DBT] Artifacts missing (skipping upload): {missing}")
            return

        s3 = S3Hook()  # uses your AWS creds (same profile env as the rest of the stack)
        base_key = f"dbt/artifacts/{pendulum.now('UTC').format('YYYYMMDD_HHmmss')}/"
        for f in [manifest, run_results]:
            key = base_key + f.name
            s3.load_file(
                filename=str(f),
                key=key,
                bucket_name=bucket,
                replace=True,
            )
            print(f"[S3] Uploaded s3://{bucket}/{key}")

    cfg = prepare()
    resume_warehouse() >> dbt_steps(cfg) >> publish_artifacts(cfg)

dbt_build()
