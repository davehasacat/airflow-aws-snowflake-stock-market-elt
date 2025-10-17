# =====================================================================
# Dockerfile for Airflow (Astro Runtime) + dbt (Snowflake)
# ---------------------------------------------------------------------
# Purpose:
# - Extend Astronomer’s Airflow base image
# - Add AWS CLI (v2) and dbt for Snowflake transformations
# - Prepare dbt packages and validate configuration at build time
# =====================================================================

FROM quay.io/astronomer/astro-runtime:13.2.0

# Switch to root for installing system dependencies
USER root

# ---------------------------------------------------------------------
# 🧰 Install AWS CLI v2 + git
# ---------------------------------------------------------------------
# Why:
# - AWS CLI: used by dbt and Airflow hooks (S3, Secrets Manager, etc.)
# - git: required by dbt deps for Git-based packages (e.g., dbt_utils)
# ---------------------------------------------------------------------
RUN apt-get update && apt-get install -y curl unzip git && \
    curl -sSL "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "/tmp/awscliv2.zip" && \
    unzip /tmp/awscliv2.zip -d /tmp && \
    /tmp/aws/install && \
    rm -rf /var/lib/apt/lists/* /tmp/aws /tmp/awscliv2.zip

# ---------------------------------------------------------------------
# 🧱 Create and install dbt in a dedicated virtual environment
# ---------------------------------------------------------------------
# Why:
# - Keeps dbt isolated from Airflow dependencies (prevents version conflicts)
# - Makes it easier to upgrade dbt without breaking Astronomer’s base runtime
# ---------------------------------------------------------------------
RUN python -m venv /usr/local/airflow/dbt_venv

# Install specific dbt + adapter versions (pinned for reproducibility)
RUN /usr/local/airflow/dbt_venv/bin/pip install --no-cache-dir \
    "dbt-core==1.10.4" \
    "dbt-snowflake==1.10.0"

# Add dbt virtualenv to PATH so `dbt` is callable anywhere
ENV PATH="/usr/local/airflow/dbt_venv/bin:${PATH}"

# ---------------------------------------------------------------------
# 📁 Prepare dbt project directory
# ---------------------------------------------------------------------
# Why:
# - Allows incremental Docker caching when dbt deps are reinstalled
# - Prevents rebuilds unless dbt dependency files actually change
# ---------------------------------------------------------------------
RUN mkdir -p /usr/local/airflow/dbt

# Copy dependency files first for efficient caching
COPY dbt/packages.yml /usr/local/airflow/dbt/packages.yml
COPY dbt/dbt_project.yml /usr/local/airflow/dbt/dbt_project.yml

# ---------------------------------------------------------------------
# 📦 Install dbt packages (dbt_utils, dbt_date, dbt_expectations, etc.)
# ---------------------------------------------------------------------
RUN dbt deps --project-dir /usr/local/airflow/dbt

# ---------------------------------------------------------------------
# 🧩 Provide a minimal profiles.yml for build-time validation
# ---------------------------------------------------------------------
# Why:
# - Enables `dbt parse` to succeed during build (avoids runtime secrets)
# - Should include a lightweight, non-sensitive Snowflake CI target
#   (profiles.ci.yml is safe and checked into source control)
# ---------------------------------------------------------------------
COPY dbt/profiles.ci.yml /usr/local/airflow/dbt/profiles.yml

# ---------------------------------------------------------------------
# 🗂 Copy the remainder of the dbt project
# ---------------------------------------------------------------------
# This includes models, snapshots, macros, etc.
# NOTE: Place COPY *after* dbt deps for build caching efficiency.
# ---------------------------------------------------------------------
COPY dbt /usr/local/airflow/dbt

# ---------------------------------------------------------------------
# ✅ Validate project structure at build time (non-destructive)
# ---------------------------------------------------------------------
# Why:
# - Ensures dbt syntax, models, and references are valid before runtime
# - Uses `--target ci` to align with lightweight CI testing target
# ---------------------------------------------------------------------
RUN dbt parse \
  --project-dir /usr/local/airflow/dbt \
  --profiles-dir /usr/local/airflow/dbt \
  --target ci

# Switch back to non-root Astro runtime user
USER astro

# ---------------------------------------------------------------------
# ✅ Final Notes:
# ---------------------------------------------------------------------
# - Runtime Airflow + dbt DAGs will use a dynamically rendered profiles.yml
#   from AWS Secrets Manager (Snowflake creds never baked into the image)
# - dbt_venv is available to all Airflow tasks via DBT_EXECUTABLE_PATH
# - To update dbt, modify both the pip install versions and dbt_project.yml
# ---------------------------------------------------------------------
