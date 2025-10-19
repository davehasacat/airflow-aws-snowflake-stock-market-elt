# =====================================================================
# Dockerfile for Airflow (Astro Runtime) + dbt (Snowflake)
# ---------------------------------------------------------------------
# Goals:
# - Extend Astronomer’s Airflow base image
# - Install AWS CLI v2 and dbt (Snowflake adapter) in an isolated venv
# - Pre-install dbt packages and validate project syntax at build time
# - Ship a runtime bootstrap that renders dbt profiles.yml from
#   AWS Secrets Manager (airflow/connections/snowflake_default)
# =====================================================================

FROM quay.io/astronomer/astro-runtime:13.2.0

# Switch to root to install system dependencies
USER root

# ---------------------------------------------------------------------
# 🧰 Install AWS CLI v2 + git
# ---------------------------------------------------------------------
RUN apt-get update && apt-get install -y --no-install-recommends \
      curl unzip git \
 && curl -sSL "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "/tmp/awscliv2.zip" \
 && unzip /tmp/awscliv2.zip -d /tmp \
 && /tmp/aws/install \
 && rm -rf /var/lib/apt/lists/* /tmp/aws /tmp/awscliv2.zip

# ---------------------------------------------------------------------
# 🧱 Create and install dbt in a dedicated virtual environment
# ---------------------------------------------------------------------
RUN python -m venv /usr/local/airflow/dbt_venv
RUN /usr/local/airflow/dbt_venv/bin/pip install --no-cache-dir \
      "dbt-core==1.10.4" \
      "dbt-snowflake==1.10.0"

# Make the dbt venv available on PATH
ENV PATH="/usr/local/airflow/dbt_venv/bin:${PATH}"

# ---------------------------------------------------------------------
# 📁 Prepare dbt project directory & install packages
# ---------------------------------------------------------------------
RUN mkdir -p /usr/local/airflow/dbt

# Copy dependency files first for better Docker layer caching
COPY dbt/packages.yml      /usr/local/airflow/dbt/packages.yml
COPY dbt/dbt_project.yml   /usr/local/airflow/dbt/dbt_project.yml

# Install dbt packages (dbt_utils, dbt_date, etc.)
RUN dbt deps --project-dir /usr/local/airflow/dbt

# Copy the remainder of the dbt project (models, snapshots, macros, etc.)
COPY dbt /usr/local/airflow/dbt

# ---------------------------------------------------------------------
# ✅ Build-time validation (non-destructive)
# ---------------------------------------------------------------------
# Create a minimal, dummy profiles.yml for build-time `dbt parse` only.
RUN printf '%s\n' \
'stock_market_elt:' \
'  target: dev' \
'  outputs:' \
'    dev:' \
'      type: snowflake' \
'      account: DUMMY_ACCOUNT' \
'      user: DUMMY_USER' \
'      role: DUMMY_ROLE' \
'      database: DUMMY_DB' \
'      warehouse: DUMMY_WH' \
'      schema: PUBLIC' \
'      private_key_path: /tmp/dummy_key.p8' \
'      private_key_passphrase: "__build_dummy__"' \
'      client_session_keep_alive: true' \
> /usr/local/airflow/dbt/profiles.yml

# Set a dummy passphrase so templating never fails during build.
ENV SNOWFLAKE_PASSPHRASE="__build_dummy__"

# dbt parse checks model syntax/refs; it does NOT contact Snowflake.
RUN dbt parse \
    --project-dir /usr/local/airflow/dbt \
    --profiles-dir /usr/local/airflow/dbt

# ---------------------------------------------------------------------
# 🚀 Runtime bootstrap: render dbt profiles.yml from AWS Secrets Manager
# ---------------------------------------------------------------------
# Expects docker/dbt_bootstrap.sh in your repo.
COPY docker/dbt_bootstrap.sh /usr/local/bin/dbt_bootstrap.sh
RUN chmod +x /usr/local/bin/dbt_bootstrap.sh

# Switch back to non-root Astro runtime user
USER astro

# ---------------------------------------------------------------------
# Notes:
# - At container start, compose should run:
#     command: ["bash","-lc","dbt_bootstrap.sh && exec /entrypoint"]
#   so dbt profiles are rendered from AWS SM before Airflow starts.
# - Private key(s) must be mounted at runtime, e.g.:
#     ./keys -> /usr/local/airflow/keys:ro
# - Secrets are never baked into the image; they are fetched at runtime.
# ---------------------------------------------------------------------
