# =====================================================================
# Dockerfile for Airflow (Astro Runtime) + dbt (Snowflake)
# ---------------------------------------------------------------------
# - Extends Astronomer’s Airflow base image
# - Installs AWS CLI v2 and dbt (Snowflake) in an isolated venv
# - Pre-installs dbt packages and validates project at build time
# - Wraps entrypoint to run dbt bootstrap once on container start
#   (renders profiles.yml from AWS Secrets Manager)
# =====================================================================

FROM quay.io/astronomer/astro-runtime:13.2.0

# Use root for system-level installs
USER root

# ---------------------------------------------------------------------
# 🧰 System deps: AWS CLI v2 + git
# ---------------------------------------------------------------------
RUN apt-get update && apt-get install -y --no-install-recommends \
      curl unzip git \
 && curl -sSL "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "/tmp/awscliv2.zip" \
 && unzip /tmp/awscliv2.zip -d /tmp \
 && /tmp/aws/install \
 && rm -rf /var/lib/apt/lists/* /tmp/aws /tmp/awscliv2.zip

# ---------------------------------------------------------------------
# 🧱 dbt in a dedicated virtualenv (isolated from Airflow deps)
# ---------------------------------------------------------------------
RUN python -m venv /usr/local/airflow/dbt_venv
RUN /usr/local/airflow/dbt_venv/bin/pip install --no-cache-dir \
      "dbt-core==1.10.4" \
      "dbt-snowflake==1.10.0" \
      "PyYAML>=6.0"

# Put dbt venv on PATH and guarantee a stable binary path
ENV PATH="/usr/local/airflow/dbt_venv/bin:${PATH}"
RUN ln -sf /usr/local/airflow/dbt_venv/bin/dbt /usr/local/bin/dbt \
 && /usr/local/airflow/dbt_venv/bin/dbt --version

# ---------------------------------------------------------------------
# 📁 Prepare dbt project & install packages
# ---------------------------------------------------------------------
RUN mkdir -p /usr/local/airflow/dbt

# Copy dependency files first for better layer caching
COPY dbt/packages.yml    /usr/local/airflow/dbt/packages.yml
COPY dbt/dbt_project.yml /usr/local/airflow/dbt/dbt_project.yml

# Install dbt packages declared in packages.yml
RUN dbt deps --project-dir /usr/local/airflow/dbt

# Copy the rest of the dbt project (models, macros, snapshots, etc.)
COPY dbt /usr/local/airflow/dbt

# ---------------------------------------------------------------------
# ✅ Build-time validation (non-destructive)
#    Create a dummy profiles.yml so `dbt parse` can validate syntax.
#    Runtime bootstrap will overwrite with real credentials.
# ---------------------------------------------------------------------
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

ENV SNOWFLAKE_PASSPHRASE="__build_dummy__"
RUN dbt parse \
    --project-dir /usr/local/airflow/dbt \
    --profiles-dir /usr/local/airflow/dbt

# ---------------------------------------------------------------------
# 🚀 Bootstrap scripts: render profiles.yml from AWS Secrets Manager
#    and entrypoint wrapper to run bootstrap once before Airflow.
#    Normalize Windows line endings to avoid bash\r errors.
# ---------------------------------------------------------------------
COPY docker/dbt_bootstrap.sh   /usr/local/bin/dbt_bootstrap.sh
COPY docker/with_bootstrap.sh  /usr/local/bin/with_bootstrap.sh
RUN sed -i 's/\r$//' /usr/local/bin/dbt_bootstrap.sh /usr/local/bin/with_bootstrap.sh \
 && chmod +x /usr/local/bin/dbt_bootstrap.sh /usr/local/bin/with_bootstrap.sh

# Run containers as the non-root Astro user
USER astro

# Replace the stock entrypoint with our wrapper; it will exec /entrypoint "$@"
ENTRYPOINT ["/usr/local/bin/with_bootstrap.sh"]

# ---------------------------------------------------------------------
# Notes:
# - No secrets are baked into the image; bootstrap reads from AWS SM at runtime.
# - Mounts expected at runtime (via compose override):
#     ./dbt  -> /usr/local/airflow/dbt
#     ./keys -> /usr/local/airflow/keys:ro   (private key used by Snowflake)
#     ~/.aws -> /home/astro/.aws:ro          (for AWS Secrets Manager access)
# - Configure via .env:
#     DBT_PROFILE_NAME, DBT_PROFILES_DIR, DBT_PROJECT_DIR,
#     SNOWFLAKE_CONN_SECRET_NAME, DBT_BOOTSTRAP_VALIDATE,
#     DBT_BOOTSTRAP_ENABLED, DBT_BOOTSTRAP_STRICT
# ---------------------------------------------------------------------
