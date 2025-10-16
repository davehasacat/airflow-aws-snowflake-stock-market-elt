FROM quay.io/astronomer/astro-runtime:13.2.0

USER root

# ------------ AWS CLI v2 ------------
RUN apt-get update && apt-get install -y curl unzip && \
    curl -sSL "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "/tmp/awscliv2.zip" && \
    unzip /tmp/awscliv2.zip -d /tmp && \
    /tmp/aws/install && \
    rm -rf /var/lib/apt/lists/* /tmp/aws /tmp/awscliv2.zip

# ------------ dbt (Snowflake) ------------
# Create a virtual environment for dbt at a stable path
RUN python -m venv /usr/local/airflow/dbt_venv

# Pin to a combo that is present on the Astronomer index and resolves cleanly
# - dbt-core 1.10.4
# - dbt-snowflake 1.10.0
RUN /usr/local/airflow/dbt_venv/bin/pip install --no-cache-dir \
    "dbt-core==1.10.4" \
    "dbt-snowflake==1.10.0"

# Put dbt on PATH for convenience during build/runtime
ENV PATH="/usr/local/airflow/dbt_venv/bin:${PATH}"

# Create the target dbt directory
RUN mkdir -p /usr/local/airflow/dbt

# Copy only dependency files first for better caching
COPY dbt/packages.yml /usr/local/airflow/dbt/packages.yml
COPY dbt/dbt_project.yml /usr/local/airflow/dbt/dbt_project.yml

# IMPORTANT: ensure packages.yml pins dbt_date to a version compatible with dbt-core 1.10.4
# packages.yml should include:
#   - package: godatadriven/dbt_date
#     version: "0.14.2"

# Clean stale lock (if any) and install dbt packages
RUN rm -f /usr/local/airflow/dbt/packages.lock && \
    dbt deps --project-dir /usr/local/airflow/dbt


# Copy the rest of the dbt project
COPY dbt /usr/local/airflow/dbt

# Keep these envs so your DAG code can find a manifest later if you choose to generate one at runtime
ENV DBT_TARGET_PATH=/usr/local/airflow/dbt_target
ENV DBT_MANIFEST_PATH=${DBT_TARGET_PATH}/manifest.json

# Minimal profiles.yml just for convenience if you run dbt manually in the container (no parse here)
RUN mkdir -p /home/astro/.dbt /usr/local/airflow/dbt_target && \
    cat > /home/astro/.dbt/profiles.yml <<'YAML' && \
    chown -R astro:astro /home/astro/.dbt /usr/local/airflow/dbt_target /usr/local/airflow/dbt
stock_market_elt:
  target: ci
  outputs:
    ci:
      type: snowflake
      account: "{{ env_var('SNOWFLAKE_ACCOUNT', 'dummy') }}"
      user: "{{ env_var('SNOWFLAKE_USER', 'dummy') }}"
      password: "{{ env_var('SNOWFLAKE_PASSWORD', 'dummy') }}"
      role: "{{ env_var('SNOWFLAKE_ROLE', 'DUMMY_ROLE') }}"
      warehouse: "{{ env_var('SNOWFLAKE_WAREHOUSE', 'DUMMY_WH') }}"
      database: "{{ env_var('SNOWFLAKE_DATABASE', 'DUMMY_DB') }}"
      schema: "{{ env_var('SNOWFLAKE_SCHEMA', 'PUBLIC') }}"
      threads: 1
      client_session_keep_alive: false
YAML

USER astro
