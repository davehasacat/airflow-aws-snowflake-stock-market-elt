FROM quay.io/astronomer/astro-runtime:13.2.0

USER root

# ------------ AWS CLI v2 ------------
RUN apt-get update && apt-get install -y curl unzip git && \
    curl -sSL "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "/tmp/awscliv2.zip" && \
    unzip /tmp/awscliv2.zip -d /tmp && \
    /tmp/aws/install && \
    rm -rf /var/lib/apt/lists/* /tmp/aws /tmp/awscliv2.zip

# ------------ dbt (Snowflake) ------------
# Create a virtual environment for dbt at a stable path
RUN python -m venv /usr/local/airflow/dbt_venv

# Pin to a combo that is present on the Astronomer index and resolves cleanly
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

# Install dbt packages
RUN dbt deps --project-dir /usr/local/airflow/dbt

# NEW: provide a build-time profiles.yml for parse
COPY dbt/profiles.ci.yml /usr/local/airflow/dbt/profiles.yml

# Copy the rest of the dbt project
COPY dbt /usr/local/airflow/dbt

# Parse (explicit target to avoid surprises)
RUN dbt parse \
  --project-dir /usr/local/airflow/dbt \
  --profiles-dir /usr/local/airflow/dbt \
  --target ci


USER astro
