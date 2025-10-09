FROM quay.io/astronomer/astro-runtime:13.2.0

USER root

# Create a virtual environment for dbt.
RUN python -m venv dbt_venv

# Install dbt and the Snowflake adapter inside the virtual environment.
RUN /usr/local/airflow/dbt_venv/bin/pip install --no-cache-dir dbt-snowflake==1.10.0

# Create the target dbt directory
RUN mkdir -p /usr/local/airflow/dbt

# Copy only the files needed to install dependencies
COPY dbt/packages.yml /usr/local/airflow/dbt/packages.yml
COPY dbt/dbt_project.yml /usr/local/airflow/dbt/dbt_project.yml

# Install dbt packages.
RUN /usr/local/airflow/dbt_venv/bin/dbt deps --project-dir /usr/local/airflow/dbt

# Copy the rest of the dbt project files
COPY dbt /usr/local/airflow/dbt

# Parse the project to generate manifest.json without a database connection.
RUN /usr/local/airflow/dbt_venv/bin/dbt parse \
    --project-dir /usr/local/airflow/dbt \
    --profiles-dir /usr/local/airflow/dbt \
    --target ci

USER astro
