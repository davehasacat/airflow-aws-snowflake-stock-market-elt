# WIP - Snowflake/Airflow Stock Market ELT Pipeline

## Table of Contents

* [Features](#features)
* [Tech Stack](#tech-stack)
* [Pipeline Architecture](#pipeline-architecture)
* [Demonstration](#demonstration)
* [Getting Started](#getting-started)
* [Future Work](#future-work)
* [Documentation](#documentation)

## Features

* **Data-Driven, Decoupled DAGs**: The Airflow DAGs are fully decoupled and communicate through Airflow Datasets, creating a resilient, event-driven workflow.
* **Incremental Loading Strategy**: The data loading pattern uses an incremental approach to preserve historical data and significantly improve performance by only processing new or updated records on each run.
* **Historical Data Tracking with dbt Snapshots**: Leverages dbt snapshots to track changes to the raw stock data over time, creating a complete historical record of every change.
* **Robust Data Transformations**: The project uses dbt Core to create a final analytics layer with key financial indicators (e.g., moving averages, volatility metrics) that directly feed into the back-testing of trading strategies.
* **Data Quality Monitoring**: A dedicated tab in the dashboard visualizes the results of `dbt` data quality tests, providing transparency into the health of the data pipeline.

## Tech Stack

* **Orchestration**: Apache Airflow
* **Data Ingestion**:
* **Data Lake**:
* **Data Warehouse**:
* **Transformation**: dbt Core
* **Dashboarding**: Plotly Dash
* **Containerization**: Docker
* **Local Development**: Astro CLI

## Pipeline Architecture

The ELT process is orchestrated by three modular and data-driven Airflow DAGs that form a seamless, automated workflow. The architecture is designed for high throughput and scalability, capable of ingesting and processing data for thousands of stock tickers efficiently.

The DAGs are fully decoupled and communicate through **Airflow Datasets**, which are URIs that represent a piece of data. This creates a more resilient, event-driven workflow.

1. **`stocks_polygon_ingest`**: This DAG fetches a complete list of all available stock tickers from the Polygon.io API. It then splits the tickers into small, manageable batches and dynamically creates parallel tasks to ingest the daily OHLCV data for each ticker, landing the raw JSON files in Minio object storage. Upon completion, it writes a list of all created file keys to a manifest file and **produces to an S3 Dataset** (`s3://test/manifests`).

2. **`stocks_polygon_load`**: This DAG is scheduled to run only when the S3 manifest Dataset is updated. It reads the list of newly created JSON files from the manifest and, using a similar batching strategy, loads the data in parallel into a raw table in the Postgres data warehouse. This ensures that the data loading process is just as scalable as the ingestion. When the load is successful, it produces to a Postgres Dataset (`postgres_dwh://public/source_polygon_stock_bars_daily`).

3. **`dbt_build`**: When the `load` DAG successfully updates the raw table, it produces the corresponding Dataset that triggers the final `build` DAG. This DAG runs `dbt build` to execute all dbt models, which transforms the raw data into:
    * A clean, casted staging model (`stg_polygon__stock_bars_casted`).
    * An enriched intermediate model with technical indicators (`int_polygon__stock_bars_enriched`).
    * A final, analytics-ready facts table (`fct_polygon__stock_bars_performance`).

    It also runs data quality tests to ensure the integrity of the transformed data.

## Demonstration

### Airflow Orchestration

### Interactive Dashboard

## Getting Started

### Prerequisites

### Running the Project

## Future Work

## Documentation

For more detailed information on the tools and technologies used in this project, please refer to their official documentation:

* **[Apache Airflow Documentation](https://airflow.apache.org/docs/)**
* **[Astro CLI Documentation](https://www.astronomer.io/docs/astro/cli/overview)**
* **[dbt Documentation](https://docs.getdbt.com/)**
* **[Docker Documentation](https://docs.docker.com/)**
* **[Plotly Dash Documentation](https://dash.plotly.com/)**
