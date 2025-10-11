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
* **Data Ingestion**: Python, Requests, S3
* **Data Lake**: AWS S3
* **Data Warehouse**: Snowflake
* **Transformation**: dbt Core
* **Dashboarding**: Plotly Dash
* **Containerization**: Docker
* **Local Development**: Astro CLI

## Pipeline Architecture

The ELT process is orchestrated by a series of modular and data-driven Airflow DAGs that form a seamless, automated workflow. The architecture is designed for high throughput and scalability, capable of ingesting and processing data for thousands of stock tickers and options contracts efficiently.

The DAGs are fully decoupled and communicate through **Airflow Datasets**, which are URIs that represent a piece of data. This creates a more resilient, event-driven workflow.

1. **Ingestion DAGs** (`polygon_stocks_ingest_daily`, `polygon_options_ingest_daily`, `polygon_stocks_ingest_backfill`, `polygon_options_ingest_backfill`): These DAGs fetch daily and historical stock and options data from the Polygon.io API. They dynamically create parallel tasks to ingest the data, landing the raw JSON files in an S3 bucket. Upon completion, they write a list of all created file keys to a manifest file and **produce to an S3 Dataset**.

2. **Loading DAGs** (`polygon_stocks_load`, `polygon_options_load`): These DAGs are scheduled to run only when the S3 manifest Dataset is updated. They read the list of newly created JSON files from the manifest and, using a similar batching strategy, load the data in parallel into a raw table in the Snowflake data warehouse. This ensures that the data loading process is just as scalable as the ingestion. When the load is successful, they produce to a Snowflake Dataset.

3. **Transformation DAG** (`dbt_build`): When a `load` DAG successfully updates a raw table, it produces the corresponding Dataset that triggers the `dbt_build` DAG. This DAG runs `dbt build` to execute all dbt models, which transforms the raw data into:

    Clean, casted staging models (`stg_polygon__stock_bars_casted`, `stg_polygon__options_bars_casted`).

    Enriched intermediate models with technical indicators and other calculated metrics (`int_polygon__stock_bars_enriched`, `int_polygon__options_bars_enriched`, `int_polygon__stocks_options_joined`).

    A final, analytics-ready facts table (`fct_polygon__stock_bars_performance`).

4. **Monitoring DAG** (`dbt_test`): This DAG runs `dbt test` and then parses the `run_results.json` artifact to load the results into a table in the data warehouse for monitoring via the dashboard.

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
* **[Snowflake Documentation](https://docs.snowflake.com/)**
