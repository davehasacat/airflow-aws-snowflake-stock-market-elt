# 🧠 Stock Market ELT Pipeline

> **This project is in pre-release (v0.1.0)** — full end-to-end ingestion, transformation, and mart creation are live and queryable in Snowflake.  
> **Version v1.0.0** will include the Plotly Dash analytics dashboard.

---

## 📈 Overview

The **Stock Market ELT Pipeline** is a cloud-native data engineering project that ingests, stores, transforms, and models historical and daily market data from **Polygon.io** (stocks and options, including Greeks).  
It demonstrates a **production-grade ELT stack** using **Airflow**, **AWS**, **Snowflake**, and **dbt Core**, all containerized and orchestrated via **Astronomer**.

This project is designed for **data engineering skill development** and to mirror an enterprise-grade data platform—secure, modular, and fully automated.

---

## ☁️ Cloud Architecture

**Core Stack:**

| Layer | Tool | Purpose |
|-------|------|----------|
| **Orchestration** | Apache Airflow (Astronomer) | Manages ingestion, load, and dbt runs |
| **Storage (Data Lake)** | AWS S3 | Raw JSON/CSV files stored by ticker/date |
| **Secrets Management** | AWS Secrets Manager | Securely stores Airflow connections & variables |
| **Data Warehouse** | Snowflake | Centralized analytical warehouse |
| **Transformation** | dbt Core (run via Astro CLI) | Models data into staged, intermediate, and mart layers |
| **Dashboard (upcoming)** | Plotly Dash | Interactive visual analytics layer |

**Data Flow:**

``` txt
Polygon API → Airflow → S3 (raw) → Snowflake (load) → dbt (models) → Dashboard (v1.0.0)
```

**Key Integrations:**

- Airflow retrieves API keys and credentials from **AWS Secrets Manager**
- Ingestion DAGs use **HTTP retries + API Pools** for rate limit control
- Snowflake external stages point to **S3** for raw data ingestion
- dbt runs are **incremental**, ensuring efficient daily refreshes
- Future dashboard (v1.0.0) will query Snowflake marts directly

---

## 🧮 Data Modeling (dbt)

The project follows a **layered dbt structure** aligned with best practices:

``` txt
raw → staging → intermediate → marts
```

| Layer | Example Model | Description |
|--------|----------------|-------------|
| **Staging (`stg_`)** | `stg_polygon__stocks`, `stg_polygon__options` | Typed + cleaned data from Snowflake landing tables |
| **Intermediate (`int_`)** | `int_polygon__options_stocks_joined` | Joins stocks and options (including Greeks) into a unified dataset |
| **Mart (`mart_`)** | `mart_polygon__options_stocks_joined` | Final queryable dataset optimized for dashboards |

**Incremental Models:**

- All dbt models downstream of staging are **incremental**, leveraging `is_incremental()` filters and unique keys.
- Enables fast re-runs and minimal recomputation during daily refreshes.

---

## ⚙️ Current Features (v0.1.0)

- ✅ Airflow running locally with Astronomer (Docker Desktop)
- ✅ AWS Secrets Manager integration for credentials
- ✅ Ingest DAGs for Stocks and Options (daily + backfill)
- ✅ S3 data structured as `raw/stocks/` and `raw/options/` (gzip JSON)
- ✅ Snowflake tables for both datasets loaded via Airflow Load DAGs
- ✅ dbt Core connected via `profiles.yml` (auto-generated)
- ✅ One dbt mart model (`mart_polygon__options_stocks_joined.sql`) fully queryable
- ✅ Sample queries returning joined Stocks + Options (Greeks) data

---

## 🧰 Tech Stack Summary

| Category | Tool / Service |
|-----------|----------------|
| **Container Runtime** | Docker Desktop + Astro CLI |
| **Scheduler** | Apache Airflow |
| **Storage** | AWS S3 |
| **Warehouse** | Snowflake |
| **Modeling** | dbt Core (v1.10.x) |
| **Secrets / IAM** | AWS Secrets Manager + IAM Roles |
| **Dashboard (planned)** | Plotly Dash |
| **Languages** | Python, SQL, YAML |
| **Region** | AWS `us-east-2` |

---

## 🧩 Example Workflow

1. **Airflow DAGs** fetch daily stock + options data from Polygon.io  
2. Files are compressed and stored in S3 (raw layer)  
3. Load DAGs move data into Snowflake typed tables  
4. dbt transforms data incrementally into marts  
5. Analysts query joined Stocks + Options + Greeks data in Snowflake  
6. *(Upcoming v1.0.0)* Plotly Dash visualizes mart results interactively  

---

## 🧠 Pre-Release Roadmap

| Version | Milestone | Key Deliverables |
|----------|------------|------------------|
| **v0.1.0** | Pre-Release | Joined mart model live in Snowflake (stocks + options) |
| **v0.5.0** | Alpha | Airflow + dbt fully automated daily refresh |
| **v0.9.0** | Beta | Plotly Dash prototype connected to Snowflake |
| **v1.0.0** | Stable | Dashboard finalized + full documentation |

---

## 📂 Repository Structure

``` txt
.
├── dags/
│   ├── polygon/
│   │   ├── stocks/
│   │   └── options/
│   └── utils/
├── dbt/
│   ├── models/
│   │   ├── staging/
│   │   ├── intermediate/
│   │   └── marts/
│   ├── snapshots/
│   ├── macros/
│   └── profiles.yml (auto-generated)
├── snowflake/
│   ├── grants.sql
│   └── setup.sql
├── docs/
│   ├── aws-secrets-manager-setup.md
│   ├── runbook.md
│   └── architecture-diagram.png
└── docker-compose.override.yml
```

---

## 🧭 Getting Started (Local Development)

1. **Start Airflow with Astronomer**

   ```bash
   astro dev start
   ```

2. **Verify AWS & Snowflake Connections**
   - AWS credentials mounted under `${USERPROFILE}/.aws`
   - Secrets fetched from AWS Secrets Manager

3. **Run dbt Commands**

   ```bash
   astro dev run dbt debug
   astro dev run dbt run
   ```

4. **Inspect Models**
   - Query Snowflake `STOCKS_ELT_DB.PUBLIC.MART_POLYGON__OPTIONS_STOCKS_JOINED`
   - Confirm incremental data build success

---

## 🧾 License

MIT License © 2025

---

### 🧩 Notes

This project emphasizes:

- Secure, secrets-managed Airflow integration  
- Incremental dbt architecture  
- Modular ELT design for scalability and low maintenance  
- End-to-end reproducibility using containerized components  
