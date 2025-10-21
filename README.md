# Airflow‑AWS‑Snowflake Stock Market ELT

## Overview

This repository hosts the ELT (Extract‑Load‑Transform) pipeline for stock‑market data using Apache Airflow, dbt and Snowflake, built to run on AWS infrastructure.  
The project is structured in **phases**:

- Phase 1 (v0.1.0): Local development and full end‑to‑end pipeline on Docker/Astro.  
- Phase 2 (v1.0.0): Fully cloud operational – infrastructure deployed and orchestrated in AWS (and Snowflake in production mode) with no local dependencies.  
- Phase 3 (v2.0.0): Analytics dashboard layer (e.g., Plotly Dash) on top of the Snowflake‑data‑mart.

## 🧩 Project Versioning

| Version     | Status            | Goal / Acceptance Criteria                                                                 | Key Components                                                                                           |
|-------------|-------------------|--------------------------------------------------------------------------------------------|----------------------------------------------------------------------------------------------------------|
| **v0.1.0 — MVP (Local)** | ✅ In progress     | Pipeline runs full end‑to‑end **locally** (Docker Desktop / Astro) with data flowing: Polygon → S3 → Snowflake → dbt models → marts. | Airflow (local), AWS S3 (dev), Snowflake (dev), dbt Core, Secrets Manager, IAM roles/policies            |
| **v1.0.0 — Fully Cloud Operational** | ⏳ Next milestone | Pipeline runs **fully in the cloud** with no local dependencies. Infrastructure is deployed on AWS/Snowflake in production‑mode, with CI/CD, logging/monitoring, cost/ops readiness. | Managed Airflow (MWAA or Astro Cloud), AWS S3 (lifecycle policies), Snowflake (prod), dbt Cloud or containerized dbt, observability/logging, IAM, network/security |
| **v2.0.0 — Analytics Dashboard (Plotly Dash)** | 🔜 Future        | Adds analytics/dashboard layer. A Plotly Dash (or Streamlit) app connects to the Snowflake marts and is hosted on AWS (App Runner or ECS) for end‑user consumption. | Dash app, Snowflake connector, authentication/authorization, deployment pipeline, optional API gateway     |

## Architecture

*(You may update this section with architecture diagrams, AWS service lists, networking/VPC setup, etc.)*

1. **Data Extraction** — Pull stock‑market data (e.g., from APIs) and store raw files in S3.  
2. **Data Ingestion** — Use Airflow DAGs to load raw files into Snowflake Staging.  
3. **Transformations** — Use dbt models to build marts and analytics tables in Snowflake.  
4. **Deployment & Ops (v1.0.0 target)** — Pipeline scheduled and managed in cloud via Managed Airflow, secrets in AWS Secrets Manager, logging/monitoring via CloudWatch/third‑party, CI/CD deployments via GitHub Actions or similar.  
5. **Dashboard Layer (v2.0.0)** — Visualization layer for stakeholders, powered by Plotly Dash, connecting to Snowflake marts.

## Getting Started (v0.1.0 Local)

*These steps assume you are running locally with Astro CLI / Docker.*

1. Clone the repository:

   ```bash
   git clone https://github.com/davehasacat/airflow-aws-snowflake-stock-market-elt.git
   cd airflow-aws-snowflake-stock-market-elt
   ```

2. Copy `.env_example` to `.env` and fill in your Snowflake, AWS and other credentials.

3. Start the local Airflow environment:

   ```bash
   astro dev start
   ```

4. Verify the DAGs, sources and target marts operate as expected.  
5. Once local pipeline is validated, move to v1.0.0 (cloud) architecture.

## Deployment for v1.0.0 (Fully Cloud)

*(Brief overview — detailed steps to be added)*

- Provision AWS resources (S3 buckets, IAM roles/policies, VPC, networking)  
- Configure Managed Airflow (e.g., MWAA or Astro Cloud)  
- Deploy Snowflake in production mode (warehouse sizing, security/roles, database/schema)  
- Configure dbt (Cloud or containerized) to run models in Snowflake  
- Setup CI/CD pipeline for DAG + DBT code changes  
- Setup monitoring, logging, alerting, cost controls, tagging, documentation  

## Roadmap & Next Steps

- Finalize infrastructure for v1.0.0 (target: cloud‑first production).  
- Once v1.0.0 is green, move to v2.0.0: build and deploy dashboard layer.  
- Continuous improvements: data‑quality checks, lineage, observability, cost optimisation, governance.

## Contributing

Contributions, feedback, issues and feature requests are welcome. Please open a GitHub Issue for any bug or feature and submit a PR for changes.

## License

*(Insert your license information here)*
