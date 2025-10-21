# Airflow-AWS-Snowflake Stock Market ELT

## Overview

This repository hosts the ELT (Extract-Load-Transform) pipeline for stock-market data using Apache Airflow, dbt, and Snowflake, built to run on AWS infrastructure.  
The project is structured in **phases**:

- **Phase 1 (v0.1.0)** — ✅ *Local Development Complete*: Full end-to-end pipeline running locally (Docker Desktop / Astro).  
- **Phase 2 (v1.0.0)** — ☁️ *Fully Cloud Operational*: Infrastructure deployed and orchestrated in AWS (and Snowflake in production mode) with no local dependencies.  
- **Phase 3 (v2.0.0)** — 📊 *Analytics Dashboard Layer*: Plotly Dash (or Streamlit) app connecting to Snowflake marts for end-user analytics.

---

## 🧩 Project Versioning

| Version | Status | Goal / Acceptance Criteria | Key Components |
|----------|---------|-----------------------------|----------------|
| **v0.1.0 — MVP (Local)** | ✅ Complete | Pipeline runs full end-to-end **locally** with data flowing Polygon → S3 → Snowflake → dbt models → marts. | Airflow (local), AWS S3 (dev), Snowflake (dev), dbt Core, Secrets Manager, IAM roles/policies |
| **v1.0.0 — Fully Cloud Operational** | 🚧 Next Milestone | Pipeline runs **fully in the cloud**, no local dependencies. Infrastructure as code + CI/CD, monitoring, and cost/ops readiness. | Managed Airflow (MWAA or Astro Cloud), AWS S3 (lifecycle policies), Snowflake (prod), dbt Cloud/containerized, observability + IAM governance |
| **v2.0.0 — Analytics Dashboard (Plotly Dash)** | 🔜 Future | Adds interactive dashboards hosted on AWS (App Runner / ECS) powered by Snowflake marts. | Dash app, Snowflake connector, authN/authZ, deployment pipeline, optional API gateway |

---

## Architecture

1. **Data Extraction** — Pull stock-market data (e.g., Polygon APIs) and store raw JSON files in S3.  
2. **Data Ingestion** — Airflow DAGs load raw files into Snowflake staging tables.  
3. **Transformations** — dbt models build marts and analytics tables in Snowflake.  
4. **Deployment & Ops (v1.0.0 Target)** — Managed Airflow, AWS Secrets Manager, CloudWatch logging, CI/CD pipelines, IAM governance.  
5. **Dashboard Layer (v2.0.0)** — Plotly Dash visualization layer for end users.

*(Architecture diagrams and AWS service maps will be added in the `/docs` directory.)*

---

## Getting Started (v0.1.0 Local)

These steps assume you are running locally with **Astro CLI / Docker**.

```bash
git clone https://github.com/davehasacat/airflow-aws-snowflake-stock-market-elt.git
cd airflow-aws-snowflake-stock-market-elt
cp .env_example .env
```

1. Update `.env` with your Snowflake and AWS credentials.  
2. Start the local Airflow environment:

   ```bash
   astro dev start
   ```

3. Trigger the DAGs to validate data flow Polygon → S3 → Snowflake.  
4. Confirm dbt models run successfully and marts are queryable in Snowflake.  
5. Once the local pipeline is validated, begin Phase 2 (v1.0.0 Cloud Deployment).

---

## Deployment (v1.0.0 Preview)

- Provision AWS resources (S3 buckets, IAM roles, VPC, networking).  
- Configure Managed Airflow (MWAA or Astro Cloud).  
- Deploy Snowflake (prod) with optimized warehouse sizing and role hierarchies.  
- Configure dbt (Cloud or containerized) to run transformations.  
- Implement CI/CD for DAG + dbt code promotion.  
- Add monitoring, alerting, cost tracking and documentation.

---

## Roadmap & Next Steps

- [x] Finalize v0.1.0 (local pipeline complete)  
- [ ] Migrate Airflow and dbt to AWS/Snowflake prod environment (v1.0.0)  
- [ ] Add Plotly Dash dashboard layer (v2.0.0)  
- [ ] Enhance data quality, lineage, and observability  

---

## Contributing

Contributions, feedback, and feature requests are welcome!  
Open a GitHub Issue for bugs/features or submit a Pull Request with proposed changes.

---

## License / Licensure

© 2025 **[Your LLC Name, LLC]**. All rights reserved.  
This project and its contents are owned and maintained by [Your LLC Name].  

Unless explicitly stated otherwise, no part of this repository may be copied, modified, or distributed without written permission.  
If you intend to open-source the project, replace this section with your chosen license (e.g. MIT, Apache 2.0, etc.) and include a `LICENSE` file.
