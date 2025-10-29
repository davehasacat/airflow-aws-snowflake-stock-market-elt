# Enterprise Stock Market ELT

## Overview

This repository hosts the ELT (Extract-Load-Transform) pipeline for stock-market data using Apache Airflow, dbt, and Snowflake, built to run on AWS infrastructure.  
The project is structured in **phases**:

- **Phase 1 (v0.1.0)** — ✅ *Local Development Complete*: Full end-to-end pipeline running locally (Docker Desktop / Astro).  
- **Phase 2 (v1.0.0)** — ✅ *Fully Cloud Operational*: Infrastructure deployed and orchestrated in AWS (and Snowflake in production mode) with no local dependencies.  
- **Phase 3 (v2.0.0)** — ☁️ *Analytics Dashboard Layer*: Plotly Dash (or Streamlit) app connecting to Snowflake marts for end-user analytics.

---

## 🧩 Project Versioning

| Version | Status | Goal / Acceptance Criteria | Key Components |
|----------|---------|-----------------------------|----------------|
| **v0.1.0 — MVP (Local)** | ✅ Complete | Pipeline runs full end-to-end **locally** with data flowing Polygon → S3 → Snowflake → dbt models → marts. | Airflow (local), AWS S3 (dev), Snowflake (dev), dbt Core, Secrets Manager, IAM roles/policies |
| **v1.0.0 — Fully Cloud Operational** | ✅ Complete | Pipeline runs **fully in the cloud**, no local dependencies. Infrastructure as code + CI/CD, monitoring, and cost/ops readiness. | Managed Airflow (MWAA), AWS S3 (lifecycle policies), Snowflake (prod), dbt Cloud, observability + IAM governance |
| **v2.0.0 — Analytics Dashboard (Plotly Dash)** | 🚧 Next Milestone | Adds interactive dashboards hosted on AWS (App Runner / ECS) powered by Snowflake marts. | Dash app, Snowflake connector, authN/authZ, deployment pipeline, optional API gateway |

---

## Architecture

1. **Data Extraction** — Pull stock-market data (e.g., Polygon APIs) and store raw JSON files in S3.  
2. **Data Ingestion** — Airflow DAGs load raw files into Snowflake staging tables.  
3. **Transformations** — dbt models build marts and analytics tables in Snowflake.  
4. **Deployment & Ops (v1.0.0 Complete)** — Managed Airflow, AWS Secrets Manager, CloudWatch logging, CI/CD pipelines, IAM governance.  
5. **Dashboard Layer (v2.0.0 Target)** — Plotly Dash visualization layer for end users.

<img width="1939" height="363" alt="v0 1 0-pipeline" src="https://github.com/user-attachments/assets/6215ef30-1003-4716-82cb-7c633be73f94" />

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

## Deployment (v1.0.0 Complete)

- All infrastructure now runs **fully within AWS** (VPC, private networking, endpoints).  
- Airflow orchestrated via **Managed Workflows for Apache Airflow (MWAA)**.  
- Snowflake operates in **production mode** with role-based access and optimized warehouses.  
- dbt Cloud handles transformations and job scheduling.  
- CI/CD pipelines govern DAG and dbt deployments.  
- CloudWatch provides centralized logging and observability.  
- S3 buckets, IAM roles, and KMS encryption all follow least-privilege, enterprise-grade standards.

---

## 🔐 Security & Key Management

This project follows **gold-standard cloud security principles**:

- **Least Privilege Policies:** Every IAM role, bucket policy, and KMS key grant is scoped narrowly to what the component needs — nothing more. MWAA, Snowflake, and Secrets Manager each operate under purpose-built roles.  
- **Customer-Managed Encryption Keys (CMKs):** All data (S3, logs, and Snowflake stages) is encrypted using CMKs with enforced `aws:SourceVpce` conditions for private-network access.  
- **KMS Governance:** Key policies delegate minimal administrative rights and explicitly define which AWS services (e.g., `logs`, `s3`, `mwaa`) may use the key via `kms:ViaService`.  
- **Network Isolation:** All traffic remains inside the custom VPC with endpoint-based access to AWS services — no public internet routes.  
- **Auditing & Observability:** CloudTrail and CloudWatch track all key usage, API calls, and operational metrics for full compliance readiness.

This architecture meets or exceeds best practices for **enterprise-grade security posture**, designed for private networking, encryption in transit and at rest, and role-based access control.

---

## Roadmap & Next Steps

- [x] Finalize v0.1.0 (local pipeline complete)  
- [x] Migrate Airflow and dbt to AWS/Snowflake prod environment (v1.0.0)  
- [ ] Build and deploy Plotly Dash dashboard layer (v2.0.0)  
- [ ] Enhance data quality, lineage, and observability  

---

## Contributing

Contributions, feedback, and feature requests are welcome!  
Open a GitHub Issue for bugs/features or submit a Pull Request with proposed changes.

---

## License

© 2025 **DaveHasACat LLC**. All rights reserved.  
This project and its contents are owned and maintained by davehasacat.  

Unless explicitly stated otherwise, no part of this repository may be copied, modified, or distributed without written permission.  
If you intend to open-source the project, replace this section with your chosen license (e.g. MIT, Apache 2.0, etc.) and include a `LICENSE` file.
