-- ====================================================================
-- Snowflake Setup for Stock & Options ELT (Least-Privilege, Idempotent)
-- ====================================================================
-- AWS S3 Integration:
--   Bucket: s3://stock-market-elt/raw/
--   Region: us-east-2
--   IAM Role: arn:aws:iam::586159464756:role/SnowflakeS3Role
-- ====================================================================

USE ROLE ACCOUNTADMIN;

-- ------------------
-- Warehouses (unchanged)
-- ------------------
CREATE WAREHOUSE IF NOT EXISTS STOCKS_ELT_WH
  WAREHOUSE_SIZE = SMALL
  AUTO_SUSPEND   = 60
  AUTO_RESUME    = TRUE
  INITIALLY_SUSPENDED = TRUE
  COMMENT = 'Primary ELT compute for stocks/options ingestion & transforms';

CREATE WAREHOUSE IF NOT EXISTS STOCKS_DASHBOARD_WH
  WAREHOUSE_SIZE = XSMALL
  AUTO_SUSPEND   = 60
  AUTO_RESUME    = TRUE
  INITIALLY_SUSPENDED = TRUE
  COMMENT = 'BI/analytics/dashboard compute';

-- ------------------
-- Database & Schemas (UPDATED)
-- ------------------
CREATE DATABASE IF NOT EXISTS STOCKS_ELT_DB COMMENT = 'Dev database for stocks/options ELT';

-- Keep PUBLIC for shared utilities (stage/file formats)
CREATE SCHEMA IF NOT EXISTS STOCKS_ELT_DB.PUBLIC
  COMMENT = 'Utility/infra objects (stage, file formats)';

-- Functional schemas
CREATE SCHEMA IF NOT EXISTS STOCKS_ELT_DB.RAW
  COMMENT = 'Immutable source tables created by loaders (Airflow); referenced by dbt sources';

CREATE SCHEMA IF NOT EXISTS STOCKS_ELT_DB.SNAPSHOTS
  COMMENT = 'dbt SCD snapshot tables (history)';

-- PREP as TRANSIENT (cost-efficient, low retention)
CREATE TRANSIENT SCHEMA IF NOT EXISTS STOCKS_ELT_DB.PREP
  COMMENT = 'Staging + intermediate models (cleaned, typed, join-ready; transient for cost savings)';

-- Reduce retention window for transient schema
ALTER SCHEMA STOCKS_ELT_DB.PREP SET DATA_RETENTION_TIME_IN_DAYS = 1;

CREATE SCHEMA IF NOT EXISTS STOCKS_ELT_DB.MARTS
  COMMENT = 'Business-facing analytics marts (facts/dims/aggregates)';

-- ------------------
-- Role (service role)
-- ------------------
CREATE ROLE IF NOT EXISTS STOCKS_ELT_ROLE COMMENT = 'Service role for Airflow/dbt running the ELT';

-- ------------------
-- Storage Integration (unchanged)
-- ------------------
CREATE OR REPLACE STORAGE INTEGRATION S3_INTEGRATION
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = S3
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::586159464756:role/SnowflakeS3Role'
  STORAGE_ALLOWED_LOCATIONS = ('s3://stock-market-elt/raw/')
  COMMENT = 'Integration between Snowflake and S3 for ELT data (scoped to /raw folder)';

-- ------------------
-- File Formats (unchanged; live in PUBLIC)
-- ------------------
CREATE OR REPLACE FILE FORMAT STOCKS_ELT_DB.PUBLIC.FF_JSON
  TYPE = JSON
  STRIP_OUTER_ARRAY = TRUE
  COMMENT = 'Default JSON format for Polygon & other API payloads';

CREATE OR REPLACE FILE FORMAT STOCKS_ELT_DB.PUBLIC.FF_CSV
  TYPE = CSV
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  SKIP_HEADER = 1
  NULL_IF = ('', 'NULL', 'null')
  EMPTY_FIELD_AS_NULL = TRUE
  COMMENT = 'CSV loader (headers in row 1)';

-- ------------------
-- Stage (unchanged; lives in PUBLIC)
-- ------------------
CREATE OR REPLACE STAGE STOCKS_ELT_DB.PUBLIC.S3_STAGE
  STORAGE_INTEGRATION = S3_INTEGRATION
  URL = 's3://stock-market-elt/raw/'
  FILE_FORMAT = STOCKS_ELT_DB.PUBLIC.FF_JSON
  COMMENT = 'Stage for ELT raw data ingestion (restricted to /raw prefix)';

-- ------------------
-- Service User (passwordless; key-pair auth)
-- ------------------
CREATE USER IF NOT EXISTS airflow_stocks_user
  DEFAULT_ROLE         = STOCKS_ELT_ROLE
  DEFAULT_WAREHOUSE    = STOCKS_ELT_WH
  DEFAULT_NAMESPACE    = STOCKS_ELT_DB.PREP
  MUST_CHANGE_PASSWORD = FALSE
  COMMENT              = 'Airflow service user for key-pair auth';

-- Attach role to service user
GRANT ROLE STOCKS_ELT_ROLE TO USER airflow_stocks_user;

-- ------------------
-- Privileges (least privilege) — RAW / SNAPSHOTS / PREP / MARTS
-- ------------------
-- Warehouse access
GRANT USAGE  ON WAREHOUSE STOCKS_ELT_WH       TO ROLE STOCKS_ELT_ROLE;
GRANT USAGE  ON WAREHOUSE STOCKS_DASHBOARD_WH TO ROLE STOCKS_ELT_ROLE;
GRANT MONITOR ON WAREHOUSE STOCKS_ELT_WH       TO ROLE STOCKS_ELT_ROLE;
GRANT MONITOR ON WAREHOUSE STOCKS_DASHBOARD_WH TO ROLE STOCKS_ELT_ROLE;

-- Database usage
GRANT USAGE ON DATABASE STOCKS_ELT_DB TO ROLE STOCKS_ELT_ROLE;

-- Schema usage
GRANT USAGE ON SCHEMA STOCKS_ELT_DB.PUBLIC    TO ROLE STOCKS_ELT_ROLE; -- for stage/file formats
GRANT USAGE ON SCHEMA STOCKS_ELT_DB.RAW       TO ROLE STOCKS_ELT_ROLE;
GRANT USAGE ON SCHEMA STOCKS_ELT_DB.SNAPSHOTS TO ROLE STOCKS_ELT_ROLE;
GRANT USAGE ON SCHEMA STOCKS_ELT_DB.PREP      TO ROLE STOCKS_ELT_ROLE;
GRANT USAGE ON SCHEMA STOCKS_ELT_DB.MARTS     TO ROLE STOCKS_ELT_ROLE;

-- Limit CREATE privileges to where they’re needed (not PUBLIC)
-- RAW: Airflow loaders create/append tables; dbt reads as sources
GRANT CREATE TABLE, CREATE VIEW
  ON SCHEMA STOCKS_ELT_DB.RAW TO ROLE STOCKS_ELT_ROLE;

-- SNAPSHOTS: dbt snapshot tables
GRANT CREATE TABLE, CREATE VIEW
  ON SCHEMA STOCKS_ELT_DB.SNAPSHOTS TO ROLE STOCKS_ELT_ROLE;

-- PREP: dbt staging/intermediate models
GRANT CREATE TABLE, CREATE VIEW
  ON SCHEMA STOCKS_ELT_DB.PREP TO ROLE STOCKS_ELT_ROLE;

-- MARTS: dbt facts/dims/aggregates
GRANT CREATE TABLE, CREATE VIEW
  ON SCHEMA STOCKS_ELT_DB.MARTS TO ROLE STOCKS_ELT_ROLE;

-- Integration & infra usage
GRANT USAGE ON INTEGRATION S3_INTEGRATION                TO ROLE STOCKS_ELT_ROLE;
GRANT USAGE ON STAGE       STOCKS_ELT_DB.PUBLIC.S3_STAGE TO ROLE STOCKS_ELT_ROLE;
GRANT USAGE ON FILE FORMAT STOCKS_ELT_DB.PUBLIC.FF_JSON  TO ROLE STOCKS_ELT_ROLE;
GRANT USAGE ON FILE FORMAT STOCKS_ELT_DB.PUBLIC.FF_CSV   TO ROLE STOCKS_ELT_ROLE;

-- ------------------
-- Object privileges (current + future) per schema
-- ------------------

-- RAW
GRANT SELECT, INSERT, UPDATE, DELETE
  ON ALL TABLES IN SCHEMA STOCKS_ELT_DB.RAW TO ROLE STOCKS_ELT_ROLE;
GRANT SELECT, INSERT, UPDATE, DELETE
  ON FUTURE TABLES IN SCHEMA STOCKS_ELT_DB.RAW TO ROLE STOCKS_ELT_ROLE;

GRANT SELECT
  ON ALL VIEWS IN SCHEMA STOCKS_ELT_DB.RAW TO ROLE STOCKS_ELT_ROLE;
GRANT SELECT
  ON FUTURE VIEWS IN SCHEMA STOCKS_ELT_DB.RAW TO ROLE STOCKS_ELT_ROLE;

-- SNAPSHOTS
GRANT SELECT, INSERT, UPDATE, DELETE
  ON ALL TABLES IN SCHEMA STOCKS_ELT_DB.SNAPSHOTS TO ROLE STOCKS_ELT_ROLE;
GRANT SELECT, INSERT, UPDATE, DELETE
  ON FUTURE TABLES IN SCHEMA STOCKS_ELT_DB.SNAPSHOTS TO ROLE STOCKS_ELT_ROLE;

GRANT SELECT
  ON ALL VIEWS IN SCHEMA STOCKS_ELT_DB.SNAPSHOTS TO ROLE STOCKS_ELT_ROLE;
GRANT SELECT
  ON FUTURE VIEWS IN SCHEMA STOCKS_ELT_DB.SNAPSHOTS TO ROLE STOCKS_ELT_ROLE;

-- PREP
GRANT SELECT, INSERT, UPDATE, DELETE
  ON ALL TABLES IN SCHEMA STOCKS_ELT_DB.PREP TO ROLE STOCKS_ELT_ROLE;
GRANT SELECT, INSERT, UPDATE, DELETE
  ON FUTURE TABLES IN SCHEMA STOCKS_ELT_DB.PREP TO ROLE STOCKS_ELT_ROLE;

GRANT SELECT
  ON ALL VIEWS IN SCHEMA STOCKS_ELT_DB.PREP TO ROLE STOCKS_ELT_ROLE;
GRANT SELECT
  ON FUTURE VIEWS IN SCHEMA STOCKS_ELT_DB.PREP TO ROLE STOCKS_ELT_ROLE;

-- MARTS
GRANT SELECT, INSERT, UPDATE, DELETE
  ON ALL TABLES IN SCHEMA STOCKS_ELT_DB.MARTS TO ROLE STOCKS_ELT_ROLE;
GRANT SELECT, INSERT, UPDATE, DELETE
  ON FUTURE TABLES IN SCHEMA STOCKS_ELT_DB.MARTS TO ROLE STOCKS_ELT_ROLE;

GRANT SELECT
  ON ALL VIEWS IN SCHEMA STOCKS_ELT_DB.MARTS TO ROLE STOCKS_ELT_ROLE;
GRANT SELECT
  ON FUTURE VIEWS IN SCHEMA STOCKS_ELT_DB.MARTS TO ROLE STOCKS_ELT_ROLE;

-- (Intentionally no broad "ALL PRIVILEGES" on schemas)

-- ------------------
-- Optional clean-up of human user access (recommended)
-- ------------------
-- REVOKE ROLE STOCKS_ELT_ROLE FROM USER DAVEHASACAT;

-- ------------------
-- Sanity checks
-- ------------------
SHOW GRANTS TO USER airflow_stocks_user;
SHOW GRANTS TO ROLE STOCKS_ELT_ROLE;

-- ------------------
-- Get S3_INTEGRATION info after setup
-- ------------------
DESCRIBE INTEGRATION S3_INTEGRATION;
