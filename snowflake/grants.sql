-- ====================================================================
-- Snowflake Setup for Stock & Options ELT (Idempotent)
-- ====================================================================
-- AWS S3 Integration:
--   Bucket: s3://stock-market-elt/raw/
--   Region: us-east-2
--   IAM Role: arn:aws:iam::586159464756:role/SnowflakeS3Role
-- ====================================================================

USE ROLE ACCOUNTADMIN;

-- ------------------
-- Warehouses
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
-- Database & Schema
-- ------------------
CREATE DATABASE IF NOT EXISTS STOCKS_ELT_DB COMMENT = 'Dev database for stocks/options ELT';
CREATE SCHEMA   IF NOT EXISTS STOCKS_ELT_DB.PUBLIC COMMENT = 'Default schema for ELT objects';

-- ------------------
-- Role
-- ------------------
CREATE ROLE IF NOT EXISTS STOCKS_ELT_ROLE COMMENT = 'Service role for Airflow/dbt running the ELT';

-- ------------------
-- Storage Integration
-- ------------------
CREATE OR REPLACE STORAGE INTEGRATION S3_INTEGRATION
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = S3
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::586159464756:role/SnowflakeS3Role'
  STORAGE_ALLOWED_LOCATIONS = ('s3://stock-market-elt/raw/')
  COMMENT = 'Integration between Snowflake and S3 for ELT data (scoped to /raw folder)';

-- ------------------
-- File Formats
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
-- Stage
-- ------------------
CREATE OR REPLACE STAGE STOCKS_ELT_DB.PUBLIC.S3_STAGE
  STORAGE_INTEGRATION = S3_INTEGRATION
  URL = 's3://stock-market-elt/raw/'
  FILE_FORMAT = STOCKS_ELT_DB.PUBLIC.FF_JSON
  COMMENT = 'Stage for ELT raw data ingestion (restricted to /raw prefix)';

-- ------------------
-- Privileges
-- ------------------
-- Warehouses
GRANT USAGE, OPERATE ON WAREHOUSE STOCKS_ELT_WH       TO ROLE STOCKS_ELT_ROLE;
GRANT USAGE, OPERATE ON WAREHOUSE STOCKS_DASHBOARD_WH TO ROLE STOCKS_ELT_ROLE;
-- Optional: better visibility/metrics
GRANT MONITOR ON WAREHOUSE STOCKS_ELT_WH       TO ROLE STOCKS_ELT_ROLE;
GRANT MONITOR ON WAREHOUSE STOCKS_DASHBOARD_WH TO ROLE STOCKS_ELT_ROLE;

-- Database / Schema
GRANT USAGE         ON DATABASE STOCKS_ELT_DB         TO ROLE STOCKS_ELT_ROLE;
GRANT CREATE SCHEMA ON DATABASE STOCKS_ELT_DB         TO ROLE STOCKS_ELT_ROLE;  -- <-- added
GRANT USAGE         ON SCHEMA   STOCKS_ELT_DB.PUBLIC  TO ROLE STOCKS_ELT_ROLE;

GRANT CREATE TABLE, CREATE VIEW, CREATE STAGE, CREATE FILE FORMAT,
      CREATE PIPE, CREATE FUNCTION, CREATE PROCEDURE
  ON SCHEMA STOCKS_ELT_DB.PUBLIC TO ROLE STOCKS_ELT_ROLE;

-- Integration & staging helpers
GRANT USAGE ON INTEGRATION S3_INTEGRATION                TO ROLE STOCKS_ELT_ROLE;
GRANT USAGE ON STAGE       STOCKS_ELT_DB.PUBLIC.S3_STAGE TO ROLE STOCKS_ELT_ROLE;
GRANT USAGE ON FILE FORMAT STOCKS_ELT_DB.PUBLIC.FF_JSON  TO ROLE STOCKS_ELT_ROLE;
GRANT USAGE ON FILE FORMAT STOCKS_ELT_DB.PUBLIC.FF_CSV   TO ROLE STOCKS_ELT_ROLE;

-- Broad schema privileges (kept for convenience)
GRANT ALL PRIVILEGES ON SCHEMA STOCKS_ELT_DB.PUBLIC TO ROLE STOCKS_ELT_ROLE;

-- Auto-grants for future objects (keeps dbt + Airflow smooth)
GRANT SELECT, INSERT, UPDATE, DELETE ON FUTURE TABLES      IN SCHEMA STOCKS_ELT_DB.PUBLIC TO ROLE STOCKS_ELT_ROLE;
GRANT SELECT                          ON FUTURE VIEWS       IN SCHEMA STOCKS_ELT_DB.PUBLIC TO ROLE STOCKS_ELT_ROLE;
GRANT USAGE                           ON FUTURE STAGES      IN SCHEMA STOCKS_ELT_DB.PUBLIC TO ROLE STOCKS_ELT_ROLE;
GRANT USAGE                           ON FUTURE FILE FORMATS IN SCHEMA STOCKS_ELT_DB.PUBLIC TO ROLE STOCKS_ELT_ROLE;

-- ------------------
-- Assign Role
-- ------------------
GRANT ROLE STOCKS_ELT_ROLE TO USER DAVEHASACAT;
-- Optional defaults:
-- ALTER USER DAVEHASACAT SET DEFAULT_ROLE = STOCKS_ELT_ROLE;
-- ALTER USER DAVEHASACAT SET DEFAULT_WAREHOUSE = STOCKS_ELT_WH;
-- ALTER USER DAVEHASACAT SET DEFAULT_NAMESPACE = STOCKS_ELT_DB.PUBLIC;
