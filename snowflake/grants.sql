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
-- Stage (server-side via storage integration)
-- ------------------
CREATE OR REPLACE STAGE STOCKS_ELT_DB.PUBLIC.S3_STAGE
  STORAGE_INTEGRATION = S3_INTEGRATION
  URL = 's3://stock-market-elt/raw/'
  FILE_FORMAT = STOCKS_ELT_DB.PUBLIC.FF_JSON
  COMMENT = 'Stage for ELT raw data ingestion (restricted to /raw prefix)';

-- ------------------
-- Privileges
-- ------------------

-- Warehouses for ELT role
GRANT USAGE, OPERATE ON WAREHOUSE STOCKS_ELT_WH       TO ROLE STOCKS_ELT_ROLE;
GRANT USAGE, OPERATE ON WAREHOUSE STOCKS_DASHBOARD_WH TO ROLE STOCKS_ELT_ROLE;
GRANT MONITOR ON WAREHOUSE STOCKS_ELT_WH              TO ROLE STOCKS_ELT_ROLE;
GRANT MONITOR ON WAREHOUSE STOCKS_DASHBOARD_WH        TO ROLE STOCKS_ELT_ROLE;

-- Database / Schema
GRANT USAGE         ON DATABASE STOCKS_ELT_DB        TO ROLE STOCKS_ELT_ROLE;
GRANT CREATE SCHEMA ON DATABASE STOCKS_ELT_DB        TO ROLE STOCKS_ELT_ROLE;
GRANT USAGE         ON SCHEMA   STOCKS_ELT_DB.PUBLIC TO ROLE STOCKS_ELT_ROLE;

GRANT CREATE TABLE, CREATE VIEW, CREATE STAGE, CREATE FILE FORMAT,
      CREATE PIPE, CREATE FUNCTION, CREATE PROCEDURE
  ON SCHEMA STOCKS_ELT_DB.PUBLIC TO ROLE STOCKS_ELT_ROLE;

-- Integration & staging helpers
GRANT USAGE ON INTEGRATION S3_INTEGRATION                 TO ROLE STOCKS_ELT_ROLE;
GRANT USAGE ON STAGE       STOCKS_ELT_DB.PUBLIC.S3_STAGE  TO ROLE STOCKS_ELT_ROLE;
GRANT USAGE ON FILE FORMAT STOCKS_ELT_DB.PUBLIC.FF_JSON   TO ROLE STOCKS_ELT_ROLE;
GRANT USAGE ON FILE FORMAT STOCKS_ELT_DB.PUBLIC.FF_CSV    TO ROLE STOCKS_ELT_ROLE;

-- Broad schema privileges (convenience)
GRANT ALL PRIVILEGES ON SCHEMA STOCKS_ELT_DB.PUBLIC TO ROLE STOCKS_ELT_ROLE;

-- 🔹 One-time backfill grants on EXISTING objects (so your role can query immediately)
GRANT SELECT, INSERT, UPDATE, DELETE
  ON ALL TABLES IN SCHEMA STOCKS_ELT_DB.PUBLIC TO ROLE STOCKS_ELT_ROLE;
GRANT SELECT
  ON ALL VIEWS  IN SCHEMA STOCKS_ELT_DB.PUBLIC TO ROLE STOCKS_ELT_ROLE;

-- 🔹 Auto-grants for FUTURE objects (already present; keeping as-is)
GRANT SELECT, INSERT, UPDATE, DELETE ON FUTURE TABLES       IN SCHEMA STOCKS_ELT_DB.PUBLIC TO ROLE STOCKS_ELT_ROLE;
GRANT SELECT                           ON FUTURE VIEWS        IN SCHEMA STOCKS_ELT_DB.PUBLIC TO ROLE STOCKS_ELT_ROLE;
GRANT USAGE                            ON FUTURE STAGES       IN SCHEMA STOCKS_ELT_DB.PUBLIC TO ROLE STOCKS_ELT_ROLE;
GRANT USAGE                            ON FUTURE FILE FORMATS IN SCHEMA STOCKS_ELT_DB.PUBLIC TO ROLE STOCKS_ELT_ROLE;

-- (Optional) Allow SYSADMIN to read raw landing tables now and in the future
GRANT SELECT ON ALL TABLES  IN SCHEMA STOCKS_ELT_DB.PUBLIC TO ROLE SYSADMIN;
GRANT SELECT ON FUTURE TABLES IN SCHEMA STOCKS_ELT_DB.PUBLIC TO ROLE SYSADMIN;
GRANT SELECT ON ALL VIEWS   IN SCHEMA STOCKS_ELT_DB.PUBLIC TO ROLE SYSADMIN;
GRANT SELECT ON FUTURE VIEWS IN SCHEMA STOCKS_ELT_DB.PUBLIC TO ROLE SYSADMIN;

-- ------------------
-- Assign Role & user defaults (QoL)
-- ------------------
GRANT ROLE STOCKS_ELT_ROLE TO USER DAVEHASACAT;

-- Make new sessions “just work”
ALTER USER DAVEHASACAT
  SET DEFAULT_ROLE      = STOCKS_ELT_ROLE,
      DEFAULT_WAREHOUSE = STOCKS_ELT_WH,
      DEFAULT_NAMESPACE = STOCKS_ELT_DB.PUBLIC;

-- (Optional) Sanity: set session context for current worksheet if needed
-- USE ROLE STOCKS_ELT_ROLE;
-- USE WAREHOUSE STOCKS_ELT_WH;
-- USE DATABASE STOCKS_ELT_DB;
-- USE SCHEMA PUBLIC;

-- ====================================================================
-- Done. You should now be able to:
-- SELECT * FROM STOCKS_ELT_DB.PUBLIC.SOURCE_POLYGON_STOCKS_RAW LIMIT 10;
-- ====================================================================
