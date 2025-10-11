-- ====================================================================
-- Snowflake Setup for Stock and Options ELT Project (Final Corrected)
-- ====================================================================
-- This script is idempotent and can be run multiple times safely.

-- WAREHOUSE CREATION
-- ------------------
CREATE WAREHOUSE IF NOT EXISTS STOCKS_ELT_WH
  WAREHOUSE_SIZE = 'SMALL' AUTO_SUSPEND = 60 AUTO_RESUME = TRUE INITIALLY_SUSPENDED = TRUE;

CREATE WAREHOUSE IF NOT EXISTS STOCKS_DASHBOARD_WH
  WAREHOUSE_SIZE = 'XSMALL' AUTO_SUSPEND = 60 AUTO_RESUME = TRUE INITIALLY_SUSPENDED = TRUE;


-- DATABASE, SCHEMA, and ROLE CREATION
-- -----------------------------------
CREATE DATABASE IF NOT EXISTS STOCKS_ELT_DB;
CREATE SCHEMA IF NOT EXISTS STOCKS_ELT_DB.PUBLIC;
CREATE ROLE IF NOT EXISTS STOCKS_ELT_ROLE;


-- STORAGE INTEGRATION CREATION
-- ----------------------------
-- This object creates the secure link between Snowflake and your AWS IAM Role.
-- !! IMPORTANT !! -> Replace 'YOUR_AWS_ACCOUNT_ID' and 'your-bucket-name'
CREATE OR REPLACE STORAGE INTEGRATION s3_integration
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::YOUR_AWS_ACCOUNT_ID:role/SnowflakeS3Role'
  STORAGE_ALLOWED_LOCATIONS = ('s3://stock-market-elt/');


-- STAGE CREATION
-- --------------
-- This command creates the Stage object that references the Storage Integration.
CREATE OR REPLACE STAGE STOCKS_ELT_DB.PUBLIC.s3_stage
  STORAGE_INTEGRATION = s3_integration
  URL = 's3://stock-market-elt/'
  FILE_FORMAT = (TYPE = 'JSON');


-- PERMISSION GRANTING
-- -------------------
-- Grant all necessary privileges to the custom role in logical order

-- 1. Grant usage on the warehouses
GRANT USAGE ON WAREHOUSE STOCKS_ELT_WH TO ROLE STOCKS_ELT_ROLE;
GRANT USAGE ON WAREHOUSE STOCKS_DASHBOARD_WH TO ROLE STOCKS_ELT_ROLE;

-- 2. Grant usage on the database and schema
GRANT USAGE ON DATABASE STOCKS_ELT_DB TO ROLE STOCKS_ELT_ROLE;
GRANT USAGE ON SCHEMA STOCKS_ELT_DB.PUBLIC TO ROLE STOCKS_ELT_ROLE;

-- 3. Grant usage on the Storage Integration
GRANT USAGE ON INTEGRATION s3_integration TO ROLE STOCKS_ELT_ROLE;

-- 4. Grant usage on the Stage
GRANT USAGE ON STAGE STOCKS_ELT_DB.PUBLIC.s3_stage TO ROLE STOCKS_ELT_ROLE;

-- 5. Grant permissions for the role to create objects within the schema
GRANT ALL PRIVILEGES ON SCHEMA STOCKS_ELT_DB.PUBLIC TO ROLE STOCKS_ELT_ROLE;


-- ROLE ASSIGNMENT
-- ---------------
-- Assign the custom role to your specific Snowflake user.
-- !! IMPORTANT !! -> Replace 'DAVEHASACAT' with your actual username if different.
GRANT ROLE STOCKS_ELT_ROLE TO USER DAVEHASACAT;
