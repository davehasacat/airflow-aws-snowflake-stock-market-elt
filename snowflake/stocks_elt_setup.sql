-- ====================================================================
-- Snowflake Setup for Stock and Options ELT Project
-- ====================================================================

-- WAREHOUSE CREATION
-- ------------------
-- A 'SMALL' warehouse for the heavy data processing (Airflow/dbt)
CREATE WAREHOUSE IF NOT EXISTS STOCKS_ELT_WH
  WAREHOUSE_SIZE = 'SMALL'
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE;

-- An 'XSMALL' warehouse for the user-facing dashboard to ensure UI responsiveness
CREATE WAREHOUSE IF NOT EXISTS STOCKS_DASHBOARD_WH
  WAREHOUSE_SIZE = 'XSMALL'
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE;


-- DATABASE CREATION
-- -----------------
-- A dedicated database to store all project-related data
CREATE DATABASE IF NOT EXISTS STOCKS_ELT_DB;


-- ROLE CREATION
-- -------------
-- A custom role with specific permissions for the application
CREATE ROLE IF NOT EXISTS STOCKS_ELT_ROLE;


-- PERMISSION GRANTING
-- -------------------
-- Grant the necessary privileges to the custom role

-- 1. Grant usage on the warehouses
GRANT USAGE ON WAREHOUSE STOCKS_ELT_WH TO ROLE STOCKS_ELT_ROLE;
GRANT USAGE ON WAREHOUSE STOCKS_DASHBOARD_WH TO ROLE STOCKS_ELT_ROLE;

-- 2. Grant usage on the database
GRANT USAGE ON DATABASE STOCKS_ELT_DB TO ROLE STOCKS_ELT_ROLE;

-- 3. Grant full permissions on the 'public' schema to allow table creation/modification
GRANT ALL PRIVILEGES ON SCHEMA STOCKS_ELT_DB.PUBLIC TO ROLE STOCKS_ELT_ROLE;


-- ROLE ASSIGNMENT
-- ---------------
-- Assign the custom role to your specific Snowflake user
-- !! IMPORTANT !! -> Replace 'YOUR_SNOWFLAKE_USER' with your actual username
GRANT ROLE STOCKS_ELT_ROLE TO USER DAVEHASACAT;
