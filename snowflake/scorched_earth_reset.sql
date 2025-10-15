-- ====================================================================
-- ⚠️ SCORCHED EARTH: DROP & RE-CREATE EVERYTHING FOR STOCKS/OPTIONS ELT
-- Run as ACCOUNTADMIN
-- ====================================================================

USE ROLE ACCOUNTADMIN;

-- ───────────────────────────────────────────────────────────────────
-- 0) Vars (adjust as needed)
-- ───────────────────────────────────────────────────────────────────
SET DB         = 'STOCKS_ELT_DB';
SET SCHEMA     = 'PUBLIC';
SET ROLE_NAME  = 'STOCKS_ELT_ROLE';
SET WH_ELT     = 'STOCKS_ELT_WH';
SET WH_DASH    = 'STOCKS_DASHBOARD_WH';

SET INTEGRATION_NAME = 'S3_INTEGRATION';
SET STAGE_NAME       = 'S3_STAGE';
SET FF_JSON          = 'FF_JSON';
SET FF_CSV           = 'FF_CSV';

SET PROJECT_USER     = 'DAVEHASACAT';

-- For rebuild:
SET S3_URL           = 's3://stock-market-elt/raw/';
SET S3_IAM_ROLE_ARN  = 'arn:aws:iam::586159464756:role/SnowflakeS3Role';

-- ───────────────────────────────────────────────────────────────────
-- 1) REVOKE & CLEAN GRANTS ON ROLE BEFORE DROP
-- ───────────────────────────────────────────────────────────────────
-- Revoke role from user(s) (ignore failures)
BEGIN
  REVOKE ROLE IDENTIFIER($ROLE_NAME) FROM USER IDENTIFIER($PROJECT_USER);
EXCEPTION
  WHEN STATEMENT_ERROR THEN
    -- continue
END;

-- ───────────────────────────────────────────────────────────────────
-- 2) DROP OBJECTS THAT REFERENCE THE INTEGRATION (STAGE, etc.)
-- ───────────────────────────────────────────────────────────────────
-- Drop stage & file formats (if DB/schema exist)
BEGIN
  EXECUTE IMMEDIATE
    'DROP STAGE IF EXISTS ' || $DB || '.' || $SCHEMA || '.' || $STAGE_NAME || ';';
EXCEPTION WHEN STATEMENT_ERROR THEN
  -- continue
END;

BEGIN
  EXECUTE IMMEDIATE
    'DROP FILE FORMAT IF EXISTS ' || $DB || '.' || $SCHEMA || '.' || $FF_JSON || ';';
EXCEPTION WHEN STATEMENT_ERROR THEN
END;

BEGIN
  EXECUTE IMMEDIATE
    'DROP FILE FORMAT IF EXISTS ' || $DB || '.' || $SCHEMA || '.' || $FF_CSV || ';';
EXCEPTION WHEN STATEMENT_ERROR THEN
END;

-- ───────────────────────────────────────────────────────────────────
-- 3) DROP WAREHOUSES (suspend first just in case)
-- ───────────────────────────────────────────────────────────────────
BEGIN
  ALTER WAREHOUSE IDENTIFIER($WH_ELT) SUSPEND;
EXCEPTION WHEN STATEMENT_ERROR THEN
END;
DROP WAREHOUSE IF EXISTS IDENTIFIER($WH_ELT);

BEGIN
  ALTER WAREHOUSE IDENTIFIER($WH_DASH) SUSPEND;
EXCEPTION WHEN STATEMENT_ERROR THEN
END;
DROP WAREHOUSE IF EXISTS IDENTIFIER($WH_DASH);

-- ───────────────────────────────────────────────────────────────────
-- 4) DROP DATABASE (CASCADE nukes schema/tables/views/procs/etc.)
-- ───────────────────────────────────────────────────────────────────
DROP DATABASE IF EXISTS IDENTIFIER($DB);

-- ───────────────────────────────────────────────────────────────────
-- 5) DROP STORAGE INTEGRATION (after stages are gone)
-- ───────────────────────────────────────────────────────────────────
DROP INTEGRATION IF EXISTS IDENTIFIER($INTEGRATION_NAME);

-- ───────────────────────────────────────────────────────────────────
-- 6) DROP ROLE (after grants revoked)
-- ───────────────────────────────────────────────────────────────────
DROP ROLE IF EXISTS IDENTIFIER($ROLE_NAME);

-- ====================================================================
-- 🧰 REBUILD CLEAN ENVIRONMENT
-- ====================================================================

-- Warehouses
CREATE WAREHOUSE IF NOT EXISTS IDENTIFIER($WH_ELT)
  WAREHOUSE_SIZE = SMALL
  AUTO_SUSPEND   = 60
  AUTO_RESUME    = TRUE
  INITIALLY_SUSPENDED = TRUE
  COMMENT = 'Primary ELT compute for stocks/options ingestion & transforms';

CREATE WAREHOUSE IF NOT EXISTS IDENTIFIER($WH_DASH)
  WAREHOUSE_SIZE = XSMALL
  AUTO_SUSPEND   = 60
  AUTO_RESUME    = TRUE
  INITIALLY_SUSPENDED = TRUE
  COMMENT = 'BI/analytics/dashboard compute';

-- Database & Schema
CREATE DATABASE IF NOT EXISTS IDENTIFIER($DB) COMMENT = 'Dev database for stocks/options ELT';
CREATE SCHEMA   IF NOT EXISTS IDENTIFIER($DB)||'.'||IDENTIFIER($SCHEMA) COMMENT = 'Default schema for ELT objects';

-- Role
CREATE ROLE IF NOT EXISTS IDENTIFIER($ROLE_NAME) COMMENT = 'Service role for Airflow/dbt running the ELT';

-- Storage Integration
CREATE OR REPLACE STORAGE INTEGRATION IDENTIFIER($INTEGRATION_NAME)
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = S3
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = $S3_IAM_ROLE_ARN
  STORAGE_ALLOWED_LOCATIONS = ($S3_URL)
  COMMENT = 'Integration between Snowflake and S3 for ELT data (scoped to /raw folder)';

-- File Formats
EXECUTE IMMEDIATE
  'CREATE OR REPLACE FILE FORMAT ' || $DB || '.' || $SCHEMA || '.' || $FF_JSON || '
   TYPE = JSON
   STRIP_OUTER_ARRAY = TRUE
   COMMENT = ''Default JSON format for Polygon & other API payloads''';
EXECUTE IMMEDIATE
  'CREATE OR REPLACE FILE FORMAT ' || $DB || '.' || $SCHEMA || '.' || $FF_CSV || '
   TYPE = CSV
   FIELD_OPTIONALLY_ENCLOSED_BY = ''"'''
   || ' SKIP_HEADER = 1
   NULL_IF = ('''', ''NULL'', ''null'')
   EMPTY_FIELD_AS_NULL = TRUE
   COMMENT = ''CSV loader (headers in row 1)''';

-- Stage
EXECUTE IMMEDIATE
  'CREATE OR REPLACE STAGE ' || $DB || '.' || $SCHEMA || '.' || $STAGE_NAME || '
   STORAGE_INTEGRATION = ' || $INTEGRATION_NAME || '
   URL = ''' || $S3_URL || '''
   FILE_FORMAT = ' || $DB || '.' || $SCHEMA || '.' || $FF_JSON || '
   COMMENT = ''Stage for ELT raw data ingestion (restricted to /raw prefix)''';

-- Privileges
GRANT USAGE, OPERATE ON WAREHOUSE IDENTIFIER($WH_ELT)  TO ROLE IDENTIFIER($ROLE_NAME);
GRANT USAGE, OPERATE ON WAREHOUSE IDENTIFIER($WH_DASH) TO ROLE IDENTIFIER($ROLE_NAME);
GRANT MONITOR ON WAREHOUSE IDENTIFIER($WH_ELT)         TO ROLE IDENTIFIER($ROLE_NAME);
GRANT MONITOR ON WAREHOUSE IDENTIFIER($WH_DASH)        TO ROLE IDENTIFIER($ROLE_NAME);

GRANT USAGE         ON DATABASE IDENTIFIER($DB)        TO ROLE IDENTIFIER($ROLE_NAME);
GRANT CREATE SCHEMA ON DATABASE IDENTIFIER($DB)        TO ROLE IDENTIFIER($ROLE_NAME);
GRANT USAGE         ON SCHEMA   IDENTIFIER($DB)||'.'||IDENTIFIER($SCHEMA) TO ROLE IDENTIFIER($ROLE_NAME);

GRANT CREATE TABLE, CREATE VIEW, CREATE STAGE, CREATE FILE FORMAT,
      CREATE PIPE, CREATE FUNCTION, CREATE PROCEDURE
  ON SCHEMA IDENTIFIER($DB)||'.'||IDENTIFIER($SCHEMA) TO ROLE IDENTIFIER($ROLE_NAME);

GRANT USAGE ON INTEGRATION IDENTIFIER($INTEGRATION_NAME)                     TO ROLE IDENTIFIER($ROLE_NAME);
GRANT USAGE ON STAGE       IDENTIFIER($DB)||'.'||IDENTIFIER($SCHEMA)||'.'||IDENTIFIER($STAGE_NAME) TO ROLE IDENTIFIER($ROLE_NAME);
GRANT USAGE ON FILE FORMAT IDENTIFIER($DB)||'.'||IDENTIFIER($SCHEMA)||'.'||IDENTIFIER($FF_JSON)    TO ROLE IDENTIFIER($ROLE_NAME);
GRANT USAGE ON FILE FORMAT IDENTIFIER($DB)||'.'||IDENTIFIER($SCHEMA)||'.'||IDENTIFIER($FF_CSV)     TO ROLE IDENTIFIER($ROLE_NAME);

GRANT ALL PRIVILEGES ON SCHEMA IDENTIFIER($DB)||'.'||IDENTIFIER($SCHEMA) TO ROLE IDENTIFIER($ROLE_NAME);

GRANT SELECT, INSERT, UPDATE, DELETE ON FUTURE TABLES      IN SCHEMA IDENTIFIER($DB)||'.'||IDENTIFIER($SCHEMA) TO ROLE IDENTIFIER($ROLE_NAME);
GRANT SELECT                          ON FUTURE VIEWS       IN SCHEMA IDENTIFIER($DB)||'.'||IDENTIFIER($SCHEMA) TO ROLE IDENTIFIER($ROLE_NAME);
GRANT USAGE                           ON FUTURE STAGES      IN SCHEMA IDENTIFIER($DB)||'.'||IDENTIFIER($SCHEMA) TO ROLE IDENTIFIER($ROLE_NAME);
GRANT USAGE                           ON FUTURE FILE FORMATS IN SCHEMA IDENTIFIER($DB)||'.'||IDENTIFIER($SCHEMA) TO ROLE IDENTIFIER($ROLE_NAME);

-- Assign Role to your user
GRANT ROLE IDENTIFIER($ROLE_NAME) TO USER IDENTIFIER($PROJECT_USER);

-- (Optional) Defaults
-- ALTER USER IDENTIFIER($PROJECT_USER) SET DEFAULT_ROLE = IDENTIFIER($ROLE_NAME);
-- ALTER USER IDENTIFIER($PROJECT_USER) SET DEFAULT_WAREHOUSE = IDENTIFIER($WH_ELT);
-- ALTER USER IDENTIFIER($PROJECT_USER) SET DEFAULT_NAMESPACE = IDENTIFIER($DB)||'.'||IDENTIFIER($SCHEMA);
