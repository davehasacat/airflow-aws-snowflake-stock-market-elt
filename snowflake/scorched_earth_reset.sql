-- ==========================================================
-- TRUE NUKE: Destroy STOCKS_ELT_DB environment (idempotent)
-- Requires: ROLE = ACCOUNTADMIN
-- ==========================================================

USE ROLE ACCOUNTADMIN;

-- Ensure we have a safe, tiny maintenance warehouse to run from.
-- If it already exists this is a no-op.
CREATE WAREHOUSE IF NOT EXISTS ADMIN_MAINT_WH
  WAREHOUSE_SIZE = XSMALL
  AUTO_SUSPEND   = 60
  AUTO_RESUME    = TRUE
  INITIALLY_SUSPENDED = TRUE
  COMMENT = 'Temporary admin warehouse for destructive ops';

-- Use maintenance warehouse so we are not using the target warehouses when dropping them.
USE WAREHOUSE ADMIN_MAINT_WH;

-- Variables (edit these if your object names differ)
DECLARE
  DB_NAME          STRING DEFAULT 'STOCKS_ELT_DB';
  ROLE_NAME        STRING DEFAULT 'STOCKS_ELT_ROLE';
  WH_ELT           STRING DEFAULT 'STOCKS_ELT_WH';
  WH_DASH          STRING DEFAULT 'STOCKS_DASHBOARD_WH';
  MAINT_WH         STRING DEFAULT 'ADMIN_MAINT_WH';
  INTEGRATION_NAME STRING DEFAULT 'S3_INTEGRATION';
  STAGE_NAME       STRING DEFAULT 'S3_STAGE';
  FF_JSON          STRING DEFAULT 'FF_JSON';
  FF_CSV           STRING DEFAULT 'FF_CSV';
  SERVICE_USER     STRING DEFAULT 'AIRFLOW_STOCKS_USER';
  HUMAN_USER       STRING DEFAULT 'DAVEHASACAT';
BEGIN
  -- -----------------------
  -- 1) Revoke role from users (best-effort)
  -- -----------------------
  BEGIN
    EXECUTE IMMEDIATE 'REVOKE ROLE ' || ROLE_NAME || ' FROM USER ' || SERVICE_USER;
  EXCEPTION WHEN STATEMENT_ERROR THEN
    NULL;
  END;

  BEGIN
    EXECUTE IMMEDIATE 'REVOKE ROLE ' || ROLE_NAME || ' FROM USER ' || HUMAN_USER;
  EXCEPTION WHEN STATEMENT_ERROR THEN
    NULL;
  END;

  -- -----------------------
  -- 2) Suspend other warehouses (best-effort), then drop them
  --    We are running on MAINT_WH, so altering/dropping target WHs is safe.
  -- -----------------------
  BEGIN
    EXECUTE IMMEDIATE 'ALTER WAREHOUSE ' || WH_ELT || ' SUSPEND';
  EXCEPTION WHEN STATEMENT_ERROR THEN NULL; END;
  BEGIN
    EXECUTE IMMEDIATE 'DROP WAREHOUSE IF EXISTS ' || WH_ELT;
  EXCEPTION WHEN STATEMENT_ERROR THEN NULL; END;

  BEGIN
    EXECUTE IMMEDIATE 'ALTER WAREHOUSE ' || WH_DASH || ' SUSPEND';
  EXCEPTION WHEN STATEMENT_ERROR THEN NULL; END;
  BEGIN
    EXECUTE IMMEDIATE 'DROP WAREHOUSE IF EXISTS ' || WH_DASH;
  EXCEPTION WHEN STATEMENT_ERROR THEN NULL; END;

  -- -----------------------
  -- 3) If DB exists, switch into it and try to drop infra objects that require DB context.
  --    Guarded so compile-time errors do not abort the entire script.
  -- -----------------------
  BEGIN
    -- Try to set context to the DB; if DB doesn't exist, this block will be skipped by the exception handler
    EXECUTE IMMEDIATE 'USE DATABASE ' || DB_NAME;
    -- Attempt to drop stage and file formats in PUBLIC schema (unqualified now)
    BEGIN
      EXECUTE IMMEDIATE 'USE SCHEMA ' || DB_NAME || '.PUBLIC';
      EXECUTE IMMEDIATE 'DROP STAGE IF EXISTS ' || STAGE_NAME;
      EXECUTE IMMEDIATE 'DROP FILE FORMAT IF EXISTS ' || FF_JSON;
      EXECUTE IMMEDIATE 'DROP FILE FORMAT IF EXISTS ' || FF_CSV;
    EXCEPTION WHEN STATEMENT_ERROR THEN
      -- if schema or objects missing, ignore and continue
      NULL;
    END;
  EXCEPTION WHEN STATEMENT_ERROR THEN
    -- DB does not exist or cannot be used; ignore and continue to global DROPs
    NULL;
  END;

  -- -----------------------
  -- 4) Drop the database (this cascades schemas/tables/views/stages/objects under it).
  --    DROP DATABASE IF EXISTS is safe even if DB is missing.
  -- -----------------------
  BEGIN
    EXECUTE IMMEDIATE 'DROP DATABASE IF EXISTS ' || DB_NAME;
  EXCEPTION WHEN STATEMENT_ERROR THEN
    NULL;
  END;

  -- -----------------------
  -- 5) Drop integration & any remaining account-scoped objects
  -- -----------------------
  BEGIN
    EXECUTE IMMEDIATE 'DROP INTEGRATION IF EXISTS ' || INTEGRATION_NAME;
  EXCEPTION WHEN STATEMENT_ERROR THEN
    NULL;
  END;

  -- -----------------------
  -- 6) Revoke, drop service + human users (best-effort)
  -- -----------------------
  BEGIN
    EXECUTE IMMEDIATE 'DROP USER IF EXISTS ' || SERVICE_USER;
  EXCEPTION WHEN STATEMENT_ERROR THEN NULL; END;

  BEGIN
    EXECUTE IMMEDIATE 'DROP USER IF EXISTS ' || HUMAN_USER;
  EXCEPTION WHEN STATEMENT_ERROR THEN NULL; END;

  -- -----------------------
  -- 7) Drop role (best-effort)
  -- -----------------------
  BEGIN
    EXECUTE IMMEDIATE 'DROP ROLE IF EXISTS ' || ROLE_NAME;
  EXCEPTION WHEN STATEMENT_ERROR THEN NULL; END;

  -- -----------------------
  -- 8) Clean up the maintenance warehouse (optional): suspend & drop it
  --    We drop it last so the script can use it until the very end.
  -- -----------------------
  BEGIN EXECUTE IMMEDIATE 'ALTER WAREHOUSE ' || MAINT_WH || ' SUSPEND'; EXCEPTION WHEN STATEMENT_ERROR THEN NULL; END;
  BEGIN EXECUTE IMMEDIATE 'DROP WAREHOUSE IF EXISTS ' || MAINT_WH; EXCEPTION WHEN STATEMENT_ERROR THEN NULL; END;

END;
