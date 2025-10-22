-- ====================================================================
-- Create Airflow Service User (key-pair auth) — Focused / Idempotent
-- ====================================================================
-- Assumptions:
-- - ROLE: STOCKS_ELT_ROLE already exists (grants handled elsewhere)
-- - WAREHOUSE: STOCKS_ELT_WH exists
-- - DB/Schema: STOCKS_ELT_DB.PUBLIC exists
-- - Privileges for schemas are managed by separate grants scripts
-- ====================================================================

USE ROLE ACCOUNTADMIN;

-- 1) Create user (no password; intended for key-pair auth)
CREATE USER IF NOT EXISTS airflow_stocks_user
  DEFAULT_ROLE         = STOCKS_ELT_ROLE
  DEFAULT_WAREHOUSE    = STOCKS_ELT_WH
  DEFAULT_NAMESPACE    = STOCKS_ELT_DB.PUBLIC
  MUST_CHANGE_PASSWORD = FALSE
  COMMENT              = 'Airflow service user for key-pair auth';

-- 2) Attach role to user
GRANT ROLE STOCKS_ELT_ROLE TO USER airflow_stocks_user;

-- 3) (Optional) Set RSA public key for key-pair auth
--    Replace <BASE64_PUBLIC_KEY> with the actual Base64-encoded public key
--    from your /usr/local/airflow/keys/rsa_key.pub (no headers/footers).
-- ALTER USER airflow_stocks_user SET RSA_PUBLIC_KEY = '<BASE64_PUBLIC_KEY>';

--    (Optional rotation path — keep old key valid during rotation)
-- ALTER USER airflow_stocks_user SET RSA_PUBLIC_KEY_2 = '<BASE64_PUBLIC_KEY>';
-- -- swap when ready:
-- ALTER USER airflow_stocks_user UNSET RSA_PUBLIC_KEY;
-- ALTER USER airflow_stocks_user SET RSA_PUBLIC_KEY = '<BASE64_PUBLIC_KEY>';
-- ALTER USER airflow_stocks_user UNSET RSA_PUBLIC_KEY_2;

-- 4) Quick sanity
SHOW GRANTS TO USER airflow_stocks_user;
DESCRIBE USER airflow_stocks_user;
