-- ============================================================================
-- reset_raw_tables.sql
-- Description: Safely reset Snowflake raw/staging tables for re-ingestion tests
-- Author: Dave Anaya
-- ============================================================================

USE ROLE STOCKS_ELT_ROLE;
USE DATABASE STOCKS_ELT_DB;
USE SCHEMA PUBLIC;

-- ----------------------------
-- Truncate main raw tables
-- ----------------------------
BEGIN
    EXECUTE IMMEDIATE $$
        BEGIN
            IF (EXISTS (SELECT 1
                        FROM INFORMATION_SCHEMA.TABLES
                        WHERE TABLE_SCHEMA = 'PUBLIC'
                          AND TABLE_NAME = 'SOURCE_POLYGON_OPTIONS_BARS_DAILY')) THEN
                TRUNCATE TABLE PUBLIC.SOURCE_POLYGON_OPTIONS_BARS_DAILY;
            END IF;
        END;
    $$;
END;

-- ----------------------------
-- Optional: clear other raw tables if needed
-- ----------------------------
-- BEGIN
--     EXECUTE IMMEDIATE $$
--         BEGIN
--             IF (EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES
--                         WHERE TABLE_SCHEMA = 'PUBLIC'
--                           AND TABLE_NAME = 'SOURCE_POLYGON_STOCK_BARS_DAILY')) THEN
--                 TRUNCATE TABLE PUBLIC.SOURCE_POLYGON_STOCK_BARS_DAILY;
--             END IF;
--         END;
--     $$;
-- END;

-- Done
SELECT '✅ Snowflake raw tables reset complete.' AS status;
