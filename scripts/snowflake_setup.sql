-- ============================================
-- ShopStream v2: Snowflake Setup
-- Run this script in a Snowflake worksheet.
-- ============================================
-- Creates:
--   1. Database (SHOPSTREAM_DWH) and schemas (RAW, STAGING, CORE, MARTS)
--   2. Three warehouses (LOADING_WH, TRANSFORM_WH, BI_WH)
--   3. Raw landing tables (loaded from Azure Blob)
--   4. Azure Blob external stage template
--   5. CSV file format
-- ============================================
-- v2 changes vs v1:
--   - Schema "STAGING" renamed to "RAW" so "staging" is reserved for dbt models.
--   - Tables prefixed RAW_ instead of STG_ (see ADR-001).
--   - events / crm_contacts removed from v2.0 scope (see CHANGELOG / ADR-003).
-- ============================================

-- ============================================
-- STEP 1: DATABASE & SCHEMAS
-- ============================================

CREATE DATABASE IF NOT EXISTS SHOPSTREAM_DWH;
USE DATABASE SHOPSTREAM_DWH;

CREATE SCHEMA IF NOT EXISTS RAW;       -- Landing zone: loaded from Azure Blob by COPY INTO
CREATE SCHEMA IF NOT EXISTS STAGING;   -- dbt staging models (views): cleaning, typing, renaming
CREATE SCHEMA IF NOT EXISTS CORE;      -- dbt dimensions and facts
CREATE SCHEMA IF NOT EXISTS MARTS;     -- dbt business-ready data marts

-- ============================================
-- STEP 2: WAREHOUSES
-- ============================================
-- All XSMALL with auto-suspend at 60s to minimize idle credits.

CREATE WAREHOUSE IF NOT EXISTS LOADING_WH
    WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'COPY INTO from Azure Blob to RAW';

CREATE WAREHOUSE IF NOT EXISTS TRANSFORM_WH
    WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'dbt build (staging/core/marts)';

CREATE WAREHOUSE IF NOT EXISTS BI_WH
    WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Power BI queries against MARTS';

-- ============================================
-- STEP 3: RAW LANDING TABLES
-- ============================================
-- These mirror the PostgreSQL source schema 1:1, plus a _loaded_at audit column.
-- Type widening is intentional: the warehouse should not fail on source schema
-- drift; data quality is enforced downstream by dbt tests.

USE SCHEMA RAW;

CREATE OR REPLACE TABLE RAW_USERS (
    id              INT,
    email           VARCHAR(255),
    first_name      VARCHAR(100),
    last_name       VARCHAR(100),
    country         VARCHAR(3),
    plan_type       VARCHAR(20),
    created_at      TIMESTAMP,
    last_login      TIMESTAMP,
    is_active       BOOLEAN,
    _loaded_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE RAW_PRODUCTS (
    id              INT,
    merchant_id     INT,
    name            VARCHAR(255),
    description     TEXT,
    category        VARCHAR(100),
    price           DECIMAL(10,2),
    stock_quantity  INT,
    created_at      TIMESTAMP,
    updated_at      TIMESTAMP,
    _loaded_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE RAW_ORDERS (
    id              INT,
    user_id         INT,
    created_at      TIMESTAMP,
    total_amount    DECIMAL(10,2),
    status          VARCHAR(20),
    country         VARCHAR(3),
    payment_method  VARCHAR(50),
    _loaded_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE RAW_ORDER_ITEMS (
    id          INT,
    order_id    INT,
    product_id  INT,
    quantity    INT,
    unit_price  DECIMAL(10,2),
    line_total  DECIMAL(10,2),
    _loaded_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- ============================================
-- STEP 4: FILE FORMAT
-- ============================================

CREATE OR REPLACE FILE FORMAT RAW.csv_format
    TYPE = CSV
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    SKIP_HEADER = 1
    NULL_IF = ('', 'NULL', 'null')
    EMPTY_FIELD_AS_NULL = TRUE;

-- ============================================
-- STEP 5: AZURE BLOB EXTERNAL STAGE (template)
-- ============================================
-- This demo uses a SAS token because it is simple to set up.
-- For production, prefer a Snowflake STORAGE INTEGRATION with Azure service principal
-- / external volume style governance instead of embedding credentials in SQL.
--
-- Replace:
--   <storage_account> with your Azure Storage Account name
--   <container>       with your Blob container name
--   <sas_token>       with a SAS token that can read/list the container

/*
CREATE OR REPLACE STAGE RAW.azure_raw_stage
    URL = 'azure://<storage_account>.blob.core.windows.net/<container>/raw/'
    CREDENTIALS = (AZURE_SAS_TOKEN = '<sas_token>')
    FILE_FORMAT = RAW.csv_format;

LIST @RAW.azure_raw_stage;
*/

-- ============================================
-- STEP 6: GRANTS (template)
-- ============================================

/*
GRANT USAGE ON DATABASE SHOPSTREAM_DWH TO ROLE YOUR_ROLE;
GRANT USAGE ON ALL SCHEMAS IN DATABASE SHOPSTREAM_DWH TO ROLE YOUR_ROLE;
GRANT SELECT ON ALL TABLES   IN SCHEMA RAW     TO ROLE YOUR_ROLE;
GRANT SELECT ON ALL VIEWS    IN SCHEMA STAGING TO ROLE YOUR_ROLE;
GRANT SELECT ON ALL TABLES   IN SCHEMA CORE    TO ROLE YOUR_ROLE;
GRANT SELECT ON ALL TABLES   IN SCHEMA MARTS   TO ROLE YOUR_ROLE;
*/

-- ============================================
-- VERIFICATION
-- ============================================

SHOW SCHEMAS IN DATABASE SHOPSTREAM_DWH;
SHOW TABLES IN SCHEMA RAW;
SHOW WAREHOUSES LIKE '%WH';

SELECT 'Snowflake setup complete.' AS status;
