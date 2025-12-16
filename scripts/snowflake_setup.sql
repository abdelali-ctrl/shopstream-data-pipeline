-- ============================================
-- ShopStream: Complete Snowflake Setup
-- Run this script in Snowflake Worksheet
-- ============================================
-- This script creates all necessary resources:
-- 1. Database and schemas
-- 2. Warehouses
-- 3. Staging tables
-- 4. S3 integration and external stage
-- ============================================

-- ============================================
-- STEP 1: CREATE DATABASE & SCHEMAS
-- ============================================

-- Create the main data warehouse database
CREATE DATABASE IF NOT EXISTS SHOPSTREAM_DWH;

USE DATABASE SHOPSTREAM_DWH;

-- Create schemas for each layer
CREATE SCHEMA IF NOT EXISTS RAW;           -- External stage & raw files
CREATE SCHEMA IF NOT EXISTS STAGING;       -- Staging tables (loaded from S3)
CREATE SCHEMA IF NOT EXISTS CORE;          -- Dimensions and Facts (dbt)
CREATE SCHEMA IF NOT EXISTS MARTS;         -- Business-ready data marts (dbt)

-- ============================================
-- STEP 2: CREATE WAREHOUSES
-- ============================================

-- Warehouse for loading data (ETL)
CREATE WAREHOUSE IF NOT EXISTS LOADING_WH
    WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Warehouse for ETL and data loading';

-- Warehouse for dbt transformations
CREATE WAREHOUSE IF NOT EXISTS TRANSFORM_WH
    WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Warehouse for dbt transformations';

-- Warehouse for BI tools (Power BI)
CREATE WAREHOUSE IF NOT EXISTS BI_WH
    WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Warehouse for Power BI queries';

-- ============================================
-- STEP 3: CREATE STAGING TABLES
-- ============================================

USE SCHEMA STAGING;

-- Users staging table
CREATE OR REPLACE TABLE STG_USERS (
    id INT,
    email VARCHAR(255),
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    country VARCHAR(3),
    plan_type VARCHAR(20),
    created_at TIMESTAMP,
    last_login TIMESTAMP,
    is_active BOOLEAN,
    _loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- Products staging table
CREATE OR REPLACE TABLE STG_PRODUCTS (
    id INT,
    merchant_id INT,
    name VARCHAR(255),
    description TEXT,
    category VARCHAR(100),
    price DECIMAL(10,2),
    stock_quantity INT,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    _loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- Orders staging table
CREATE OR REPLACE TABLE STG_ORDERS (
    id INT,
    user_id INT,
    created_at TIMESTAMP,
    total_amount DECIMAL(10,2),
    status VARCHAR(20),
    country VARCHAR(3),
    payment_method VARCHAR(50),
    _loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- Order Items staging table
CREATE OR REPLACE TABLE STG_ORDER_ITEMS (
    id INT,
    order_id INT,
    product_id INT,
    quantity INT,
    unit_price DECIMAL(10,2),
    line_total DECIMAL(10,2),
    _loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- CRM Contacts staging table
CREATE OR REPLACE TABLE STG_CRM_CONTACTS (
    id INT,
    email VARCHAR(255),
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    source VARCHAR(100),
    campaign_id VARCHAR(100),
    created_at TIMESTAMP,
    converted BOOLEAN,
    converted_at TIMESTAMP,
    _loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- Events staging table (JSON)
CREATE OR REPLACE TABLE STG_EVENTS (
    id BIGINT,
    user_id INT,
    event_type VARCHAR(50),
    event_ts TIMESTAMP,
    metadata VARIANT,
    _loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- ============================================
-- STEP 4: CREATE S3 STORAGE INTEGRATION
-- ============================================
-- NOTE: Replace YOUR_AWS_ACCOUNT_ID and YOUR_ROLE_ARN with your actual values

/*
-- Create storage integration for S3
CREATE OR REPLACE STORAGE INTEGRATION s3_shopstream_integration
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = 'S3'
    STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::YOUR_AWS_ACCOUNT_ID:role/snowflake-s3-role'
    ENABLED = TRUE
    STORAGE_ALLOWED_LOCATIONS = ('s3://shopstream-datalake-abdelali/');

-- Describe integration to get Snowflake AWS IAM user ARN
-- (Use this to configure trust relationship in AWS)
DESC INTEGRATION s3_shopstream_integration;
*/

-- ============================================
-- STEP 5: CREATE EXTERNAL STAGE
-- ============================================

USE SCHEMA RAW;

/*
-- Create external stage pointing to S3
CREATE OR REPLACE STAGE s3_raw_stage
    STORAGE_INTEGRATION = s3_shopstream_integration
    URL = 's3://shopstream-datalake-abdelali/raw/'
    FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

-- List files in the stage
LIST @s3_raw_stage;
*/

-- ============================================
-- STEP 6: CREATE FILE FORMATS
-- ============================================

-- CSV file format for most tables
CREATE OR REPLACE FILE FORMAT csv_format
    TYPE = CSV
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    SKIP_HEADER = 1
    NULL_IF = ('', 'NULL', 'null')
    EMPTY_FIELD_AS_NULL = TRUE;

-- JSON file format for events
CREATE OR REPLACE FILE FORMAT json_format
    TYPE = JSON
    STRIP_OUTER_ARRAY = TRUE;

-- ============================================
-- STEP 7: GRANT PERMISSIONS (if needed)
-- ============================================

/*
-- Grant usage on database and schemas
GRANT USAGE ON DATABASE SHOPSTREAM_DWH TO ROLE YOUR_ROLE;
GRANT USAGE ON ALL SCHEMAS IN DATABASE SHOPSTREAM_DWH TO ROLE YOUR_ROLE;
GRANT SELECT ON ALL TABLES IN SCHEMA SHOPSTREAM_DWH.STAGING TO ROLE YOUR_ROLE;
GRANT SELECT ON ALL TABLES IN SCHEMA SHOPSTREAM_DWH.CORE TO ROLE YOUR_ROLE;
GRANT SELECT ON ALL TABLES IN SCHEMA SHOPSTREAM_DWH.MARTS TO ROLE YOUR_ROLE;
*/

-- ============================================
-- VERIFICATION
-- ============================================

-- Show all objects created
SHOW SCHEMAS IN DATABASE SHOPSTREAM_DWH;
SHOW TABLES IN SCHEMA STAGING;
SHOW WAREHOUSES LIKE '%WH';
SHOW FILE FORMATS IN SCHEMA RAW;

-- Print success message
SELECT 'Snowflake setup completed successfully!' AS status;
