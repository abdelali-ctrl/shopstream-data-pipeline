-- ============================================
-- ShopStream: COPY INTO commands for Snowflake
-- Load data from S3 to Staging tables
-- ============================================
-- Run these commands in Snowflake Worksheet
-- after setting up the S3 integration and stage
-- ============================================

USE WAREHOUSE LOADING_WH;
USE DATABASE SHOPSTREAM_DWH;
USE SCHEMA STAGING;

-- ============================================
-- 1. COPY USERS
-- ============================================
COPY INTO stg_users (id, email, first_name, last_name, country, plan_type, created_at, last_login, is_active)
FROM @RAW.s3_raw_stage/postgres/users/2025-12-16/
FILE_FORMAT = (
    TYPE = CSV 
    FIELD_OPTIONALLY_ENCLOSED_BY = '"' 
    SKIP_HEADER = 1
    NULL_IF = ('', 'NULL')
)
ON_ERROR = 'CONTINUE';

-- ============================================
-- 2. COPY PRODUCTS
-- ============================================
COPY INTO stg_products (id, merchant_id, name, description, category, price, stock_quantity, created_at, updated_at)
FROM @RAW.s3_raw_stage/postgres/products/2025-12-16/
FILE_FORMAT = (
    TYPE = CSV 
    FIELD_OPTIONALLY_ENCLOSED_BY = '"' 
    SKIP_HEADER = 1
    NULL_IF = ('', 'NULL')
)
ON_ERROR = 'CONTINUE';

-- ============================================
-- 3. COPY ORDERS
-- ============================================
COPY INTO stg_orders (id, user_id, created_at, total_amount, status, country, payment_method)
FROM @RAW.s3_raw_stage/postgres/orders/2025-12-16/
FILE_FORMAT = (
    TYPE = CSV 
    FIELD_OPTIONALLY_ENCLOSED_BY = '"' 
    SKIP_HEADER = 1
    NULL_IF = ('', 'NULL')
)
ON_ERROR = 'CONTINUE';

-- ============================================
-- 4. COPY ORDER_ITEMS
-- ============================================
COPY INTO stg_order_items (id, order_id, product_id, quantity, unit_price, line_total)
FROM @RAW.s3_raw_stage/postgres/order_items/2025-12-16/
FILE_FORMAT = (
    TYPE = CSV 
    FIELD_OPTIONALLY_ENCLOSED_BY = '"' 
    SKIP_HEADER = 1
    NULL_IF = ('', 'NULL')
)
ON_ERROR = 'CONTINUE';

-- ============================================
-- 5. COPY CRM_CONTACTS
-- ============================================
COPY INTO stg_crm_contacts (id, email, first_name, last_name, source, campaign_id, created_at, converted, converted_at)
FROM @RAW.s3_raw_stage/postgres/crm_contacts/2025-12-16/
FILE_FORMAT = (
    TYPE = CSV 
    FIELD_OPTIONALLY_ENCLOSED_BY = '"' 
    SKIP_HEADER = 1
    NULL_IF = ('', 'NULL')
)
ON_ERROR = 'CONTINUE';

-- ============================================
-- 6. COPY EVENTS (JSON format)
-- ============================================
COPY INTO stg_events (id, user_id, event_type, event_ts, metadata)
FROM @RAW.s3_raw_stage/events/2025-12-16/
FILE_FORMAT = (
    TYPE = JSON
)
ON_ERROR = 'CONTINUE';

-- ============================================
-- VERIFY DATA LOADED
-- ============================================
SELECT 'stg_users' AS table_name, COUNT(*) AS row_count FROM stg_users
UNION ALL
SELECT 'stg_products', COUNT(*) FROM stg_products
UNION ALL
SELECT 'stg_orders', COUNT(*) FROM stg_orders
UNION ALL
SELECT 'stg_order_items', COUNT(*) FROM stg_order_items
UNION ALL
SELECT 'stg_crm_contacts', COUNT(*) FROM stg_crm_contacts
UNION ALL
SELECT 'stg_events', COUNT(*) FROM stg_events;
