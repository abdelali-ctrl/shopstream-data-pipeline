-- ============================================
-- ShopStream v2: COPY INTO from Azure Blob to RAW
-- ============================================
-- Loads CSVs produced by scripts/export_to_azure_blob.py into the RAW schema.
-- The placeholder {{ partition_date }} is replaced by Airflow / run_snowflake_copy_into.py.
--
-- Convention:
-- azure://<account>.blob.core.windows.net/<container>/raw/postgres/<table>/<YYYY-MM-DD>/<table>_<YYYYMMDD>.csv
-- Run scripts/snowflake_setup.sql first.
-- ============================================

USE WAREHOUSE LOADING_WH;
USE DATABASE SHOPSTREAM_DWH;
USE SCHEMA RAW;

-- 1. RAW_USERS
COPY INTO RAW_USERS (
    id, email, first_name, last_name, country, plan_type,
    created_at, last_login, is_active
)
FROM @RAW.azure_raw_stage/postgres/users/{{ partition_date }}/
FILE_FORMAT = (FORMAT_NAME = 'RAW.csv_format')
ON_ERROR = 'ABORT_STATEMENT';

-- 2. RAW_PRODUCTS
COPY INTO RAW_PRODUCTS (
    id, merchant_id, name, description, category, price,
    stock_quantity, created_at, updated_at
)
FROM @RAW.azure_raw_stage/postgres/products/{{ partition_date }}/
FILE_FORMAT = (FORMAT_NAME = 'RAW.csv_format')
ON_ERROR = 'ABORT_STATEMENT';

-- 3. RAW_ORDERS
COPY INTO RAW_ORDERS (
    id, user_id, created_at, total_amount, status, country, payment_method
)
FROM @RAW.azure_raw_stage/postgres/orders/{{ partition_date }}/
FILE_FORMAT = (FORMAT_NAME = 'RAW.csv_format')
ON_ERROR = 'ABORT_STATEMENT';

-- 4. RAW_ORDER_ITEMS
COPY INTO RAW_ORDER_ITEMS (
    id, order_id, product_id, quantity, unit_price, line_total
)
FROM @RAW.azure_raw_stage/postgres/order_items/{{ partition_date }}/
FILE_FORMAT = (FORMAT_NAME = 'RAW.csv_format')
ON_ERROR = 'ABORT_STATEMENT';

SELECT 'RAW_USERS' AS table_name, COUNT(*) AS row_count FROM RAW_USERS
UNION ALL
SELECT 'RAW_PRODUCTS', COUNT(*) FROM RAW_PRODUCTS
UNION ALL
SELECT 'RAW_ORDERS', COUNT(*) FROM RAW_ORDERS
UNION ALL
SELECT 'RAW_ORDER_ITEMS', COUNT(*) FROM RAW_ORDER_ITEMS;
