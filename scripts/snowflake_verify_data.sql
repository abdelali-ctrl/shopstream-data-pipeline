-- ============================================
-- ShopStream v2: Data Verification
-- ============================================
-- Smoke-test queries to confirm each layer is populated and consistent
-- after a full pipeline run. Safe to re-run any time.
-- ============================================

USE DATABASE SHOPSTREAM_DWH;

-- ============================================
-- 1. RAW (loaded from Azure Blob)
-- ============================================
SELECT '=== RAW LAYER ===' AS section;

SELECT 'RAW.RAW_USERS'        AS table_name, COUNT(*) AS row_count FROM RAW.RAW_USERS
UNION ALL
SELECT 'RAW.RAW_PRODUCTS',    COUNT(*) FROM RAW.RAW_PRODUCTS
UNION ALL
SELECT 'RAW.RAW_ORDERS',      COUNT(*) FROM RAW.RAW_ORDERS
UNION ALL
SELECT 'RAW.RAW_ORDER_ITEMS', COUNT(*) FROM RAW.RAW_ORDER_ITEMS;

-- ============================================
-- 2. STAGING (dbt views: cleaning, typing, renaming)
-- ============================================
SELECT '=== STAGING LAYER ===' AS section;

SELECT 'STAGING.STG_USERS'        AS table_name, COUNT(*) AS row_count FROM STAGING.STG_USERS
UNION ALL
SELECT 'STAGING.STG_PRODUCTS',    COUNT(*) FROM STAGING.STG_PRODUCTS
UNION ALL
SELECT 'STAGING.STG_ORDERS',      COUNT(*) FROM STAGING.STG_ORDERS
UNION ALL
SELECT 'STAGING.STG_ORDER_ITEMS', COUNT(*) FROM STAGING.STG_ORDER_ITEMS;

-- ============================================
-- 3. CORE (dimensions and facts)
-- ============================================
SELECT '=== CORE LAYER ===' AS section;

SELECT
    'CORE.DIM_CUSTOMERS' AS table_name,
    COUNT(*)                       AS total_rows,
    COUNT(DISTINCT customer_key)   AS unique_customers,
    COUNT(DISTINCT country_code)   AS countries
FROM CORE.DIM_CUSTOMERS;

SELECT
    'CORE.DIM_PRODUCTS' AS table_name,
    COUNT(*)                         AS total_rows,
    COUNT(DISTINCT product_key)      AS unique_products,
    COUNT(DISTINCT product_category) AS categories
FROM CORE.DIM_PRODUCTS;

SELECT
    'CORE.FACT_ORDERS' AS table_name,
    COUNT(*)                    AS total_rows,
    COUNT(DISTINCT order_key)   AS unique_orders,
    SUM(line_revenue)           AS total_revenue
FROM CORE.FACT_ORDERS;

-- ============================================
-- 4. MARTS (business-ready)
-- ============================================
SELECT '=== MARTS LAYER ===' AS section;

SELECT
    'MARTS.MART_SALES_OVERVIEW' AS table_name,
    COUNT(*)              AS total_rows,
    SUM(total_revenue)    AS grand_total_revenue,
    SUM(total_orders)     AS grand_total_orders
FROM MARTS.MART_SALES_OVERVIEW;

SELECT
    'MARTS.MART_CUSTOMER_LTV' AS table_name,
    COUNT(*)                       AS total_customers,
    AVG(lifetime_value)            AS avg_ltv,
    COUNT(DISTINCT rfm_segment)    AS rfm_segments
FROM MARTS.MART_CUSTOMER_LTV;

SELECT
    'MARTS.MART_PRODUCT_PERFORMANCE' AS table_name,
    COUNT(*)                      AS total_products,
    SUM(total_revenue)            AS total_product_revenue,
    COUNT(DISTINCT abc_class)     AS abc_classes
FROM MARTS.MART_PRODUCT_PERFORMANCE;

-- ============================================
-- 5. SAMPLE BUSINESS QUERIES
-- ============================================

SELECT '=== TOP 5 CUSTOMERS BY LTV ===' AS section;
SELECT
    full_name,
    email,
    lifetime_value,
    rfm_segment,
    churn_risk
FROM MARTS.MART_CUSTOMER_LTV
ORDER BY lifetime_value DESC
LIMIT 5;

SELECT '=== TOP 5 PRODUCTS BY REVENUE ===' AS section;
SELECT
    product_name,
    product_category,
    total_revenue,
    abc_class,
    revenue_rank
FROM MARTS.MART_PRODUCT_PERFORMANCE
ORDER BY revenue_rank
LIMIT 5;

SELECT '=== REVENUE BY COUNTRY ===' AS section;
SELECT
    country_code,
    SUM(total_orders)  AS orders,
    SUM(total_revenue) AS revenue
FROM MARTS.MART_SALES_OVERVIEW
GROUP BY country_code
ORDER BY revenue DESC;

-- ============================================
-- 6. CONSISTENCY CHECKS (should all return 0 rows)
-- ============================================
SELECT '=== CONSISTENCY CHECKS ===' AS section;

-- Orphan facts: order line referencing a missing customer
SELECT 'Orphan customer in FACT_ORDERS' AS check_name, COUNT(*) AS bad_rows
FROM CORE.FACT_ORDERS f
LEFT JOIN CORE.DIM_CUSTOMERS c ON f.customer_key = c.customer_key
WHERE c.customer_key IS NULL;

-- Orphan facts: order line referencing a missing product
SELECT 'Orphan product in FACT_ORDERS' AS check_name, COUNT(*) AS bad_rows
FROM CORE.FACT_ORDERS f
LEFT JOIN CORE.DIM_PRODUCTS p ON f.product_key = p.product_key
WHERE p.product_key IS NULL;

-- Negative or zero revenue
SELECT 'Non-positive line_revenue in FACT_ORDERS' AS check_name, COUNT(*) AS bad_rows
FROM CORE.FACT_ORDERS
WHERE line_revenue <= 0;
