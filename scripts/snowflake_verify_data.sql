-- ============================================
-- ShopStream: Data Verification Queries
-- Run in Snowflake Worksheet to verify data
-- ============================================

USE DATABASE SHOPSTREAM_DWH;

-- ============================================
-- 1. CHECK STAGING TABLES (Raw Data from S3)
-- ============================================
SELECT '=== STAGING TABLES ===' AS section;

SELECT 
    'STAGING.STG_USERS' AS table_name, 
    COUNT(*) AS row_count 
FROM STAGING.STG_USERS
UNION ALL
SELECT 'STAGING.STG_PRODUCTS', COUNT(*) FROM STAGING.STG_PRODUCTS
UNION ALL
SELECT 'STAGING.STG_ORDERS', COUNT(*) FROM STAGING.STG_ORDERS
UNION ALL
SELECT 'STAGING.STG_ORDER_ITEMS', COUNT(*) FROM STAGING.STG_ORDER_ITEMS
UNION ALL
SELECT 'STAGING.STG_CRM_CONTACTS', COUNT(*) FROM STAGING.STG_CRM_CONTACTS;

-- ============================================
-- 2. CHECK CORE TABLES (dbt Transformations)
-- ============================================
SELECT '=== CORE TABLES (dbt) ===' AS section;

-- Check dim_customers
SELECT 
    'CORE_core.DIM_CUSTOMERS' AS table_name,
    COUNT(*) AS total_rows,
    COUNT(DISTINCT customer_key) AS unique_customers,
    COUNT(DISTINCT country_code) AS countries
FROM CORE_core.DIM_CUSTOMERS;

-- Check dim_products
SELECT 
    'CORE_core.DIM_PRODUCTS' AS table_name,
    COUNT(*) AS total_rows,
    COUNT(DISTINCT product_key) AS unique_products,
    COUNT(DISTINCT product_category) AS categories
FROM CORE_core.DIM_PRODUCTS;

-- Check fact_orders
SELECT 
    'CORE_core.FACT_ORDERS' AS table_name,
    COUNT(*) AS total_rows,
    COUNT(DISTINCT order_key) AS unique_orders,
    SUM(line_revenue) AS total_revenue
FROM CORE_core.FACT_ORDERS;

-- ============================================
-- 3. CHECK DATA MARTS (Business-Ready Tables)
-- ============================================
SELECT '=== DATA MARTS ===' AS section;

-- Sales Overview
SELECT 
    'CORE_marts.MART_SALES_OVERVIEW' AS table_name,
    COUNT(*) AS total_rows,
    SUM(total_revenue) AS grand_total_revenue,
    SUM(total_orders) AS grand_total_orders
FROM CORE_marts.MART_SALES_OVERVIEW;

-- Customer LTV
SELECT 
    'CORE_marts.MART_CUSTOMER_LTV' AS table_name,
    COUNT(*) AS total_customers,
    AVG(lifetime_value) AS avg_ltv,
    COUNT(DISTINCT rfm_segment) AS rfm_segments
FROM CORE_marts.MART_CUSTOMER_LTV;

-- Product Performance
SELECT 
    'CORE_marts.MART_PRODUCT_PERFORMANCE' AS table_name,
    COUNT(*) AS total_products,
    SUM(total_revenue) AS total_product_revenue,
    COUNT(DISTINCT abc_class) AS abc_classes
FROM CORE_marts.MART_PRODUCT_PERFORMANCE;

-- ============================================
-- 4. SAMPLE DATA FROM EACH MART
-- ============================================

-- Top 5 customers by LTV
SELECT '=== TOP 5 CUSTOMERS BY LTV ===' AS section;
SELECT 
    full_name,
    email,
    lifetime_value,
    rfm_segment,
    churn_risk
FROM CORE_marts.MART_CUSTOMER_LTV
ORDER BY lifetime_value DESC
LIMIT 5;

-- Top 5 products by revenue
SELECT '=== TOP 5 PRODUCTS BY REVENUE ===' AS section;
SELECT 
    product_name,
    product_category,
    total_revenue,
    abc_class,
    revenue_rank
FROM CORE_marts.MART_PRODUCT_PERFORMANCE
ORDER BY revenue_rank
LIMIT 5;

-- Sales by country
SELECT '=== SALES BY COUNTRY ===' AS section;
SELECT 
    country_code,
    SUM(total_orders) AS orders,
    SUM(total_revenue) AS revenue
FROM CORE_marts.MART_SALES_OVERVIEW
GROUP BY country_code
ORDER BY revenue DESC;
