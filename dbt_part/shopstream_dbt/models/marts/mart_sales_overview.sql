-- Mart: Sales Overview
-- Layer: marts
-- Grain: one row per (sale_date, country, product_category, customer_segment)
-- Use case: top-of-funnel BI dashboard for daily revenue / order trends.

{{ config(
    materialized='table',
    tags=['mart', 'sales']
) }}

with fact_orders as (

    select * from {{ ref('fact_orders') }}
    where is_completed = 1

),

dim_products as (

    select product_key, product_category from {{ ref('dim_products') }}

),

dim_customers as (

    select customer_key, customer_segment from {{ ref('dim_customers') }}

),

daily_sales as (

    select
        f.date_key                                              as sale_date,
        f.country_code,
        p.product_category,
        c.customer_segment,

        -- volume
        count(distinct f.order_key)                             as total_orders,
        count(distinct f.customer_key)                          as unique_customers,
        sum(f.quantity_sold)                                    as total_quantity,

        -- revenue and margin
        sum(f.line_revenue)                                     as total_revenue,
        sum(f.estimated_margin)                                 as total_margin,
        avg(f.order_total)                                      as avg_order_value,

        -- ratios
        sum(f.line_revenue) / nullif(count(distinct f.order_key), 0) as revenue_per_order,
        sum(f.estimated_margin) / nullif(sum(f.line_revenue), 0)     as margin_rate

    from fact_orders f
    inner join dim_products  p on f.product_key  = p.product_key
    inner join dim_customers c on f.customer_key = c.customer_key
    group by 1, 2, 3, 4

)

select * from daily_sales
