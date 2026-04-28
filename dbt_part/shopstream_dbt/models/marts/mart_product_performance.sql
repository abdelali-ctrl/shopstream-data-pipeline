-- Mart: Product Performance & ABC Classification
-- Layer: marts
-- Grain: one row per product (with at least one completed sale)
-- Use case: merchandising prioritization (ABC analysis: A = top 80% of revenue,
-- B = next 15%, C = bottom 5%).

{{ config(
    materialized='table',
    tags=['mart', 'product']
) }}

with fact_orders as (

    select * from {{ ref('fact_orders') }}
    where is_completed = 1

),

dim_products as (

    select * from {{ ref('dim_products') }}

),

product_metrics as (

    select
        f.product_key,
        p.product_name,
        p.product_category,

        -- volume
        count(distinct f.order_key)                                         as total_orders,
        sum(f.quantity_sold)                                                as total_quantity_sold,

        -- revenue & price
        sum(f.line_revenue)                                                 as total_revenue,
        avg(f.unit_price)                                                   as avg_unit_price,

        -- margin
        sum(f.estimated_margin)                                             as total_margin,
        sum(f.estimated_margin) / nullif(sum(f.line_revenue), 0)            as margin_rate,

        -- dates
        min(f.order_timestamp)                                              as first_sale_date,
        max(f.order_timestamp)                                              as last_sale_date

    from fact_orders f
    inner join dim_products p on f.product_key = p.product_key
    group by 1, 2, 3

),

ranked as (

    select
        *,
        row_number() over (order by total_revenue desc)                     as revenue_rank,
        sum(total_revenue) over (
            order by total_revenue desc
            rows between unbounded preceding and current row
        )                                                                   as cumulative_revenue,
        sum(total_revenue) over ()                                          as total_revenue_all
    from product_metrics

),

final as (

    select
        *,
        total_revenue / nullif(total_revenue_all, 0)                        as revenue_pct,
        cumulative_revenue / nullif(total_revenue_all, 0)                   as cumulative_revenue_pct,

        case
            when cumulative_revenue / nullif(total_revenue_all, 0) <= 0.80 then 'A'
            when cumulative_revenue / nullif(total_revenue_all, 0) <= 0.95 then 'B'
            else 'C'
        end                                                                 as abc_class,

        case
            when revenue_rank <= 10 then 'Top 10'
            when revenue_rank <= 50 then 'Top 50'
            else 'Long Tail'
        end                                                                 as performance_tier

    from ranked

)

select
    product_key,
    product_name,
    product_category,
    total_orders,
    total_quantity_sold,
    total_revenue,
    avg_unit_price,
    total_margin,
    margin_rate,
    first_sale_date,
    last_sale_date,
    revenue_rank,
    revenue_pct,
    cumulative_revenue_pct,
    abc_class,
    performance_tier
from final
order by revenue_rank
