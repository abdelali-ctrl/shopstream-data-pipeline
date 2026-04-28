-- Dimension: Customers
-- Layer: core
-- Grain: one row per customer
-- Surrogate key strategy: see ADR-002.

{{ config(
    materialized='table',
    tags=['core', 'dimension']
) }}

with users as (

    select * from {{ ref('stg_users') }}

),

orders_completed as (

    -- Aggregate completed orders per customer for denormalized convenience columns.
    -- "Completed" excludes cancelled / pending so churn metrics aren't polluted.
    select
        customer_id,
        min(order_at)                          as first_order_at,
        max(order_at)                          as last_order_at,
        count(distinct order_id)               as completed_orders,
        sum(order_amount)                      as completed_revenue
    from {{ ref('stg_orders') }}
    where order_status in ('paid', 'shipped', 'delivered')
    group by 1

),

final as (

    select
        -- surrogate key (stable, MD5-based)
        {{ dbt_utils.generate_surrogate_key(['u.user_id']) }}        as customer_key,

        -- natural key (preserved for traceability and join-debugging)
        u.user_id                                                    as customer_id,

        -- attributes
        u.email,
        u.first_name,
        u.last_name,
        u.full_name,
        u.country_code,

        -- segmentation (business-rule, lives in core for reuse across marts)
        case
            when u.plan_type in ({{ "'" ~ var('premium_plans') | join("', '") ~ "'" }}) then 'Premium'
            else 'Freemium'
        end                                                          as customer_segment,
        u.plan_type,
        u.is_active,

        -- key dates
        u.registered_at,
        u.last_login_at,
        o.first_order_at,
        o.last_order_at,

        -- denormalized metrics
        coalesce(o.completed_orders, 0)                              as total_orders,
        coalesce(o.completed_revenue, 0)                             as lifetime_value,

        -- recency status (simple bucketing; richer RFM lives in mart_customer_ltv)
        case
            when o.last_order_at is null                                                  then 'No Purchase'
            when datediff(day, o.last_order_at, current_date()) <= 30                     then 'Active'
            when datediff(day, o.last_order_at, current_date()) <= 90                     then 'At Risk'
            else 'Churned'
        end                                                          as customer_status,

        -- audit
        current_timestamp()                                          as _dbt_updated_at

    from users u
    left join orders_completed o on u.user_id = o.customer_id

)

select * from final
