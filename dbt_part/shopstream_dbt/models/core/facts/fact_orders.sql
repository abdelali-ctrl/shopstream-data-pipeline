-- Fact: Order Lines
-- Layer: core
-- Grain: one row per (order, product) line item
-- Surrogate key strategy: see ADR-002.
-- Margin: derived from dim_products.gross_margin_rate (see ADR-003).

{{ config(
    materialized='table',
    tags=['core', 'fact']
) }}

with orders as (

    select * from {{ ref('stg_orders') }}

),

order_items as (

    select * from {{ ref('stg_order_items') }}

),

dim_customers as (

    select customer_id, customer_key from {{ ref('dim_customers') }}

),

dim_products as (

    select product_id, product_key, gross_margin_rate from {{ ref('dim_products') }}

),

final as (

    select
        -- surrogate key for the fact line itself
        {{ dbt_utils.generate_surrogate_key(['oi.order_item_id']) }}     as order_line_key,

        -- order-level surrogate (composite of order_id only; one order has many lines)
        {{ dbt_utils.generate_surrogate_key(['o.order_id']) }}           as order_key,

        -- natural keys (kept for debug & joins back to source)
        oi.order_item_id,
        o.order_id,

        -- foreign keys to dimensions (surrogates)
        c.customer_key,
        p.product_key,

        -- date key (used for time-based joins; format YYYYMMDD-style not required)
        o.order_date                                                     as date_key,

        -- degenerate dimensions
        o.order_status,
        o.country_code,
        o.payment_method,

        -- timestamps
        o.order_at                                                       as order_timestamp,

        -- additive measures
        oi.quantity                                                      as quantity_sold,
        oi.unit_price,
        oi.line_revenue,
        o.order_amount                                                   as order_total,

        -- derived measure (margin rate from dim_products; see ADR-003)
        oi.line_revenue * p.gross_margin_rate                            as estimated_margin,

        -- flags
        case when o.order_status in ('paid', 'shipped', 'delivered') then 1 else 0 end as is_completed,
        case when o.order_status = 'cancelled'                       then 1 else 0 end as is_cancelled,

        -- audit
        current_timestamp()                                              as _dbt_updated_at

    from order_items oi
    inner join orders        o on oi.order_id    = o.order_id
    inner join dim_customers c on o.customer_id  = c.customer_id
    inner join dim_products  p on oi.product_id  = p.product_id
    where o.order_at >= '{{ var("start_date") }}'

)

select * from final
