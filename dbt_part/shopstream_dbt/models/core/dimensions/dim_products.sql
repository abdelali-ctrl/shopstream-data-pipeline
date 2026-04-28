-- Dimension: Products
-- Layer: core
-- Grain: one row per product
-- Surrogate key strategy: see ADR-002.
-- Margin: comes from a seed file (seeds/product_margins.csv); see ADR-003.

{{ config(
    materialized='table',
    tags=['core', 'dimension']
) }}

with products as (

    select * from {{ ref('stg_products') }}

),

product_stats as (

    -- Lifetime sales aggregates per product, for convenience columns on the dimension.
    select
        product_id,
        sum(quantity)               as total_quantity_sold,
        sum(line_revenue)           as total_revenue,
        count(distinct order_id)    as total_orders
    from {{ ref('stg_order_items') }}
    group by 1

),

margins as (

    -- Per-category gross margin rate. Falls back to 0.20 if the category is
    -- not present in the seed (acts as a deliberately-conservative default).
    select * from {{ ref('product_margins') }}

),

final as (

    select
        -- surrogate key
        {{ dbt_utils.generate_surrogate_key(['p.product_id']) }}    as product_key,

        -- natural key
        p.product_id,

        -- attributes
        p.product_name,
        p.product_description,
        p.product_category,
        p.merchant_id,

        -- pricing & inventory
        p.current_price,
        p.current_stock,

        -- margin (sourced from seed; see ADR-003)
        coalesce(m.gross_margin_rate, 0.20)                          as gross_margin_rate,

        -- denormalized lifetime metrics
        coalesce(s.total_quantity_sold, 0)                           as total_quantity_sold,
        coalesce(s.total_revenue, 0)                                 as total_revenue,
        coalesce(s.total_orders, 0)                                  as total_orders,

        -- status
        case
            when s.total_revenue is null then 'No Sales'
            else 'Active'
        end                                                          as product_status,

        -- key dates
        p.product_created_at,
        p.product_updated_at,

        -- audit
        current_timestamp()                                          as _dbt_updated_at

    from products p
    left join product_stats s on p.product_id = s.product_id
    left join margins        m on p.product_category = m.product_category

)

select * from final
