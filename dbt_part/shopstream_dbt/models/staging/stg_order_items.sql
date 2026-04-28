-- staging model: order_items
-- Layer: staging
-- Materialization: view
-- Purpose: light cleaning, typing, renaming. No business logic.

{{ config(
    materialized='view',
    tags=['staging', 'order_items']
) }}

with source as (

    select * from {{ source('raw', 'raw_order_items') }}

),

renamed as (

    select
        -- identifiers
        id                          as order_item_id,
        order_id,
        product_id,

        -- metrics
        quantity,
        unit_price,
        line_total                  as line_revenue,


    from source
    where id is not null
      and quantity > 0

)

select * from renamed
