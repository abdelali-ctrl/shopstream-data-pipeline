-- staging model: orders
-- Layer: staging
-- Materialization: view
-- Purpose: light cleaning, typing, renaming. No business logic.
-- Note: order amounts <= 0 are dropped here as a cleaning step (test data
-- generator can produce $0 orders for cancelled rows). If a real source ever
-- has legitimately-zero orders, move this filter to a dedicated mart.

{{ config(
    materialized='view',
    tags=['staging', 'orders']
) }}

with source as (

    select * from {{ source('raw', 'raw_orders') }}

),

renamed as (

    select
        -- identifiers
        id                                  as order_id,
        user_id                             as customer_id,

        -- timestamps
        created_at                          as order_at,
        cast(created_at as date)            as order_date,

        -- metrics
        total_amount                        as order_amount,

        -- attributes
        lower(status)                       as order_status,
        upper(country)                      as country_code,
        lower(payment_method)               as payment_method,


    from source
    where id is not null
      and total_amount > 0

)

select * from renamed
