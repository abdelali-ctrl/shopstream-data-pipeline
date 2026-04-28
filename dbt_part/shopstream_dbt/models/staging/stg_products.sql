-- staging model: products
-- Layer: staging
-- Materialization: view
-- Purpose: light cleaning, typing, renaming. No business logic.

{{ config(
    materialized='view',
    tags=['staging', 'products']
) }}

with source as (

    select * from {{ source('raw', 'raw_products') }}

),

renamed as (

    select
        -- identifiers
        id                          as product_id,
        merchant_id,

        -- attributes
        trim(name)                  as product_name,
        description                 as product_description,
        lower(trim(category))       as product_category,

        -- pricing & inventory
        price                       as current_price,
        stock_quantity              as current_stock,

        -- timestamps
        created_at                  as product_created_at,
        updated_at                  as product_updated_at,


    from source
    where id is not null
      and price >= 0

)

select * from renamed
