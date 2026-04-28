-- staging model: users
-- Layer: staging
-- Materialization: view (cheap to rebuild; reads cleanly from RAW)
-- Purpose: light cleaning, typing, renaming. No business logic.

{{ config(
    materialized='view',
    tags=['staging', 'users']
) }}

with source as (

    select * from {{ source('raw', 'raw_users') }}

),

renamed as (

    select
        -- identifiers
        id                                 as user_id,
        lower(trim(email))                 as email,

        -- attributes
        first_name,
        last_name,
        trim(first_name) || ' ' || trim(last_name) as full_name,
        upper(country)                     as country_code,
        lower(plan_type)                   as plan_type,
        is_active,

        -- timestamps
        created_at                         as registered_at,
        last_login                         as last_login_at,


    from source
    where id is not null
      and email is not null

)

select * from renamed
