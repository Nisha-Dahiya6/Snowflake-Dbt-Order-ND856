{{ config(
    materialized='incremental',
    unique_key='customer_id'
) }}

with source as (

    select *
    from {{ ref('stg_customers') }}

),

filtered as (

    select *
    from source

    {% if is_incremental() %}
        where updated_at > (
            select coalesce(max(updated_at), '1900-01-01')
            from {{ this }}
        )
    {% endif %}

)

select
    customer_id,
    first_name,
    last_name,
    email,
    loyalty_tier,
    loyalty_points,
    region,
    is_deleted,
    created_at,
    updated_at
from filtered