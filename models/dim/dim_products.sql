{{ config(
    materialized='incremental',
    unique_key='productid'
) }}

with source as (

    select *
    from {{ ref('stg_products') }}

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
    productid,
    name,
    sku,
    l1,
    l2,
    warrantymonths,
    color,
    active,
    updated_at
from filtered