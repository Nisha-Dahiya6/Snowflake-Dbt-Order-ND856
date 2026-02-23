{{ config(
    materialized='incremental',
    unique_key=['order_id','item_id']
) }}

with source as (

    select *
    from {{ ref('stg_orders') }}

),

base as (

    select *
    from source

    {% if is_incremental() %}
        where updated_at > (
            select coalesce(max(order_updated_at), '1900-01-01'::timestamp)
            from {{ this }}
        )
    {% endif %}
),

discount as (

        select
        orderid,
        itemid,
        productid,
        qty,
        unitprice,
        currency,
        (unitprice * qty) as line_gross,
        case
            when type = 'PCT'
                then (qty * unitprice) * (value / 100)
            when type = 'AMT'
                then value
            else 0
        end as line_discount,
        updated_at
    from base
    where type in ('PCT', 'AMT')

),

final as (

    select
        orderid as order_id,
        itemid as item_id,
        productid as product_id,
        qty,
        unitprice,
        currency,
        line_gross as gross_amount,
        line_discount as discount_amount,
        (line_gross - line_discount) as net_amount,
        updated_at as order_updated_at
    from discount
    

)

select
    order_id,
    item_id,
    product_id,
    qty,
    unitprice,
    currency,
    gross_amount,
    discount_amount,
    net_amount,
    order_updated_at
from final
 


