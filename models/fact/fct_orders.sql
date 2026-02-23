{{ config(
    materialized='incremental',
    unique_key='order_id',
    cluster_by=['order_created_at','customer_id'],
    incremental_strategy='insert_overwrite'
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
        updated_at,
        created_at,
        status,
        region,
        customerid,
        isdeleted,
        (unitprice * qty) as gross_amount,
        case
            when type = 'PCT'
                then (qty * unitprice) * (value / 100)
            when type = 'AMT'
                then value
            else 0
        end as discount_amount
    from base
    where type in ('PCT', 'AMT')

),

final as (

    select
        orderid as order_id,
        max(updated_at) as order_updated_at,
        max(created_at) as order_created_at,
        status as current_status,
        region,
        customerid as customer_id,
        isdeleted as is_deleted,
        sum(gross_amount) as gross_amount,
        sum(discount_amount) as discount_amount,
        sum(gross_amount - discount_amount) as net_amount
    from discount
    group by orderid,status,region,customerid,isdeleted

)

select
    order_id,
    region,
    customer_id,
    order_created_at,
    order_updated_at,
    current_status,
    gross_amount,
    discount_amount,
    net_amount,
    is_deleted
from final



