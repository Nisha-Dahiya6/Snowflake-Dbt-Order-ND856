with source as (

    select *
    from {{ source('my_source', 'CUSTOMERS') }}

),

base as (

    select
        to_date(doc:audit.createdAt."$date"::string)      as created_at,
        to_date(doc:audit.updatedAt."$date"::string)      as updated_at,
        doc:customerId::string                            as customer_id,
        doc:isDeleted::boolean                            as is_deleted,
        doc:loyalty.points::number                        as loyalty_points,
        doc:loyalty.tier::string                          as loyalty_tier,
        doc:name.first::string                            as first_name,
        doc:name.last::string                             as last_name,
        doc:region::string                                as region,
        doc:contacts.emails                               as emails_array,
        doc:contacts.phones                               as phones_array
    from source

),

emails as (

    select
        b.customer_id,
        e.value::string as email
    from base b
    left join lateral flatten(input => b.emails_array) e

),

phones as (

    select
        b.customer_id,
        p.value:type::string  as phone_type,
        p.value:value::string as phone_number
    from base b
    left join lateral flatten(input => b.phones_array) p

)

select
    b.created_at,
    b.updated_at,
    b.customer_id,
    b.is_deleted,
    b.loyalty_points,
    b.loyalty_tier,
    b.first_name,
    b.last_name,
    b.region,
    e.email,
    p.phone_type,
    p.phone_number
from base b
left join emails e using (customer_id)
left join phones p using (customer_id)