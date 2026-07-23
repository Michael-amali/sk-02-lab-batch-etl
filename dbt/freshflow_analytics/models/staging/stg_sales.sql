with source as (
    select * from {{ source('raw', 'sales') }}
),

renamed as (
    select
        order_id,
        store_id,
        product_id,
        cast(quantity as integer)          as quantity,
        cast(unit_price as numeric(10,2))  as unit_price,
        cast(quantity as integer) * cast(unit_price as numeric(10,2))
                                           as line_amount,
        cast(order_ts as timestamp)        as ordered_at,
        date(cast(order_ts as timestamp))  as order_date
    from source
)

select * from renamed