{{ config(materialized='table') }}

with sales as (
    select * from {{ ref('stg_sales') }}
)

select
    order_date,
    store_id,
    count(distinct order_id)  as orders,
    sum(quantity)             as units_sold,
    sum(line_amount)          as revenue
from sales
group by 1, 2