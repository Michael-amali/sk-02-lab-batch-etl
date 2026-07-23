-- Rows returned = failures. Negative daily revenue means bad source data
-- or a broken transformation upstream.
select *
from {{ ref('fct_daily_store_sales') }}
where revenue < 0