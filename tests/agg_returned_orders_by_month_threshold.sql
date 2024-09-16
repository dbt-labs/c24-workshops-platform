{{
    config(
        enabled=true,
        severity='error',
    )
}}

with agg_returned_orders_by_month as ( select * from {{ ref('agg_returned_orders_by_month') }} )

select *
from agg_returned_orders_by_month
where order_month = (select max(order_month) from agg_returned_orders_by_month)
and return_rate > 0.50 

