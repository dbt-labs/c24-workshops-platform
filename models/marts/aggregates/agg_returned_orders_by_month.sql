with fct_order_items as (
    select * from {{ ref('fct_order_items') }}
),

final as (
    select
        date_trunc(MONTH, fct_order_items.order_date) as order_month 
        , count(case when is_return then order_item_key else null end) as returned_orders
        , 1.0* returned_orders / nullif (
            count(order_item_key)
            , 0
        ) as return_rate
        , count(*) as row_count
    from fct_order_items
    group by 1
    order by 1 desc
)

select * from final  