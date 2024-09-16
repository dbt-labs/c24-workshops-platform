{{
    config(
        materialized='table'
    )
}}

with customer as (
    select * from {{ ref('stg_tpch_customers') }}
),

orders as (
    select * from {{ ref('stg_tpch_orders') }}
),
final as (
    select
        customer.customer_key,
        sum(orders.total_price) as lifetime_value,
        case 
            when lifetime_value <= 200000 then 'tier1'
            when lifetime_value > 2000000 then 'tier2'
            when lifetime_value between 1000000 and 1999999 then 'tier3'
            when lifetime_value between 0 and 999999 then 'tier4' 
        end as tier_name
    from customer
        inner join orders
            on customer.customer_key = orders.customer_key
    group by 1
)

select * from final