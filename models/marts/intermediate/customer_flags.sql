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
        coalesce(sum(orders.total_price),0) as lifetime_value,
        iff(lifetime_value > 3000000, 'Y', 'N') as is_high_value,
        iff(lifetime_value between 1000000 and 2999999, 'Y', 'N') as is_mid_value,
        iff(lifetime_value between 0 and 999999, 'Y','N') as is_low_value

    from customer
        inner join orders
            on customer.customer_key = orders.customer_key
    group by 1
)

select * from final