{{
    config(
        materialized = 'table',
        transient=false
    )
}}

with customer as (

    select * from {{ ref('stg_tpch_customers') }}

),
nation as (

    select * from {{ ref('stg_tpch_nations') }}
),
region as (

    select * from {{ ref('stg_tpch_regions') }}

),
customer_flags as (
    select * from {{ ref('customer_flags') }}
),

final as (
    select 
        customer.customer_key,
        customer.name,
        customer.address,
        --- Break the contract 
        -- nation.nation_key as nation_key, 
        nation.name as nation,
        --- Break the contract again
        -- region.region_key as region_key,
        region.name as region,
        customer.phone_number,
        customer.account_balance,
        customer.market_segment,
        customer_flags.lifetime_value,
        customer_flags.is_high_value,
        customer_flags.is_mid_value,
        customer_flags.is_low_value
    from
        customer
        inner join nation
            on customer.nation_key = nation.nation_key
        inner join region
            on nation.region_key = region.region_key
        left join customer_flags
            on customer.customer_key = customer_flags.customer_key
)
select 
    *
from
    final
order by
    customer_key