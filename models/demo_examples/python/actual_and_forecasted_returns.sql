with agg_daily_returned_orders as (
    select * from {{ ref('agg_daily_returned_orders') }}
),

forecast_daily_returns as (
    select * from {{ ref('forecast_daily_returns') }}
),

final as (
    select 
        "DS" as relevant_date, 
        "Y" as returned_orders, 
        false as is_forecast,
        null as lower_bound,
        null as upper_bound
    from agg_daily_returned_orders

    union all 

    select 
        cast(to_timestamp_ntz("ds"::varchar) as date) as relevant_date, 
        "yhat" as returned_orders, 
        true as is_forecast,
        "yhat_lower" as lower_bound,
        "yhat_upper" as upper_bound 
    from forecast_daily_returns
    where relevant_date > (select max("DS") from agg_daily_returned_orders)
)

select * from final 