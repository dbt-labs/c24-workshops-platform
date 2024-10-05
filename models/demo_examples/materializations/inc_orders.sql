{{
    config(
        materialized='incremental',
        unique_key='order_id',
        tags=["frequent_runs"]
    )
}}

select * from {{ ref('raw_orders') }}

{% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    where modified_at > (select max(modified_at) from {{ this }}) 
{% endif %}