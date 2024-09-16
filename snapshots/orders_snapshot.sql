{% snapshot orders_snapshot %}
    {{
        config(
            unique_key='order_id',
            strategy='timestamp',
            updated_at='modified_at'
        )
    }}

    select * from {{ ref('raw_orders') }}
    
 {% endsnapshot %}