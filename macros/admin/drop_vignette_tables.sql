-- dbt run-operation drop_vignette_tables --args '{tables_to_drop: [inc_orders,orders_snapshot]}'

{% macro drop_vignette_tables(tables_to_drop) %}
    {% if execute %}
        {% for table_to_drop in tables_to_drop %}
        
            {% if table_to_drop.endswith("snapshot") %}
                {% set query %}
                    drop table if exists {{target.database}}.snapshots_dev.{{table_to_drop}};
                {% endset %}
                {% do run_query(query) %}
                
            {% else %}
                {% set query %}
                    drop table if exists {{target.database}}.{{target.schema}}.{{table_to_drop}};
                {% endset %}
                {% do run_query(query) %}
                
            {% endif %}

        {% endfor %}
    {% endif %}
{% endmacro %}