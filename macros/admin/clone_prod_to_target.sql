{% macro clone_prod_to_target(from) %}

    {% set sql -%}
        create schema if not exists {{ target.database }}.{{ target.schema }} clone {{ from }};
    {%- endset %}

    {{ dbt_utils.log_info("Cloning schema " ~ from ~ " into target schema.") }}

    {% do run_query(sql) %}

    {{ dbt_utils.log_info("Cloned schema " ~ from ~ " into target schema.") }}

{% endmacro %}