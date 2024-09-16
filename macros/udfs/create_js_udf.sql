{% macro create_js_udf() %}

{% set results = run_query(string_reverse_nulls('this is a string')) %}
{% do results.print_table() %}

{% endmacro %}