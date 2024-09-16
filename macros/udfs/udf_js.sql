{% macro string_reverse_nulls(s) %}
CREATE OR REPLACE FUNCTION string_reverse_nulls(s string)
    RETURNS string
    LANGUAGE JAVASCRIPT
    AS '
    if (S === undefined) {
        return "string was null";
    } else
    {
        return undefined;
    }
    ';
{% endmacro %}