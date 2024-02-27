{% macro event_param(key, type='int_value') %}
SELECT value.{{type}} FROM UNNEST(event_params) WHERE key = '{{ key }}'
{% endmacro %}