{% macro get_month(column_name) %}to_char({{ timestamp_to_local(column_name) }}, 'YYYY-MM'){% endmacro %}