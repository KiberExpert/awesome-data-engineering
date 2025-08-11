{% macro current_date_utc() %}
    current_timestamp at time zone 'UTC'
{% endmacro %}
