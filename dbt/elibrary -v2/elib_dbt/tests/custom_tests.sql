{% test is_recent(model, column_name) %}
    select *
    from {{ model }}
    where {{ column_name }} < current_date - interval '5 years'
{% endtest %}