{% snapshot users_snapshots %}
{{ config(
    target_schema='snapshots',
    unique_key='user_id',
    strategy='check',
    check_cols=['full_name', 'email']
) }}
select * from {{ ref('stg_users__source') }}
{% endsnapshot %}
