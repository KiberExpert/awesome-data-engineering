{% snapshot books_snapshots %}
{{ config(
    target_schema='snapshots',
    unique_key='book_id',
    strategy='check',
    check_cols=['title', 'author_id']
) }}
select * from {{ ref('stg_books__source') }}
{% endsnapshot %}
