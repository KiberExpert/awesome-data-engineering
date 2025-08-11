with source as (
    select * from {{ source('elib_source', 'authors') }}
)
select
    author_id,
    full_name,
    country
from source
