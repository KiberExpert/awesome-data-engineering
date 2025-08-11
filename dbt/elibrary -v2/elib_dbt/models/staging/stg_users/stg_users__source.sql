with source as (
    select * from {{ source('elib_source', 'users') }}
)
select
    user_id,
    full_name,
    email,
    created_at
from source