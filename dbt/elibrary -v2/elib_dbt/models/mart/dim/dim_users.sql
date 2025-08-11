select
    user_id,
    full_name,
    email,
    created_at
from {{ ref('stg_users__source') }}
