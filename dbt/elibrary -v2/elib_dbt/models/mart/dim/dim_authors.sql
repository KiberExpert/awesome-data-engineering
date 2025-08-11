select
    author_id,
    full_name,
    country
from {{ ref('stg_authors__source') }}
