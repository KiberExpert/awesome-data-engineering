with source as (
    select * from {{ source('elib_source', 'books') }}
)
select
    book_id,
    title,
    author_id,
    published_year
from source
