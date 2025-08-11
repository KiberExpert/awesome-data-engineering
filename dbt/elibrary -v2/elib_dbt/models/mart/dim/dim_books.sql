select
    b.book_id,
    b.title,
    a.full_name as author_name,
    b.published_year
from {{ ref('stg_books__source') }} b
left join {{ ref('stg_authors__source') }} a
    on b.author_id = a.author_id
