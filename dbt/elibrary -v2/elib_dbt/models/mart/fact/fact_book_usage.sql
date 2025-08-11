select
    book_id,
    count(*) as total_loans
from {{ ref('stg_loans__source') }}
group by book_id
