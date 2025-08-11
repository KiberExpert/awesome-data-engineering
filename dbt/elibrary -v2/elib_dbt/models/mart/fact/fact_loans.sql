select
    l.loan_id,
    l.user_id,
    l.book_id,
    l.loan_date,
    l.return_date
from {{ ref('stg_loans__source') }} l
