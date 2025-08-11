select
    l.loan_id,
    l.user_id,
    l.book_id,
    l.loan_date,
    l.return_date
from {{ ref('fact_loans') }} l
where l.return_date > l.loan_date + interval '14 days'