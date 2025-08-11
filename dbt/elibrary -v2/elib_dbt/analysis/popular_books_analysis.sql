select
    b.title,
    count(l.loan_id) as total_loans
from {{ ref('fact_loans') }} l
join {{ ref('dim_books') }} b on l.book_id = b.book_id
group by b.title
order by total_loans desc
limit 10