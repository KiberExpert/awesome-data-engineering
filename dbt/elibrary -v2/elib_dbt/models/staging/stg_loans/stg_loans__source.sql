with source as (
    select * from {{ source('elib_source', 'loans') }}
)
select
    loan_id,
    user_id,
    book_id,
    loan_date,
    return_date
from source
