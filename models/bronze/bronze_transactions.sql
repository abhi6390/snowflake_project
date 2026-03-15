with bronze_transactions as (
    select * 
    from {{ source('dnb', 'transactions_raw') }}
)
select *
from bronze_transactions