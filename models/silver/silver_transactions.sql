with silver_transactions as (
    select
        *
    from {{ ref('copper_transactions') }} 
)

select *
from silver_transactions