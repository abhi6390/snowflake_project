with silver_accounts as (
    select
        *
    from {{ ref('copper_accounts') }} 
)

select *
from silver_accounts