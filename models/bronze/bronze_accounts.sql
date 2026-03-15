with bronze_accounts as (
    select * 
    from {{ source('dnb', 'accounts_raw') }}
)

select * 
from bronze_accounts