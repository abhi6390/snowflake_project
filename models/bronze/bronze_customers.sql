with bronze_customers as (
    select * 
    from {{ source('dnb', 'customers_raw') }}
)

select * 
from bronze_customers