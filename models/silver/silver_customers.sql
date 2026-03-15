with silver_customers as (
    select * 
    from {{ ref('copper_customers') }}
)

select * 
from silver_customers 