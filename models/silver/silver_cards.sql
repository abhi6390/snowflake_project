with silver_cards as (
    select
        *
    from {{ ref('copper_cards') }} 
)

select *
from silver_cards