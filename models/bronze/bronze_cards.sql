with bronze_cards as (
    select * 
    from {{ source('dnb','cards_raw') }}
)

SELECT *
from bronze_cards