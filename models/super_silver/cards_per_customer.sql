select 
    customer_id,
    count(card_id) as number_of_cards
from {{ ref('silver_cards') }}
group by customer_id