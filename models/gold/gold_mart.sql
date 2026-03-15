select
    at.*,
    ca.email,
    coalesce(cp.number_of_cards, 0) as number_of_cards
from {{ ref('accounts_transactions') }} at

join {{ ref('customers_accounts') }} ca
    on at.account_id = ca.account_id

left join {{ ref('cards_per_customer') }} cp
    on at.customer_id = cp.customer_id