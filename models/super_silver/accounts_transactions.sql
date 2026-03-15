select
    t.*,
    a.account_type,
    a.account_status,
    a.customer_id
from {{ ref('silver_transactions') }} t
join {{ ref('silver_accounts') }} a
on t.account_id = a.account_id