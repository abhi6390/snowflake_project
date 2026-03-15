select
    a.account_id,
    a.account_type,
    a.account_status,
    c.customer_id,
    c.email
from {{ ref('silver_accounts') }} a
join {{ ref('silver_customers') }} c
on a.customer_id = c.customer_id