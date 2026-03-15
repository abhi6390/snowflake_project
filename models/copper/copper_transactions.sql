with copper_transactions as (
    select * 
    from {{ ref('bronze_transactions') }}
)

SELECT
    concat('TXN', transaction_id) as transaction_id,
    
    concat('ACC', account_id) as account_id,
    
    INITCAP(transaction_type) as transaction_type,

    ABS(CAST(
    REGEXP_REPLACE(amount, '[^0-9.-]', '')
    AS NUMBER(18,2)
    )) AS amount,

    case 
        when UPPER(currency) IN ('RS', 'INR', '₹') then 'INR'
        else 'OTHER'
    end as currency,

    merchant_name,

    COALESCE(
    TRY_TO_DATE(transaction_timestamp, 'YYYY-MM-DD'),
    TRY_TO_DATE(transaction_timestamp, 'YYYY/MM/DD'),
    TRY_TO_DATE(transaction_timestamp, 'DD/MM/YYYY'),
    TRY_TO_DATE(transaction_timestamp, 'MM/DD/YYYY'),
    TRY_TO_DATE(transaction_timestamp, 'DD-MM-YYYY')
    ) AS transaction_timestamp,

    CASE
        WHEN UPPER(status) in ('SUCCESS', 'S') THEN 'Success'
        WHEN UPPER(status) in ('FAILED', 'F') THEN 'Failed'
        ELSE 'Pending'
    END AS status,

    COALESCE(description, 'No Description') as description,

    COALESCE(
    TRY_TO_DATE(updated_at, 'YYYY-MM-DD'),
    TRY_TO_DATE(updated_at, 'YYYY/MM/DD'),
    TRY_TO_DATE(updated_at, 'DD/MM/YYYY'),
    TRY_TO_DATE(updated_at, 'MM/DD/YYYY'),
    TRY_TO_DATE(updated_at, 'DD-MM-YYYY')
    ) AS updated_at,
    
    CURRENT_TIMESTAMP as loaded_at

from copper_transactions