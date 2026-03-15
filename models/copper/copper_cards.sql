with copper_cards as (
    select * 
    from {{ ref('bronze_cards') }}
)

SELECT
    concat('CARD', card_id) as card_id,
    
    concat('CUST', customer_id) as customer_id,
    
    CASE
        WHEN UPPER(card_type) in ('VISA') THEN 'Visa'
        WHEN UPPER(card_type) in ('MASTERCARD', 'MASTER') THEN 'MasterCard'
        WHEN UPPER(card_type) in ('RUPAY') THEN 'RuPay'
        ELSE 'Other'
    END AS card_type,

    card_number,

    COALESCE(
    TRY_TO_DATE(expiry_date, 'YYYY-MM-DD'),
    TRY_TO_DATE(expiry_date, 'YYYY/MM/DD'),
    TRY_TO_DATE(expiry_date, 'DD/MM/YYYY'),
    TRY_TO_DATE(expiry_date, 'MM/DD/YYYY'),
    TRY_TO_DATE(expiry_date, 'DD-MM-YYYY')
    ) AS expiry_date,

    cvv,
    
    COALESCE(
    TRY_TO_DATE(issued_date, 'YYYY-MM-DD'),
    TRY_TO_DATE(issued_date, 'YYYY/MM/DD'),
    TRY_TO_DATE(issued_date, 'DD/MM/YYYY'),
    TRY_TO_DATE(issued_date, 'MM/DD/YYYY'),
    TRY_TO_DATE(issued_date, 'DD-MM-YYYY')
    ) AS issued_date,

    CASE
        WHEN UPPER(status) in ('ACTIVE') THEN 'Active'
        WHEN UPPER(status) in ('BLOCK', 'BLOCKED') THEN 'Blocked'
        ELSE 'Expired'
    END AS status,

    COALESCE(
    TRY_TO_DATE(updated_at, 'YYYY-MM-DD'),
    TRY_TO_DATE(updated_at, 'YYYY/MM/DD'),
    TRY_TO_DATE(updated_at, 'DD/MM/YYYY'),
    TRY_TO_DATE(updated_at, 'MM/DD/YYYY'),
    TRY_TO_DATE(updated_at, 'DD-MM-YYYY')
    ) AS updated_at,
    
    CURRENT_TIMESTAMP as loaded_at

from copper_cards