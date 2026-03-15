with copper_accounts as (
    select * 
    from {{ ref('bronze_accounts') }}
)

select 
    concat('ACC', account_id) as account_id,
    concat('CUST', customer_id) as customer_id,
    CASE
        WHEN UPPER(account_type) IN ('SAVINGS','SAV','SAVING', 'S') THEN 'Savings'
        WHEN UPPER(account_type) IN ('CURR','CURRENT','CURRENTS','C') THEN 'Current'
        ELSE 'Other'
    END AS account_type,

    CASE
        WHEN UPPER(account_status) IN ('ACTIVE','ACT', 'A') THEN 'Active'
        WHEN UPPER(account_status) IN ('CLOSED','CLOSE','CLO','C') THEN 'Current'
        ELSE 'Suspended'
    END AS account_status,

    ABS(CAST(
    REGEXP_REPLACE(balance, '[^0-9.-]', '')
    AS NUMBER(18,2)
    )) AS balance,

    COALESCE(
    TRY_TO_DATE(opened_date, 'YYYY-MM-DD'),
    TRY_TO_DATE(opened_date, 'YYYY/MM/DD'),
    TRY_TO_DATE(opened_date, 'DD/MM/YYYY'),
    TRY_TO_DATE(opened_date, 'MM/DD/YYYY'),
    TRY_TO_DATE(opened_date, 'DD-MM-YYYY')
    ) AS opened_date,

    CASE 
    WHEN LOWER(account_status) LIKE 'act%' THEN NULL
    ELSE 
        COALESCE(
        TRY_TO_DATE(closed_date, 'YYYY-MM-DD'),
        TRY_TO_DATE(closed_date, 'YYYY/MM/DD'),
        TRY_TO_DATE(closed_date, 'DD/MM/YYYY'),
        TRY_TO_DATE(closed_date, 'MM/DD/YYYY'),
        TRY_TO_DATE(closed_date, 'DD-MM-YYYY')
        )
    END as closed_date,

    COALESCE(branch_code, 'OTHER') AS branch_code,

    {{ date_macro('updated_at') }} as updated_at,

    CURRENT_TIMESTAMP as loaded_at


from copper_accounts