with copper_customers as (
    select * 
    from {{ ref('bronze_customers') }}
)

select 
    concat('CUST', customer_id) as customer_id,

    REGEXP_REPLACE(full_name, '[^A-Za-z ]', '') as full_name,

    REGEXP_REPLACE(email, '[^A-Za-z0-9@._-]', '') as email,

    REGEXP_REPLACE(phone_number, '[^0-9]', '') as phone_number,

    COALESCE(
    TRY_TO_DATE(date_of_birth, 'YYYY-MM-DD'),
    TRY_TO_DATE(date_of_birth, 'YYYY/MM/DD'),
    TRY_TO_DATE(date_of_birth, 'DD/MM/YYYY'),
    TRY_TO_DATE(date_of_birth, 'MM/DD/YYYY'),
    TRY_TO_DATE(date_of_birth, 'DD-MM-YYYY')
    ) AS date_of_birth,

    pan_number,

    address,

    CASE
        WHEN UPPER(country) IN ('IND','IN','INDIA', 'BHARAT') THEN 'India'
        WHEN UPPER(country) IN ('US','USA', 'U.S.', 'UNITED STATES') THEN 'USA'
        WHEN UPPER(country) IN ('GB','UK', 'UNITED KINGDOM') THEN 'United Kingdom'
        ELSE 'OTHER'
    END AS country,

    {{ date_macro('created_at') }} as created_at,

    {{ date_macro('updated_at') }} as updated_at,

    CURRENT_TIMESTAMP as loaded_at

from copper_customers