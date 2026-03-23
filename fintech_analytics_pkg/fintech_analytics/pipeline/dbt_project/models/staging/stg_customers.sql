-- staging/stg_customers.sql
-- Standardise and lightly clean raw customer records.
-- One row per customer. No joins, no business logic.

with source as (
    select * from raw.customers
),

renamed as (
    select
        customer_id,
        full_name,
        lower(trim(email))          as email,
        phone,
        country                     as country_code,
        account_tier,
        cast(signup_date as date)   as signup_date,
        kyc_status,
        cast(date_of_birth as date) as date_of_birth,
        currency                    as account_currency,

        -- derived
        date_diff('year',
            cast(date_of_birth as date),
            current_date)           as age_years,

        case
            when kyc_status = 'verified' then true
            else false
        end                         as is_kyc_verified

    from source
)

select * from renamed
