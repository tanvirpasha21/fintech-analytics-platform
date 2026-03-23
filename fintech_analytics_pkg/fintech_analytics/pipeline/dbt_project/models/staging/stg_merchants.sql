-- staging/stg_merchants.sql

with source as (
    select * from raw.merchants
),

renamed as (
    select
        merchant_id,
        merchant_name,
        category                        as merchant_category,
        mcc_code,
        country                         as merchant_country,
        cast(is_high_risk as boolean)   as is_high_risk,
        cast(onboarded_date as date)    as onboarded_date,

        -- derived
        date_diff('day',
            cast(onboarded_date as date),
            current_date)               as days_since_onboarding

    from source
)

select * from renamed
