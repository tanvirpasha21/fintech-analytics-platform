-- staging/stg_transactions.sql
-- Cast types, rename columns, derive simple flags.
-- No currency conversion here — that happens in intermediate.

with source as (
    select * from raw.transactions
),

renamed as (
    select
        transaction_id,
        customer_id,
        merchant_id,
        amount,
        currency,
        cast(transaction_at as timestamp) as transaction_at,
        status,
        cast(is_flagged_fraud as boolean) as is_flagged_fraud,
        channel,
        ip_country,

        -- date parts (used heavily in marts)
        cast(transaction_at as date)                        as transaction_date,
        date_trunc('month', cast(transaction_at as date))   as transaction_month,
        date_trunc('week',  cast(transaction_at as date))   as transaction_week,
        extract('hour' from cast(transaction_at as timestamp)) as transaction_hour,
        dayofweek(cast(transaction_at as date))             as day_of_week,

        -- convenience flags
        status = 'completed'            as is_completed,
        status = 'declined'             as is_declined,
        ip_country != currency          as is_cross_border

    from source
)

select * from renamed
