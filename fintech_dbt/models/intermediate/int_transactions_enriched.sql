-- intermediate/int_transactions_enriched.sql
-- Joins transactions with FX rates, merchant, and customer context.
-- Produces a single enriched transaction grain with GBP-normalised amounts.

with transactions as (
    select * from {{ ref('stg_transactions') }}
),

customers as (
    select
        customer_id,
        country_code        as customer_country,
        account_tier,
        is_kyc_verified,
        account_currency
    from {{ ref('stg_customers') }}
),

merchants as (
    select
        merchant_id,
        merchant_name,
        merchant_category,
        mcc_code,
        merchant_country,
        is_high_risk
    from {{ ref('stg_merchants') }}
),

fx as (
    select
        rate_date,
        currency,
        rate_to_gbp
    from {{ ref('stg_fx_rates') }}
),

enriched as (
    select
        t.transaction_id,
        t.customer_id,
        t.merchant_id,
        t.amount,
        t.currency,

        -- GBP-normalised amount (join on transaction date + currency)
        round(t.amount / coalesce(fx.rate_to_gbp, 1.0), 2)  as amount_gbp,

        t.transaction_at,
        t.transaction_date,
        t.transaction_month,
        t.transaction_week,
        t.transaction_hour,
        t.day_of_week,
        t.status,
        t.is_completed,
        t.is_declined,
        t.is_flagged_fraud,
        t.is_cross_border,
        t.channel,
        t.ip_country,

        -- merchant dims
        m.merchant_name,
        m.merchant_category,
        m.mcc_code,
        m.merchant_country,
        m.is_high_risk          as is_high_risk_merchant,

        -- customer dims
        c.customer_country,
        c.account_tier,
        c.is_kyc_verified,
        c.account_currency,

        -- derived risk signal
        case
            when t.is_flagged_fraud                     then 'fraud_flagged'
            when m.is_high_risk and t.is_cross_border   then 'high_risk_cross_border'
            when m.is_high_risk                         then 'high_risk_merchant'
            when t.is_cross_border                      then 'cross_border'
            else 'standard'
        end                     as risk_tier

    from transactions t
    left join customers c  using (customer_id)
    left join merchants m  using (merchant_id)
    left join fx           on fx.rate_date = t.transaction_date
                          and fx.currency  = t.currency
)

select * from enriched
