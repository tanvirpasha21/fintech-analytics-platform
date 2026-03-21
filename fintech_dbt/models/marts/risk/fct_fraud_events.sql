-- marts/risk/fct_fraud_events.sql
-- INCREMENTAL model — appends new fraud-flagged transactions only.
-- In production this would run hourly against a streaming source.
-- Demonstrates incremental materialisation pattern for large data.

{{
    config(
        materialized = 'incremental',
        unique_key   = 'transaction_id',
        on_schema_change = 'append_new_columns'
    )
}}

with flagged as (
    select
        transaction_id,
        customer_id,
        merchant_id,
        amount_gbp,
        currency,
        transaction_at,
        transaction_date,
        channel,
        ip_country,
        customer_country,
        account_tier,
        is_kyc_verified,
        merchant_category,
        mcc_code,
        is_high_risk_merchant,
        is_cross_border,
        risk_tier,
        status

    from {{ ref('int_transactions_enriched') }}
    where is_flagged_fraud = true

    {% if is_incremental() %}
        -- only process rows newer than the latest record in the table
        and transaction_at > (select max(transaction_at) from {{ this }})
    {% endif %}
),

enriched as (
    select
        f.*,

        -- dispute match
        case when d.dispute_id is not null then true else false end  as has_dispute,
        d.dispute_id,
        d.reason                                                    as dispute_reason,
        d.status                                                    as dispute_status,

        -- velocity: how many fraud flags for this customer in last 30 days
        count(*) over (
            partition by f.customer_id
            order by f.transaction_at
            range between interval '30 days' preceding and current row
        )                                                           as customer_fraud_velocity_30d

    from flagged f
    left join {{ ref('stg_disputes') }} d
        on d.transaction_id = f.transaction_id
)

select * from enriched
