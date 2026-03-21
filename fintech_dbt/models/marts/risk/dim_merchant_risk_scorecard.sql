-- marts/risk/dim_merchant_risk_scorecard.sql
-- One row per merchant with aggregated risk and performance signals.

with merchants as (
    select * from {{ ref('stg_merchants') }}
),

txns as (
    select * from {{ ref('int_transactions_enriched') }}
),

merchant_stats as (
    select
        merchant_id,
        count(*)                                            as total_transactions,
        count(case when is_completed  then 1 end)           as completed_transactions,
        count(case when is_flagged_fraud then 1 end)        as fraud_flagged_count,
        count(case when is_cross_border then 1 end)         as cross_border_count,
        sum(case when is_completed then amount_gbp end)     as total_volume_gbp,
        avg(case when is_completed then amount_gbp end)     as avg_transaction_gbp,
        count(distinct customer_id)                         as unique_customers,
        min(transaction_date)                               as first_seen_date,
        max(transaction_date)                               as last_seen_date
    from txns
    group by 1
),

disputes as (
    select
        t.merchant_id,
        count(distinct d.dispute_id)    as total_disputes,
        sum(d.amount)                   as total_disputed_amount
    from {{ ref('stg_disputes') }} d
    join {{ ref('stg_transactions') }} t using (transaction_id)
    group by 1
)

select
    m.merchant_id,
    m.merchant_name,
    m.merchant_category,
    m.mcc_code,
    m.merchant_country,
    m.is_high_risk,
    m.onboarded_date,

    coalesce(s.total_transactions, 0)       as total_transactions,
    coalesce(s.completed_transactions, 0)   as completed_transactions,
    coalesce(s.fraud_flagged_count, 0)      as fraud_flagged_count,
    coalesce(s.total_volume_gbp, 0)         as total_volume_gbp,
    coalesce(s.avg_transaction_gbp, 0)      as avg_transaction_gbp,
    coalesce(s.unique_customers, 0)         as unique_customers,
    coalesce(d.total_disputes, 0)           as total_disputes,
    coalesce(d.total_disputed_amount, 0)    as total_disputed_amount,

    {{ safe_divide('s.fraud_flagged_count', 's.total_transactions') }}  as fraud_rate,
    {{ safe_divide('d.total_disputes', 's.completed_transactions') }}   as dispute_rate,
    {{ safe_divide('s.completed_transactions', 's.total_transactions') }} as completion_rate,

    -- composite risk score (0-100)
    round(
        least(100,
            coalesce({{ safe_divide('s.fraud_flagged_count', 's.total_transactions') }}, 0) * 4000
          + coalesce({{ safe_divide('d.total_disputes', 's.completed_transactions') }}, 0) * 3000
          + case when m.is_high_risk then 20 else 0 end
        )
    , 1)                                    as risk_score,

    case
        when m.is_high_risk
          or coalesce({{ safe_divide('s.fraud_flagged_count', 's.total_transactions') }}, 0) > 0.05
        then 'critical'
        when coalesce({{ safe_divide('s.fraud_flagged_count', 's.total_transactions') }}, 0) > 0.02
        then 'elevated'
        else 'standard'
    end                                     as risk_band

from merchants m
left join merchant_stats s using (merchant_id)
left join disputes d       using (merchant_id)
