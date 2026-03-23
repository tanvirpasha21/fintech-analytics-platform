-- intermediate/int_customer_transaction_summary.sql
-- Aggregates per-customer transaction behaviour used across multiple marts.
-- Avoids re-computing the same aggregations in each mart.

with txns as (
    select * from {{ ref('int_transactions_enriched') }}
    where is_completed
),

summary as (
    select
        customer_id,
        account_tier,
        customer_country,
        is_kyc_verified,

        -- volume
        count(*)                                        as total_transactions,
        count(distinct transaction_month)               as active_months,
        count(distinct merchant_id)                     as distinct_merchants,
        count(distinct merchant_category)               as distinct_categories,

        -- spend
        sum(amount_gbp)                                 as total_spend_gbp,
        avg(amount_gbp)                                 as avg_transaction_gbp,
        max(amount_gbp)                                 as max_transaction_gbp,
        percentile_cont(0.5) within group
            (order by amount_gbp)                       as median_transaction_gbp,

        -- time
        min(transaction_date)                           as first_transaction_date,
        max(transaction_date)                           as last_transaction_date,
        date_diff('day',
            min(transaction_date),
            max(transaction_date))                      as customer_tenure_days,

        -- risk
        sum(case when is_flagged_fraud then 1 else 0 end)   as fraud_flagged_count,
        sum(case when is_cross_border  then 1 else 0 end)   as cross_border_count,

        -- channel mix
        sum(case when channel = 'app'  then 1 else 0 end)   as app_txn_count,
        sum(case when channel = 'card' then 1 else 0 end)   as card_txn_count,
        sum(case when channel = 'web'  then 1 else 0 end)   as web_txn_count

    from txns
    group by 1,2,3,4
)

select
    *,
    -- engagement score: more months active + more categories = higher engagement
    round(
        (active_months * 3.0)
      + (distinct_categories * 2.0)
      + (least(total_transactions, 100) * 0.5)
    , 1) as engagement_score

from summary
