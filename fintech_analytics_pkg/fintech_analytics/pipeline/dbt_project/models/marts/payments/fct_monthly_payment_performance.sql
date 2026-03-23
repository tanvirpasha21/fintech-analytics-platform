-- marts/payments/fct_monthly_payment_performance.sql
-- Executive-level monthly payments KPI table.
-- One row per month × currency × channel.

with txns as (
    select * from {{ ref('int_transactions_enriched') }}
),

monthly as (
    select
        transaction_month                               as month,
        currency,
        channel,
        merchant_category,

        -- volume
        count(*)                                        as total_transactions,
        count(case when is_completed  then 1 end)       as completed_transactions,
        count(case when is_declined   then 1 end)       as declined_transactions,
        count(case when status = 'pending' then 1 end)  as pending_transactions,

        -- value (GBP-normalised)
        sum(case when is_completed then amount_gbp end) as total_volume_gbp,
        avg(case when is_completed then amount_gbp end) as avg_transaction_gbp,

        -- success rate
        {{ safe_divide(
            'count(case when is_completed then 1 end)',
            'count(*)'
        ) }}                                            as completion_rate,

        -- fraud
        count(case when is_flagged_fraud then 1 end)    as fraud_flagged_count,
        {{ safe_divide(
            'count(case when is_flagged_fraud then 1 end)',
            'count(*)'
        ) }}                                            as fraud_rate,

        -- cross-border
        count(case when is_cross_border then 1 end)     as cross_border_count

    from txns
    group by 1, 2, 3, 4
)

select
    *,
    {{ pct_of_total('total_volume_gbp', 'month') }}     as pct_of_monthly_volume

from monthly
order by month, total_volume_gbp desc
