-- marts/customers/dim_customers_360.sql
-- Full customer dimension with LTV, engagement, and cohort fields.
-- One row per customer.

with customers as (
    select * from {{ ref('stg_customers') }}
),

summary as (
    select * from {{ ref('int_customer_transaction_summary') }}
),

disputes as (
    select
        customer_id,
        count(*)                                            as total_disputes,
        sum(case when is_won     then 1 else 0 end)         as disputes_won,
        sum(case when is_pending then 1 else 0 end)         as disputes_pending,
        sum(amount)                                         as total_disputed_amount
    from {{ ref('stg_disputes') }}
    group by 1
),

joined as (
    select
        c.customer_id,
        c.full_name,
        c.email,
        c.country_code,
        c.account_tier,
        c.signup_date,
        c.kyc_status,
        c.is_kyc_verified,
        c.account_currency,
        c.age_years,

        -- transaction summary
        coalesce(s.total_transactions, 0)       as total_transactions,
        coalesce(s.active_months, 0)            as active_months,
        coalesce(s.total_spend_gbp, 0)          as total_spend_gbp,
        coalesce(s.avg_transaction_gbp, 0)      as avg_transaction_gbp,
        coalesce(s.median_transaction_gbp, 0)   as median_transaction_gbp,
        coalesce(s.distinct_merchants, 0)       as distinct_merchants,
        coalesce(s.distinct_categories, 0)      as distinct_categories,
        s.first_transaction_date,
        s.last_transaction_date,
        coalesce(s.customer_tenure_days, 0)     as customer_tenure_days,
        coalesce(s.fraud_flagged_count, 0)      as fraud_flagged_count,
        coalesce(s.cross_border_count, 0)       as cross_border_count,
        coalesce(s.engagement_score, 0)         as engagement_score,

        -- disputes
        coalesce(d.total_disputes, 0)           as total_disputes,
        coalesce(d.disputes_won, 0)             as disputes_won,
        coalesce(d.total_disputed_amount, 0)    as total_disputed_amount,

        -- cohort (first full month of activity)
        date_trunc('month', c.signup_date)      as signup_cohort,

        -- LTV tier (simple rule-based segmentation)
        case
            when coalesce(s.total_spend_gbp, 0) >= 10000 then 'platinum'
            when coalesce(s.total_spend_gbp, 0) >=  3000 then 'gold'
            when coalesce(s.total_spend_gbp, 0) >=   500 then 'silver'
            else 'bronze'
        end                                     as ltv_tier,

        -- churn risk: no transactions in last 60 days
        case
            when s.last_transaction_date is null                            then 'churned'
            when date_diff('day', s.last_transaction_date, current_date) > 60 then 'at_risk'
            when date_diff('day', s.last_transaction_date, current_date) > 30 then 'cooling'
            else 'active'
        end                                     as churn_risk_segment,

        date_diff('day',
            c.signup_date,
            current_date)                       as days_since_signup

    from customers c
    left join summary  s using (customer_id)
    left join disputes d using (customer_id)
)

select * from joined
