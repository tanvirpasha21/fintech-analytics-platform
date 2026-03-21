-- analyses/executive_insights.sql
-- Ad-hoc analytical queries that demonstrate the mart layer in action.
-- These are the kinds of questions an analytics engineer fields from leadership.

-- ─────────────────────────────────────────────────────────────
-- 1. OVERALL BUSINESS HEALTH SNAPSHOT
-- ─────────────────────────────────────────────────────────────
-- Monthly TPV (Total Payment Volume), completion rate, and fraud rate trend
select
    month,
    sum(total_volume_gbp)                                       as total_volume_gbp,
    sum(total_transactions)                                     as total_transactions,
    round(avg(completion_rate) * 100, 2)                        as avg_completion_pct,
    round(avg(fraud_rate) * 100, 3)                             as avg_fraud_pct,
    round(sum(total_volume_gbp)
        / lag(sum(total_volume_gbp)) over (order by month)
        * 100 - 100, 1)                                         as mom_volume_growth_pct
from main_marts_payments.fct_monthly_payment_performance
group by 1
order by 1;


-- ─────────────────────────────────────────────────────────────
-- 2. CUSTOMER LTV DISTRIBUTION BY TIER
-- ─────────────────────────────────────────────────────────────
select
    account_tier,
    ltv_tier,
    count(*)                                    as customer_count,
    round(avg(total_spend_gbp), 2)              as avg_ltv_gbp,
    round(avg(engagement_score), 1)             as avg_engagement,
    round(100.0 * sum(case when churn_risk_segment in ('at_risk','churned')
                           then 1 else 0 end) / count(*), 1) as churn_risk_pct
from main_marts_customers.dim_customers_360
group by 1, 2
order by account_tier, avg_ltv_gbp desc;


-- ─────────────────────────────────────────────────────────────
-- 3. COHORT RETENTION — MONTH-0 THROUGH MONTH-12
-- ─────────────────────────────────────────────────────────────
select
    strftime(cohort_month, '%Y-%m')             as cohort,
    cohort_size,
    max(case when months_since_signup = 0  then round(retention_rate*100,1) end) as "M0",
    max(case when months_since_signup = 1  then round(retention_rate*100,1) end) as "M1",
    max(case when months_since_signup = 2  then round(retention_rate*100,1) end) as "M2",
    max(case when months_since_signup = 3  then round(retention_rate*100,1) end) as "M3",
    max(case when months_since_signup = 6  then round(retention_rate*100,1) end) as "M6",
    max(case when months_since_signup = 12 then round(retention_rate*100,1) end) as "M12"
from main_marts_customers.fct_cohort_retention
group by 1, 2
order by 1;


-- ─────────────────────────────────────────────────────────────
-- 4. TOP-10 HIGHEST RISK MERCHANTS
-- ─────────────────────────────────────────────────────────────
select
    merchant_name,
    merchant_category,
    merchant_country,
    total_transactions,
    round(total_volume_gbp, 2)          as total_volume_gbp,
    round(fraud_rate * 100, 2)          as fraud_rate_pct,
    round(dispute_rate * 100, 2)        as dispute_rate_pct,
    risk_score,
    risk_band
from main_marts_risk.dim_merchant_risk_scorecard
where total_transactions >= 10
order by risk_score desc
limit 10;


-- ─────────────────────────────────────────────────────────────
-- 5. FRAUD VELOCITY — CUSTOMERS WITH REPEAT FRAUD FLAGS
-- ─────────────────────────────────────────────────────────────
select
    f.customer_id,
    c.account_tier,
    c.country_code,
    c.is_kyc_verified,
    count(*)                                    as fraud_events,
    max(f.customer_fraud_velocity_30d)          as max_30d_velocity,
    round(sum(f.amount_gbp), 2)                 as total_fraud_amount_gbp,
    count(case when f.has_dispute then 1 end)   as disputed_count,
    count(distinct f.merchant_category)         as merchant_categories_hit
from main_marts_risk.fct_fraud_events f
join main_marts_customers.dim_customers_360 c using (customer_id)
group by 1, 2, 3, 4
having count(*) >= 3
order by fraud_events desc, total_fraud_amount_gbp desc;


-- ─────────────────────────────────────────────────────────────
-- 6. PAYMENT SUCCESS RATE BY CHANNEL × CATEGORY
-- ─────────────────────────────────────────────────────────────
select
    channel,
    merchant_category,
    sum(total_transactions)                     as total_txns,
    round(avg(completion_rate) * 100, 2)        as completion_pct,
    round(avg(fraud_rate) * 100, 3)             as fraud_pct,
    round(sum(total_volume_gbp), 2)             as volume_gbp
from main_marts_payments.fct_monthly_payment_performance
group by 1, 2
order by volume_gbp desc;


-- ─────────────────────────────────────────────────────────────
-- 7. FX EXPOSURE — VOLUME BY CURRENCY (GBP-NORMALISED)
-- ─────────────────────────────────────────────────────────────
select
    currency,
    sum(total_transactions)                     as total_txns,
    round(sum(total_volume_gbp), 2)             as volume_gbp,
    round(100.0 * sum(total_volume_gbp)
        / sum(sum(total_volume_gbp)) over (), 2) as pct_of_total
from main_marts_payments.fct_monthly_payment_performance
group by 1
order by volume_gbp desc;
