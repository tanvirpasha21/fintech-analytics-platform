-- marts/customers/fct_cohort_retention.sql
-- Monthly cohort retention analysis.
-- Shows what % of each signup cohort transacted in month N after signup.

with txns as (
    select
        customer_id,
        transaction_month
    from {{ ref('int_transactions_enriched') }}
    where is_completed
),

customers as (
    select
        customer_id,
        date_trunc('month', signup_date) as cohort_month
    from {{ ref('stg_customers') }}
),

cohort_activity as (
    select
        c.customer_id,
        c.cohort_month,
        t.transaction_month,
        date_diff('month',
            c.cohort_month,
            t.transaction_month)        as months_since_signup
    from customers c
    left join txns t using (customer_id)
),

cohort_sizes as (
    select
        cohort_month,
        count(distinct customer_id)     as cohort_size
    from customers
    group by 1
),

retention as (
    select
        a.cohort_month,
        a.months_since_signup,
        count(distinct a.customer_id)   as active_customers
    from cohort_activity a
    where a.months_since_signup >= 0
    group by 1, 2
)

select
    r.cohort_month,
    r.months_since_signup,
    r.active_customers,
    cs.cohort_size,
    {{ safe_divide('r.active_customers', 'cs.cohort_size') }} as retention_rate

from retention r
join cohort_sizes cs using (cohort_month)
order by cohort_month, months_since_signup
