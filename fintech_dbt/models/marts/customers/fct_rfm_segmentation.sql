with reference_date as (
    select max(transaction_date) as today from {{ ref('stg_transactions') }}
),
completed_txns as (
    select customer_id, transaction_date, amount_gbp
    from {{ ref('int_transactions_enriched') }}
    where is_completed
),
rfm_raw as (
    select t.customer_id,
        date_diff('day', max(t.transaction_date), r.today) as recency_days,
        count(*)                                            as frequency,
        sum(t.amount_gbp)                                   as monetary_gbp,
        max(t.transaction_date)                             as last_transaction_date
    from completed_txns t cross join reference_date r
    group by t.customer_id, r.today
),
rfm_scored as (
    select *,
        6 - ntile(5) over (order by recency_days desc) as r_score,
        ntile(5) over (order by frequency asc)          as f_score,
        ntile(5) over (order by monetary_gbp asc)       as m_score
    from rfm_raw
),
rfm_segmented as (
    select *,
        r_score + f_score + m_score as rfm_total,
        case
            when r_score=5 and f_score>=4 and m_score>=4 then 'Champions'
            when f_score>=4 and m_score>=3               then 'Loyal Customers'
            when r_score>=4 and f_score>=2 and f_score<4 then 'Potential Loyalists'
            when r_score>=4 and f_score<2                then 'Recent Customers'
            when r_score=3 and f_score>=2                then 'Promising'
            when r_score=3 and f_score<2 and m_score>=3  then 'Need Attention'
            when r_score=2 and f_score<=2                then 'About to Sleep'
            when r_score<=2 and f_score>=3 and m_score>=3 then 'At Risk'
            when r_score=1 and f_score>=4                then 'Cannot Lose Them'
            when r_score<=2 and f_score<=2 and m_score<=2 then 'Hibernating'
            when r_score=1 and f_score=1                 then 'Lost'
            else 'Others'
        end as rfm_segment
    from rfm_scored
)
select
    s.customer_id, c.full_name, c.email, c.country_code,
    c.account_tier, c.account_currency, c.signup_date, c.is_kyc_verified,
    s.recency_days, s.frequency, round(s.monetary_gbp,2) as monetary_gbp,
    s.last_transaction_date, s.r_score, s.f_score, s.m_score,
    s.rfm_total, s.rfm_segment,
    case
        when s.rfm_segment in ('Champions','Loyal Customers') then 'High Value'
        when s.rfm_segment in ('Potential Loyalists','Recent Customers','Promising') then 'Growth'
        when s.rfm_segment in ('Need Attention','About to Sleep') then 'Slipping'
        when s.rfm_segment in ('At Risk','Cannot Lose Them') then 'At Risk'
        when s.rfm_segment in ('Hibernating','Lost') then 'Lost'
        else 'Others'
    end as segment_group,
    case
        when s.rfm_segment='Champions'          then 'Reward them. Ask for reviews. Upsell premium.'
        when s.rfm_segment='Loyal Customers'    then 'Offer loyalty programme. Upsell higher tier.'
        when s.rfm_segment='Potential Loyalists' then 'Offer membership. Personalised recommendations.'
        when s.rfm_segment='Recent Customers'   then 'Onboarding support. Build the habit.'
        when s.rfm_segment='Promising'          then 'Create brand awareness. Free trials.'
        when s.rfm_segment='Need Attention'     then 'Reactivation campaign. Limited time offer.'
        when s.rfm_segment='About to Sleep'     then 'Reconnect. Share relevant products.'
        when s.rfm_segment='At Risk'            then 'Send personalised email. Win-back campaign.'
        when s.rfm_segment='Cannot Lose Them'   then 'Win back with renewals. Talk to them.'
        when s.rfm_segment='Hibernating'        then 'Offer relevant products. Discounts.'
        when s.rfm_segment='Lost'               then 'Revive interest with new offering or ignore.'
        else 'Monitor'
    end as recommended_action
from rfm_segmented s
join {{ ref('stg_customers') }} c using (customer_id)