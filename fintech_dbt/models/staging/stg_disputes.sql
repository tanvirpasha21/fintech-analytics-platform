-- staging/stg_disputes.sql

with source as (
    select * from raw.disputes
)

select
    dispute_id,
    transaction_id,
    customer_id,
    cast(opened_at as timestamp)    as opened_at,
    cast(opened_at as date)         as opened_date,
    reason,
    status,
    amount,
    currency,

    status = 'won'                  as is_won,
    status = 'pending'              as is_pending

from source
