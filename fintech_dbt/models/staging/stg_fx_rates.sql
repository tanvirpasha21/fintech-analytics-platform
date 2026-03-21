-- staging/stg_fx_rates.sql

with source as (
    select * from raw.fx_rates
)

select
    cast(rate_date as date) as rate_date,
    currency,
    rate_to_gbp,
    1.0 / rate_to_gbp       as gbp_to_currency
from source
