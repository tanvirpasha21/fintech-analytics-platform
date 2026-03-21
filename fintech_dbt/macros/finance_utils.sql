{#
  convert_currency(amount_col, from_currency_col, fx_table, target='GBP')
  Reusable macro to inline a currency conversion expression.
  Usage:
    {{ convert_currency('amount', 'currency', ref('stg_fx_rates')) }}
#}

{% macro convert_currency(amount_col, from_currency_col, target='GBP') %}
    round(
        {{ amount_col }} / nullif(
            (select rate_to_gbp
             from {{ ref('stg_fx_rates') }} fx
             where fx.currency = {{ from_currency_col }}
               and fx.rate_date = current_date
             limit 1)
        , 0)
    , 2)
{% endmacro %}


{#
  generate_date_spine_cte(start, end)
  Returns a CTE fragment producing one row per calendar day.
  Usage: {{ generate_date_spine_cte('2023-01-01', '2024-12-31') }}
#}
{% macro generate_date_spine(start_date, end_date) %}
    with date_spine as (
        select
            (DATE '{{ start_date }}' + INTERVAL (i) DAY)::date as calendar_date
        from generate_series(
            0,
            date_diff('day', DATE '{{ start_date }}', DATE '{{ end_date }}')
        ) t(i)
    )
{% endmacro %}


{#
  safe_divide(numerator, denominator)
  Avoids division-by-zero; returns null instead.
#}
{% macro safe_divide(numerator, denominator) %}
    case
        when ({{ denominator }}) = 0 or ({{ denominator }}) is null
        then null
        else ({{ numerator }}) * 1.0 / ({{ denominator }})
    end
{% endmacro %}


{#
  pct_of_total(metric_col, partition_col)
  Inline window function for share-of-total calculations.
#}
{% macro pct_of_total(metric_col, partition_col=none) %}
    {% if partition_col %}
        round(100.0 * {{ metric_col }} / nullif(sum({{ metric_col }}) over (partition by {{ partition_col }}), 0), 2)
    {% else %}
        round(100.0 * {{ metric_col }} / nullif(sum({{ metric_col }}) over (), 0), 2)
    {% endif %}
{% endmacro %}
