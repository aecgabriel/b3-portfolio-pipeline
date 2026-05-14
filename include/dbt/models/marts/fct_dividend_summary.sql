with dividends as (
    select
        trim(split_part(cast(product_name as varchar), ' - ', 1)) as ticker,
        product_name,
        payment_date,
        event_type,
        institution,
        quantity,
        unit_price,
        net_value,
        _reference_period
    from {{ ref('stg_dividends') }}
)

select
    ticker,
    _reference_period,
    event_type,
    sum(net_value) as total_income,
    sum(quantity) as total_shares,
    count(*) as num_payments
from dividends
group by ticker, _reference_period, event_type
order by _reference_period, ticker
