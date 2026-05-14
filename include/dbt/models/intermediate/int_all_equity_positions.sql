with stocks as (
    select
        ticker,
        product_name,
        institution,
        stock_type as asset_subtype,
        'stock' as asset_type,
        quantity,
        closing_price,
        current_value,
        _reference_period
    from {{ ref('stg_stock_positions') }}
),

etfs as (
    select
        ticker,
        product_name,
        institution,
        etf_type as asset_subtype,
        'etf' as asset_type,
        quantity,
        closing_price,
        current_value,
        _reference_period
    from {{ ref('stg_etf_positions') }}
),

combined as (
    select * from stocks
    union all
    select * from etfs
)

select
    ticker,
    asset_type,
    _reference_period,
    sum(quantity) as total_quantity,
    sum(closing_price * quantity) / nullif(sum(quantity), 0) as avg_closing_price,
    sum(current_value) as total_current_value
from combined
group by ticker, asset_type, _reference_period
