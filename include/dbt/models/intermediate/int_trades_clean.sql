with trades as (
    select * from {{ ref('stg_trades') }}
)

select
    ticker,
    _reference_period,
    institution,
    buy_quantity,
    avg_buy_price,
    sell_quantity,
    avg_sell_price,
    buy_quantity * avg_buy_price as total_buy_cost,
    sell_quantity * avg_sell_price as total_sell_value
from trades
where buy_quantity > 0 or sell_quantity > 0
