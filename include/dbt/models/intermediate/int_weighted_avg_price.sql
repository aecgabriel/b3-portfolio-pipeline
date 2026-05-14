with trade_lots as (
    select * from {{ ref('int_trades_clean') }}
    where buy_quantity > 0 and avg_buy_price > 0
)

select
    ticker,
    sum(buy_quantity) as total_shares_bought,
    sum(total_buy_cost) as total_invested,
    sum(total_buy_cost) / nullif(sum(buy_quantity), 0) as weighted_avg_price
from trade_lots
group by ticker
