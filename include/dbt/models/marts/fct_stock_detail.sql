with positions as (
    select * from {{ ref('int_all_equity_positions') }}
),

avg_prices as (
    select * from {{ ref('int_weighted_avg_price') }}
)

select
    p.ticker,
    p.asset_type,
    p._reference_period,
    p.total_quantity,
    a.weighted_avg_price as mean_purchase_price,
    p.avg_closing_price as current_closing_price,
    p.total_current_value,
    p.total_quantity * coalesce(a.weighted_avg_price, 0) as total_invested,
    p.total_current_value - (p.total_quantity * coalesce(a.weighted_avg_price, 0)) as unrealized_gain_loss,
    case
        when coalesce(a.weighted_avg_price, 0) > 0
        then ((p.avg_closing_price - a.weighted_avg_price) / a.weighted_avg_price) * 100
        else 0
    end as pct_price_change
from positions p
left join avg_prices a on p.ticker = a.ticker
order by p._reference_period, p.ticker
