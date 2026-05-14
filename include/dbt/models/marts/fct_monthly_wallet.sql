with portfolio as (
    select
        _reference_period,
        sum(total_value) as total_portfolio_value
    from {{ ref('int_monthly_portfolio_value') }}
    group by _reference_period
),

portfolio_by_class as (
    select
        _reference_period,
        sum(case when asset_class = 'equity' then total_value else 0 end) as equity_value,
        sum(case when asset_class = 'treasury' then total_value else 0 end) as treasury_value,
        sum(case when asset_class = 'fixed_income' then total_value else 0 end) as fixed_income_value
    from {{ ref('int_monthly_portfolio_value') }}
    group by _reference_period
),

equity_positions as (
    select * from {{ ref('int_all_equity_positions') }}
),

avg_prices as (
    select * from {{ ref('int_weighted_avg_price') }}
),

equity_invested as (
    select
        p._reference_period,
        sum(p.total_quantity * coalesce(a.weighted_avg_price, 0)) as total_equity_invested
    from equity_positions p
    left join avg_prices a on p.ticker = a.ticker
    group by p._reference_period
),

treasury_invested as (
    select
        _reference_period,
        sum(invested_value) as total_treasury_invested
    from {{ ref('stg_treasury_bonds') }}
    group by _reference_period
),

fixed_income_invested as (
    select
        _reference_period,
        sum(curva_value) as total_fi_invested
    from {{ ref('stg_fixed_income') }}
    group by _reference_period
)

select
    p._reference_period,
    p.total_portfolio_value,
    pbc.equity_value,
    pbc.treasury_value,
    pbc.fixed_income_value,
    coalesce(ei.total_equity_invested, 0)
        + coalesce(ti.total_treasury_invested, 0)
        + coalesce(fi.total_fi_invested, 0) as total_invested,
    p.total_portfolio_value - (
        coalesce(ei.total_equity_invested, 0)
        + coalesce(ti.total_treasury_invested, 0)
        + coalesce(fi.total_fi_invested, 0)
    ) as absolute_gain_loss,
    case
        when (coalesce(ei.total_equity_invested, 0) + coalesce(ti.total_treasury_invested, 0) + coalesce(fi.total_fi_invested, 0)) > 0
        then (
            (p.total_portfolio_value - (coalesce(ei.total_equity_invested, 0) + coalesce(ti.total_treasury_invested, 0) + coalesce(fi.total_fi_invested, 0)))
            / (coalesce(ei.total_equity_invested, 0) + coalesce(ti.total_treasury_invested, 0) + coalesce(fi.total_fi_invested, 0))
        ) * 100
        else 0
    end as pct_return
from portfolio p
left join portfolio_by_class pbc on p._reference_period = pbc._reference_period
left join equity_invested ei on p._reference_period = ei._reference_period
left join treasury_invested ti on p._reference_period = ti._reference_period
left join fixed_income_invested fi on p._reference_period = fi._reference_period
order by p._reference_period
