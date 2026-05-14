with equity as (
    select
        _reference_period,
        'equity' as asset_class,
        sum(total_current_value) as total_value
    from {{ ref('int_all_equity_positions') }}
    group by _reference_period
),

treasury as (
    select
        _reference_period,
        'treasury' as asset_class,
        sum(current_value) as total_value
    from {{ ref('stg_treasury_bonds') }}
    group by _reference_period
),

fixed_income as (
    select
        _reference_period,
        'fixed_income' as asset_class,
        sum(curva_value) as total_value
    from {{ ref('stg_fixed_income') }}
    group by _reference_period
),

all_assets as (
    select * from equity
    union all
    select * from treasury
    union all
    select * from fixed_income
)

select
    _reference_period,
    asset_class,
    total_value
from all_assets
