with bonds as (
    select * from {{ ref('stg_treasury_bonds') }}
)

select
    product_name,
    index_type,
    maturity_date,
    _reference_period,
    sum(quantity) as total_quantity,
    sum(invested_value) as total_invested,
    sum(gross_value) as total_gross,
    sum(net_value) as total_net,
    sum(current_value) as total_current_value,
    sum(current_value) - sum(invested_value) as gain_loss
from bonds
group by product_name, index_type, maturity_date, _reference_period
order by _reference_period, product_name
