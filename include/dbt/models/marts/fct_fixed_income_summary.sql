with fi as (
    select * from {{ ref('stg_fixed_income') }}
)

select
    product_name,
    issuer,
    code,
    index_type,
    regime_type,
    issue_date,
    maturity_date,
    _reference_period,
    sum(quantity) as total_quantity,
    sum(curva_value) as total_curva_value
from fi
group by
    product_name, issuer, code, index_type, regime_type,
    issue_date, maturity_date, _reference_period
order by _reference_period, product_name
