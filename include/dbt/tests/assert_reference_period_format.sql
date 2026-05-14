-- Garante que _reference_period segue o formato YYYY-MM em todas as tabelas mart
select _reference_period, 'fct_monthly_wallet' as source_table
from {{ ref('fct_monthly_wallet') }}
where _reference_period not similar to '\d{4}-\d{2}'

union all

select _reference_period, 'fct_stock_detail'
from {{ ref('fct_stock_detail') }}
where _reference_period not similar to '\d{4}-\d{2}'

union all

select _reference_period, 'fct_dividend_summary'
from {{ ref('fct_dividend_summary') }}
where _reference_period not similar to '\d{4}-\d{2}'
