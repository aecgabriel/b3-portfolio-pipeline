-- Garante que o valor total da carteira nunca e negativo
select _reference_period, total_portfolio_value
from {{ ref('fct_monthly_wallet') }}
where total_portfolio_value < 0
