-- Garante que nenhuma posicao de acao/ETF tem quantidade negativa
select ticker, _reference_period, total_quantity
from {{ ref('int_all_equity_positions') }}
where total_quantity < 0
