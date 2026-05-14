-- Garante que o preco medio ponderado e igual a total_invested / total_shares_bought
select
    ticker,
    weighted_avg_price,
    total_invested / total_shares_bought as expected_price,
    abs(weighted_avg_price - (total_invested / total_shares_bought)) as diff
from {{ ref('int_weighted_avg_price') }}
where total_shares_bought > 0
  and abs(weighted_avg_price - (total_invested / total_shares_bought)) > 0.01
