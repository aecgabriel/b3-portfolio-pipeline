-- Garante que nenhum ticker no staging de trades ainda tem sufixo F
select ticker
from {{ ref('stg_trades') }}
where ticker like '%F'
  and length(ticker) > 1
