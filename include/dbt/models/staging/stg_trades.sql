with source as (
    select * from {{ source('raw', 'raw_trades') }}
)

select
    case
        when trim(cast(Codigo_de_Negociacao as varchar)) like '%F'
        then left(trim(cast(Codigo_de_Negociacao as varchar)), length(trim(cast(Codigo_de_Negociacao as varchar))) - 1)
        else trim(cast(Codigo_de_Negociacao as varchar))
    end as ticker,
    Periodo_Inicial as period_start,
    Periodo_Final as period_end,
    trim(Instituicao) as institution,
    cast(Quantidade_Compra as double) as buy_quantity,
    cast(Quantidade_Venda as double) as sell_quantity,
    cast(Quantidade_Liquida as double) as net_quantity,
    cast(Preco_Medio_Compra as double) as avg_buy_price,
    cast(Preco_Medio_Venda as double) as avg_sell_price,
    _source_file,
    _reference_period
from source
where Codigo_de_Negociacao is not null
  and trim(cast(Codigo_de_Negociacao as varchar)) != ''
