with source as (
    select * from {{ source('raw', 'raw_etf_positions') }}
)

select
    trim(Codigo_de_Negociacao) as ticker,
    trim(Produto) as product_name,
    trim(Instituicao) as institution,
    cast(Conta as varchar) as account,
    trim(Tipo) as etf_type,
    cast(Quantidade as integer) as quantity,
    cast(Quantidade_Disponivel as integer) as quantity_available,
    cast(Preco_de_Fechamento as double) as closing_price,
    cast(Valor_Atualizado as double) as current_value,
    _source_file,
    _reference_period
from source
where Codigo_de_Negociacao is not null
  and trim(cast(Codigo_de_Negociacao as varchar)) != ''
