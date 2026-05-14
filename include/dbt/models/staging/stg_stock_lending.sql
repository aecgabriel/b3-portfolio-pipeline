with source as (
    select * from {{ source('raw', 'raw_stock_lending') }}
)

select
    trim(split_part(cast(Produto as varchar), ' - ', 1)) as ticker,
    trim(Produto) as product_name,
    trim(Instituicao) as institution,
    trim(cast(Natureza as varchar)) as nature,
    trim(cast(Numero_de_Contrato as varchar)) as contract_number,
    cast(Taxa as double) as rate,
    cast(Quantidade as integer) as quantity,
    cast(Preco_de_Fechamento as double) as closing_price,
    cast(Valor_Atualizado as double) as current_value,
    Data_de_registro as registration_date,
    Data_de_vencimento as maturity_date,
    _source_file,
    _reference_period
from source
where Produto is not null
  and trim(cast(Produto as varchar)) != ''
