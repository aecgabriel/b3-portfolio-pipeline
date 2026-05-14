with source as (
    select * from {{ source('raw', 'raw_treasury_bonds') }}
)

select
    trim(Produto) as product_name,
    trim(Instituicao) as institution,
    trim(Codigo_ISIN) as isin_code,
    trim(cast(Indexador as varchar)) as index_type,
    Vencimento as maturity_date,
    cast(Quantidade as double) as quantity,
    cast(Valor_Aplicado as double) as invested_value,
    cast(Valor_bruto as double) as gross_value,
    cast(Valor_liquido as double) as net_value,
    cast(Valor_Atualizado as double) as current_value,
    _source_file,
    _reference_period
from source
where Produto is not null
  and trim(cast(Produto as varchar)) != ''
