with source as (
    select * from {{ source('raw', 'raw_fixed_income') }}
)

select
    trim(Produto) as product_name,
    trim(Instituicao) as institution,
    trim(Emissor) as issuer,
    trim(cast(Codigo as varchar)) as code,
    trim(cast(Indexador as varchar)) as index_type,
    trim(cast(Tipo_de_regime as varchar)) as regime_type,
    Data_de_Emissao as issue_date,
    Vencimento as maturity_date,
    cast(Quantidade as double) as quantity,
    cast(Preco_Atualizado_CURVA as double) as curva_price,
    cast(Valor_Atualizado_CURVA as double) as curva_value,
    _source_file,
    _reference_period
from source
where Produto is not null
  and trim(cast(Produto as varchar)) != ''
