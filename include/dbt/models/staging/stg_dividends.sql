with source as (
    select * from {{ source('raw', 'raw_dividends') }}
)

select
    trim(Produto) as product_name,
    Pagamento as payment_date,
    trim(Tipo_de_Evento) as event_type,
    trim(Instituicao) as institution,
    cast(Quantidade as double) as quantity,
    cast(Preco_unitario as double) as unit_price,
    cast(Valor_liquido as double) as net_value,
    _source_file,
    _reference_period
from source
where Produto is not null
  and trim(cast(Produto as varchar)) != ''
