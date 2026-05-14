# B3 Portfolio Analytics Pipeline

Pipeline de dados de ponta a ponta que ingere os relatorios consolidados mensais da B3 (xlsx), transforma os dados com dbt e gera um relatorio profissional em Excel com benchmarks de performance. Roda localmente via Apache Airflow (Astronomer CLI) com DuckDB como data warehouse analitico.

## Por que este projeto existe

Todo mes, a B3 disponibiliza um **Relatorio Consolidado Mensal** para cada investidor na [Area do Investidor](https://www.investidor.b3.com.br/). Esses arquivos xlsx contem dados brutos sobre posicoes em acoes, ETFs, renda fixa, tesouro direto, proventos e negociacoes — porem estao espalhados em multiplas abas, multiplos arquivos e com formatacao inconsistente (nomes de colunas em portugues, decimais com virgula, linhas de total no final).

Este pipeline resolve tres problemas:

1. **Consolidacao de dados** — Combina 19+ arquivos xlsx mensais em um unico banco analitico, limpando e normalizando os dados ao longo do processo.
2. **Analise de carteira** — Calcula preco medio ponderado de compra, lucro/prejuizo nao realizado, evolucao patrimonial mensal e composicao por classe de ativo.
3. **Geracao de relatorio** — Produz um relatorio Excel completo com graficos de performance comparando a carteira contra CDI, IBOV e DIVO11, alem de todos os dados necessarios para a declaracao anual de IRPF.

## Arquitetura

```
files/*.xlsx          Airflow (Astronomer)              DuckDB                    Relatorio Excel
 19 relatorios ────>  ingest_xlsx_to_duckdb  ──────>  7 tabelas raw  ──────>
 mensais B3           (PythonOperator)                     |
                                                      dbt_deps
                                                           |
                                                      dbt_staging ──────>  7 views de staging
                                                           |
                                                      dbt_transform ───>  4 views intermediarias
                                                           |               5 tabelas mart
                                                           |
                                                      generate_excel_report ──> include/reports/carteira_report.xlsx
                                                                                (8 abas + graficos)
```

| Componente | Tecnologia | Funcao |
|------------|-----------|--------|
| Orquestracao | Apache Airflow 3.x (astro-runtime 3.2-4) | Agendamento de tasks, gerenciamento de dependencias, UI |
| Transformacao | dbt-core + dbt-duckdb | Modelagem de dados em SQL (staging/intermediate/marts) |
| Warehouse | DuckDB | Banco analitico local, zero infraestrutura |
| Integracao | astronomer-cosmos | Renderiza cada modelo dbt como uma task individual no Airflow |
| Relatorio | openpyxl + yfinance | Geracao de Excel com graficos e dados de benchmark |
| Benchmarks | API BCB + Yahoo Finance | Taxas CDI, indice IBOV, precos do ETF DIVO11 |

## Pre-requisitos

- **Docker Desktop** — instalado e rodando
- **Astronomer CLI** >= 1.42.0 — [guia de instalacao](https://www.astronomer.io/docs/astro/cli/install-cli)
- **Relatorios mensais da B3** — baixados da [Area do Investidor B3](https://www.investidor.b3.com.br/)

### Verificando os pre-requisitos

```bash
docker --version        # Docker 24+ recomendado
astro version           # 1.42.0+
```

## Estrutura do Projeto

```
airflow/
├── dags/
│   └── b3_portfolio_pipeline.py    # Definicao do DAG principal
├── include/
│   ├── report_generator.py         # Modulo de geracao do relatorio Excel
│   └── dbt/
│       ├── dbt_project.yml         # Configuracao do projeto dbt
│       ├── profiles.yml            # Perfil de conexao com DuckDB
│       ├── packages.yml            # Dependencia dbt_utils
│       └── models/
│           ├── staging/            # 7 modelos — limpeza e tipagem de dados
│           ├── intermediate/       # 4 modelos — logica de negocio e agregacoes
│           └── marts/              # 5 modelos — tabelas prontas para relatorio
├── files/                          # Coloque seus relatorios xlsx da B3 aqui
├── include/reports/                 # Relatorios Excel gerados aparecem aqui
├── dbt_data/                       # Arquivo do banco DuckDB (criado automaticamente)
├── Dockerfile                      # Imagem base astro-runtime 3.2-4
├── requirements.txt                # Dependencias Python
├── airflow_settings.yaml           # Configuracao de pool para concorrencia DuckDB
└── .gitignore
```

## Dados de Entrada — Relatorios Mensais B3

Baixe seus relatorios consolidados mensais na Area do Investidor da B3. Os arquivos seguem o padrao de nomenclatura:

```
relatorio-consolidado-mensal-YYYY-nomemes.xlsx
```

Exemplos: `relatorio-consolidado-mensal-2024-junho.xlsx`, `relatorio-consolidado-mensal-2025-dezembro.xlsx`

Coloque todos os arquivos xlsx no diretorio `files/`. O pipeline processa **7 tipos de abas**:

| Nome da Aba | Tabela Raw | Conteudo |
|-------------|-----------|----------|
| Posicao - Acoes | `raw_stock_positions` | Posicoes em acoes (ticker, quantidade, preco de fechamento, valor atual) |
| Posicao - ETF | `raw_etf_positions` | Posicoes em ETFs (B5P211, WRLD11, etc.) |
| Posicao - Renda Fixa | `raw_fixed_income` | Renda fixa (CDB, LCI, LCA com valores MTM e CURVA) |
| Posicao - Tesouro Direto | `raw_treasury_bonds` | Titulos do Tesouro (IPCA+, Selic, Prefixado) |
| Proventos Recebidos | `raw_dividends` | Dividendos, JCP e rendimentos recebidos |
| Negociacoes | `raw_trades` | Compras e vendas com quantidades e precos medios |
| Posicao - Emprestimos | `raw_stock_lending` | Posicoes de emprestimo de acoes |

**Nem toda aba aparece em todo arquivo** — o pipeline trata abas ausentes sem erro. Por exemplo, dados de ETF podem aparecer apenas a partir de determinado mes.

## Como Usar

### 1. Clonar e posicionar os dados

```bash
cd airflow/
# Copie seus arquivos xlsx da B3 para o diretorio files/
cp ~/Downloads/relatorio-consolidado-mensal-*.xlsx files/
```

### 2. Iniciar o ambiente Airflow

```bash
astro dev start
```

Isso cria 5 containers Docker:

| Container | Funcao |
|-----------|--------|
| api-server | API REST e interface web do Airflow (porta 8080) |
| scheduler | Monitora e dispara as tasks |
| dag-processor | Faz o parse dos arquivos de DAG |
| triggerer | Gerencia tasks assincronas/deferred |
| postgres | Banco de metadados do Airflow |

### 3. Acessar a interface do Airflow

Abra [http://localhost:8080](http://localhost:8080) e faca login com:
- **Usuario:** `admin`
- **Senha:** `admin`

### 4. Executar o pipeline

1. Localize o DAG `b3_portfolio_pipeline` na lista de DAGs
2. Ative-o com o toggle **ON** (caso nao esteja)
3. Clique no botao **Play** para disparar uma execucao manual
4. Acompanhe o progresso das tasks na visualizacao **Grid** ou **Graph**

### 5. Obter o relatorio

Apos a conclusao do pipeline, o relatorio Excel estara disponivel em:

```
include/reports/carteira_report.xlsx
```

## Tasks do Pipeline (em ordem)

### Task 1: `ingest_xlsx_to_duckdb`

Le todos os arquivos xlsx do diretorio `files/`, extrai o ano-mes a partir do nome do arquivo e carrega cada aba na tabela raw correspondente no DuckDB.

Comportamentos principais:
- **Idempotente**: apaga e recria todas as tabelas raw a cada execucao
- **Sanitizacao de colunas**: converte caracteres portugueses para ASCII (`Negociacao` -> `Negociacao`)
- **Correcao de decimais**: converte virgulas (`0,23`) para pontos (`0.23`)
- **Filtragem de linhas**: remove linhas de Total/NaN no final e colunas sem nome (Unnamed)
- **Colunas de metadados**: adiciona `_source_file` e `_reference_period` a cada linha

### Task 2: `dbt_deps`

Instala as dependencias de pacotes dbt (dbt_utils).

### Task 3: `dbt_staging` (Cosmos DbtTaskGroup)

Executa 7 modelos de staging que limpam e tipam os dados brutos:

| Modelo | Logica Principal |
|--------|-----------------|
| `stg_stock_positions` | Tipagem de colunas, filtragem de linhas invalidas |
| `stg_etf_positions` | Mesma estrutura das acoes, tipo de ativo separado |
| `stg_fixed_income` | Remove colunas sem nome, extrai curva_value |
| `stg_treasury_bonds` | Extrai valores investido/bruto/liquido/atual |
| `stg_dividends` | Trata payment_date, event_type, net_value |
| `stg_trades` | **Remove o sufixo F** dos tickers (BBAS3F -> BBAS3) |
| `stg_stock_lending` | Extrai taxa, informacoes de contrato |

Todos os modelos de staging sao materializados como **views** e executados sequencialmente (pool DuckDB = 1 slot) para evitar conflitos de lock de escrita.

### Task 4: `dbt_transform` (Cosmos DbtTaskGroup)

Executa 4 modelos intermediarios + 5 modelos mart:

**Intermediarios (views):**

| Modelo | Funcao |
|--------|--------|
| `int_trades_clean` | Calcula total_buy_cost e total_sell_value por lote de negociacao |
| `int_weighted_avg_price` | **Preco medio ponderado de compra** por ticker: `soma(custo) / soma(qtd)` |
| `int_all_equity_positions` | Une acoes + ETFs, agrega por ticker por mes entre corretoras |
| `int_monthly_portfolio_value` | Totais mensais por classe de ativo (acoes, tesouro, renda fixa) |

**Marts (tabelas):**

| Modelo | Entrega |
|--------|---------|
| `fct_monthly_wallet` | Carteira mensal: valor total, valor por classe, investido, ganho/perda, % retorno |
| `fct_stock_detail` | Por acao/ETF por mes: preco medio, preco de tela, lucro/prejuizo nao realizado |
| `fct_dividend_summary` | Proventos por ticker, mes e tipo de evento |
| `fct_treasury_summary` | Tesouro Direto: investido vs valor atual, ganho/perda |
| `fct_fixed_income_summary` | Renda fixa (CDB/LCI/LCA) por produto, emissor, vencimento |

### Task 5: `generate_excel_report`

Consulta todas as tabelas mart, busca dados de benchmark em APIs externas e gera o relatorio Excel final.

## Relatorio Excel — Abas e Graficos

O `carteira_report.xlsx` gerado contem 8 abas:

### 1. Resumo (Dashboard)

- Indicadores principais: valor total da carteira, total investido, ganho/perda absoluto, % de retorno
- Composicao por classe de ativo (Acoes & ETFs, Tesouro Direto, Renda Fixa) com percentuais
- Total de proventos recebidos por tipo de evento
- **Grafico de pizza**: alocacao da carteira por classe de ativo

### 2. Evolucao & Performance

- Tabela de evolucao patrimonial mensal (valor, investido, ganho/perda por mes)
- Comparacao de retorno acumulado: **Carteira vs CDI vs IBOV vs DIVO11**
- **Grafico de linha**: valor patrimonial ao longo do tempo por classe de ativo
- **Grafico de linha**: retorno acumulado comparado com CDI, IBOV e DIVO11

### 3. Acoes & ETFs

- Detalhamento por ticker da posicao atual: quantidade, preco medio de compra, preco de tela, valor investido, valor atual, lucro/prejuizo nao realizado, variacao %
- Linha de total com formulas SUM
- **Grafico de barras**: lucro/prejuizo por ativo

### 4. Proventos

- Resumo por tipo de evento (Dividendo, JCP, Rendimento)
- Resumo por ticker (ordenado por valor total recebido)
- Tabela de detalhamento completo com periodo, tipo de evento, valor, quantidade de acoes
- **Grafico de barras**: proventos por ticker

### 5. Negociacoes

- Todas as compras e vendas: ticker, periodo, instituicao, quantidades, precos, totais
- Resumo de volume mensal de compras e vendas
- **Grafico de barras**: volume mensal de negociacoes (compras vs vendas)

### 6. Tesouro Direto

- Posicoes atuais: produto, indexador, vencimento, investido, bruto, liquido, valor atual, ganho/perda
- **Grafico de linha**: investido vs valor atual ao longo dos meses

### 7. Renda Fixa

- Posicoes em CDB, LCI, LCA: produto, emissor, codigo, indexador, regime, datas de emissao/vencimento, quantidade, valor na curva

### 8. IR - Resumo (Declaracao de Imposto de Renda)

Dados consolidados para a declaracao anual de IRPF:
- **Bens e Direitos**: posicoes em acoes e ETFs no final do ano com custo de aquisicao (para a ficha "Bens e Direitos")
- **Rendimentos Isentos**: dividendos isentos de IR agrupados por ticker
- **Rendimentos Tributaveis**: JCP (Juros sobre Capital Proprio) tributaveis agrupados por ticker
- **Vendas Realizadas**: totais mensais de venda com **flag de isencao de R$20 mil** (destaca os meses em que o total de vendas ultrapassa o limite de isencao para acoes)

## Fontes de Dados de Benchmark

| Benchmark | Fonte | API |
|-----------|-------|-----|
| CDI | Banco Central do Brasil | `api.bcb.gov.br` — Serie 4391 (taxa CDI diaria) |
| IBOV | Yahoo Finance | `yfinance` — Ticker `^BVSP` |
| DIVO11 | Yahoo Finance | `yfinance` — Ticker `DIVO11.SA` |

Caso as APIs externas estejam indisponiveis (problemas de rede, limites de requisicao), o relatorio ainda e gerado com colunas de benchmark zeradas. Os dados da carteira nunca sao afetados.

## Concorrencia do DuckDB

O DuckDB e um banco de dados single-writer (apenas uma escrita por vez). Para evitar conflitos de lock quando o Cosmos renderiza multiplos modelos dbt como tasks paralelas no Airflow, o pipeline utiliza um **pool do Airflow** (`duckdb_single_writer`) com 1 slot. Isso serializa todas as escritas no DuckDB mantendo a visibilidade por modelo na interface do Airflow.

Configurado em `airflow_settings.yaml`:

```yaml
pools:
  - pool_name: duckdb_single_writer
    pool_slot: 1
    pool_description: Limita a concorrencia de escrita no DuckDB para 1 task por vez
```

## Consultando o DuckDB Diretamente

Para inspecionar os dados fora do pipeline, conecte-se ao DuckDB de dentro do container:

```bash
# Abrir um shell no container do scheduler
astro dev bash

# Consultar via Python
python3 -c "
import duckdb
con = duckdb.connect('/usr/local/airflow/dbt_data/b3_portfolio.duckdb', read_only=True)
print(con.execute('SHOW TABLES').fetchdf())
con.execute('SELECT * FROM fct_monthly_wallet ORDER BY _reference_period').fetchdf()
"
```

Ou instale o `duckdb` localmente e conecte-se ao arquivo em `dbt_data/b3_portfolio.duckdb`.

## Comandos Uteis

```bash
astro dev start          # Inicia o ambiente Airflow
astro dev stop           # Para os containers (preserva os dados)
astro dev restart        # Reconstroi e reinicia (apos alterar requirements.txt ou Dockerfile)
astro dev kill           # Forca a parada e remove os containers
astro dev bash           # Abre um shell no container do scheduler
astro dev logs           # Visualiza os logs dos containers
```

## Solucao de Problemas

| Problema | Solucao |
|----------|---------|
| `No xlsx files found` | Verifique se os arquivos estao no diretorio `files/` com o padrao de nome correto |
| Erro de lock do DuckDB | O pool deveria prevenir isso; verifique se `airflow_settings.yaml` tem o pool configurado e reinicie |
| Timeout do `yfinance` | Os dados de benchmark sao opcionais; o relatorio e gerado com colunas de benchmark vazias |
| Container reiniciando | Execute `astro dev logs` para verificar erros de importacao; confira se os pacotes do `requirements.txt` sao compativeis |
| Porta 8080 em uso | Pare outros servicos na porta 8080 ou configure uma porta diferente nas configuracoes do Astronomer |
| Erro de versao do Astro CLI | Atualize com `astro version upgrade` — requer >= 1.42.0 para runtime 3.2-4 |

## Dependencias

```
dbt-core          # Framework de transformacao SQL
dbt-duckdb        # Adaptador DuckDB para dbt
duckdb            # Banco de dados analitico embutido
astronomer-cosmos # Renderiza modelos dbt como tasks do Airflow
pandas            # Manipulacao de dados na ingestao
openpyxl          # Geracao de arquivos Excel
yfinance          # Dados de benchmark IBOV e DIVO11
requests          # Dados de CDI via API do BCB
```
