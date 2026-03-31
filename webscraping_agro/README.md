# Webscraping Agro - Avaliacao Tecnica

Projeto base para a avaliacao tecnica de Analista de Dados (Webscraping):

- Estrutura do repositorio e camadas de dados (`raw`, `processed`, `curated`)
- Coleta externa (FAO Food Price Index) e persistencia na camada **raw**
- Documentacao da organizacao raw, formatos de arquivo e equivalente em S3

## 1) Estrutura do projeto

```text
webscraping_agro/
  data/
    raw/
      fao/
        food_price_index/
          ingested_at=YYYY-MM-DD/
            run_id=YYYYMMDD_HHMMSS/
              records.csv
              records.json
              records.parquet
              _manifest.json
    processed/
    curated/
  docs/
    images/
  sql/
  streamlit_app.py
  src/
    scraping/
    etl/
    analysis/
  tests/
  requirements.txt
  .env.example
  .gitignore
  README.md
```

## 2) Fonte de dados escolhida (Dia 1)

Para o inicio da etapa de scraping, foi utilizada uma fonte publica da FAO com descoberta dinamica do arquivo CSV mensal a partir de uma pagina HTML:

- Pagina base: [https://www.fao.org/worldfoodsituation/foodpricesindex/en/](https://www.fao.org/worldfoodsituation/foodpricesindex/en/)

> Observacao: o nome do arquivo CSV muda periodicamente. O scraper foi implementado para encontrar o link automaticamente na pagina.

## 3) Setup rapido

### Windows (PowerShell)

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

### Executar coleta raw

```powershell
python -m src.scraping.run_scraper
```

Cada execucao cria uma pasta de **run** com:

- `records.csv`, `records.json`, `records.parquet` (mesmo conteudo tabular)
- `_manifest.json` (metadados da ingestao: fonte, URL, horario UTC, contagem de linhas)

### PostgreSQL — criar tabelas (camada processed)

Arquivos em `sql/` (rodar nesta ordem):

1. `ddl.sql` — schema `processed`, dimensoes e fato mensal.
2. `seed_reference.sql` — regioes de referencia (inclui `GLOBAL` para FAO).
3. `seed_commodities.sql` — categorias do indice FAO.
4. `queries.sql` — consultas da questao 6 (LAG, top 5, anomalias).
5. `index_optimization.sql` — plano de performance e indices da questao 7.

Exemplo com `psql`:

```powershell
psql -h localhost -U postgres -d agro -f sql/ddl.sql
psql -h localhost -U postgres -d agro -f sql/seed_reference.sql
psql -h localhost -U postgres -d agro -f sql/seed_commodities.sql
psql -h localhost -U postgres -d agro -f sql/queries.sql
psql -h localhost -U postgres -d agro -f sql/index_optimization.sql
```

(Ajuste host, usuario e nome do banco conforme seu `.env`.)

### Executar ETL (raw -> processed em arquivo)

```powershell
python -m src.etl.run_etl
```

Saidas:

- `data/processed/fao/food_price_index/ingested_at=.../run_id=.../prices_processed.csv`
- `data/processed/fao/food_price_index/ingested_at=.../run_id=.../prices_processed.parquet`

### Executar ETL + carga no PostgreSQL

```powershell
python -m src.etl.run_etl --load-postgres
```

### Dashboard Streamlit (visualizacao + EDA)

Requer `prices_processed.parquet` na camada processed (rode o ETL antes). Na pasta `webscraping_agro`:

```powershell
streamlit run streamlit_app.py
```

A aplicacao carrega o Parquet **mais recente** por padrao. Ha duas abas: **Dashboard** (linhas, barras, boxplot interativos com Plotly) e **Analise exploratoria** (estatisticas descritivas, outliers IQR, graficos Matplotlib).

## 4) Passo 2 — Camada Raw (organizacao local)

### 4.1 Convencao de diretorios

A camada **raw** segue particoes nomeadas por **data de ingestao** e **identificador do run**, espelhando o que se faria em objeto storage:

- `ingested_at=` data do lote (dia no formato `YYYY-MM-DD` no momento da execucao).
- `run_id=` carimbo `YYYYMMDD_HHMMSS` para idempotencia e reprocessamento (cada execucao do scraper gera um run novo).

Isso permite reter historico de ingestoes sem sobrescrever arquivos, auditar qual coleta alimentou o ETL e alinhar com particoes em bucket.

### 4.2 Arquivos no run (`_manifest.json`)

O manifest registra `source_system`, `source_landing_url`, `ingested_at_utc`, `row_count` e nomes dos artefatos. Em cenario real, ele vincula o lote raw a jobs orquestrados ou a uma tabela de controle (`ingestion_log`).

### 4.3 CSV, JSON e Parquet — vantagens e escolha

| Formato | Vantagens principais | Limitacoes |
|--------|----------------------|------------|
| **CSV** | Universal, legivel por humanos e por qualquer ferramenta; facil inspecionar para volumes pequenos. | Sem tipagem forte; parse de datas/decimais pode variar; compressao pior que binario. |
| **JSON** | Estrutura flexivel (lista de objetos); bom para APIs e pipelines JSON; aninha campos. | Arquivo maior que CSV para tabelas “flat”; menos eficiente para analytics direto no arquivo bruto. |
| **Parquet** | Colunar, tipado, compacto; leitura seletiva por coluna; padrao em data lakes com Spark/Pandas/DuckDB. | Binario; exige libs (ex.: pyarrow). |

**Justificativa deste projeto:** na **raw** mantemos **CSV + JSON** para transparencia, debug e aderencia ao escopo do teste. Incluimos **Parquet** no mesmo run para deixar explicita a ponte para consumo analitico e para a camada **processed** (onde Parquet costuma ser preferido por custo e performance). Os tres formatos sao complementares, nao excludentes.

### 4.4 Equivalente em AWS S3

```text
s3://<bucket-empresa>-datalake/
  raw/
    source=fao/
      dataset=food_price_index/
        ingested_at=2026-03-27/
          run_id=20260327_143000/
            records.csv
            records.json
            records.parquet
            _manifest.json
```

Boas praticas: prefixos `raw/`, `processed/`, `curated/` separados; particoes previsiveis para filtros e lifecycle; manifest junto aos dados para rastreio ou catalogo (Glue); criptografia e bucket privado em producao.

## 5) O que ja foi entregue (resumo)

- Coleta FAO com descoberta do CSV na pagina e parse do arquivo com linhas de metadados.
- Camada raw particionada por `ingested_at` e `run_id`, com CSV, JSON, Parquet e manifest.
- Testes unitarios na transformacao dos registros, ETL e funcoes de EDA (IQR).
- Dashboard Streamlit (`streamlit_app.py`): filtros por produto, regiao e periodo; graficos interativos e aba de EDA (Pandas/Matplotlib).

## 6) Proximos passos (Dia 2+)

- Validar carga PostgreSQL em ambiente local com evidencias (prints SQL).
- Incluir no GitHub prints das consultas SQL e do Streamlit (requisito da avaliacao).
- Documentar insights de negocio, limitacoes da fonte e a proposta de uso no agro.
- (Opcional) Popular camada `curated` com agregados prontos para consumo externo.
