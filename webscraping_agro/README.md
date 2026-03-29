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
  src/
    scraping/
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

Exemplo com `psql`:

```powershell
psql -h localhost -U postgres -d agro -f sql/ddl.sql
psql -h localhost -U postgres -d agro -f sql/seed_reference.sql
psql -h localhost -U postgres -d agro -f sql/seed_commodities.sql
```

(Ajuste host, usuario e nome do banco conforme seu `.env`.)

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
- Testes unitarios na transformacao dos registros.

## 6) Proximos passos (Dia 2+)

- Modelagem SQL no PostgreSQL (`commodities`, `regions`, `prices`).
- ETL para corrigir tipos, nulos e padronizacao de categorias.
- Carga em camada `processed`.
- Consultas analiticas e dashboard Streamlit.

Pastas `src/etl`, `src/db` e `src/app` serao criadas quando essas etapas existirem no codigo (evita pacotes vazios no repositorio).
