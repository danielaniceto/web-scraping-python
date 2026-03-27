# Webscraping Agro - Avaliacao Tecnica

Projeto base para a avaliacao tecnica de Analista de Dados (Webscraping), com foco na entrega do Dia 1:

- Estrutura inicial do repositorio
- Coleta de dados externos (commodities)
- Persistencia da camada `raw` em CSV e JSON
- Documentacao de decisoes tecnicas

## 1) Estrutura do projeto

```text
webscraping_agro/
  data/
    raw/
      commodities/
    processed/
    curated/
  docs/
    images/
  sql/
  src/
    scraping/
    etl/
    db/
    app/
  tests/
  requirements.txt
  .env.example
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

Arquivos gerados:

- `data/raw/commodities/commodities_raw_YYYYMMDD_HHMMSS.csv`
- `data/raw/commodities/commodities_raw_YYYYMMDD_HHMMSS.json`

## 4) O que foi entregue no Dia 1

- Estruturacao inicial do projeto e camadas de dados.
- Scraper de commodities com tratamento basico e padronizacao inicial.
- Salvamento de dados brutos em dois formatos (CSV e JSON), pronto para etapa ETL.

## 5) Proximos passos (Dia 2+)

- Modelagem SQL no PostgreSQL (`commodities`, `regions`, `prices`).
- ETL para corrigir tipos, nulos e padronizacao de categorias.
- Carga em camada `processed`.
- Consultas analiticas e dashboard Streamlit.
