-- =============================================================================
-- PostgreSQL — modelo normalizado (camada processed)
-- Projeto: webscraping_agro / avaliacao tecnica
--
-- Granularidade do fato: uma linha por (commodity, regiao, mes de referencia).
-- Fonte principal: FAO Food Price Index (indice agregado; regiao padrao GLOBAL).
-- PKs/FKs:
--   - dim_commodity / dim_region: chaves surrogate (SERIAL) para estabilidade
--     quando o codigo de negocio mudar ou houver correcao de grafia.
--   - fact_price_monthly: PK surrogate observation_id; FKs garantem integridade
--     referencial; UNIQUE evita duplicata logica da mesma serie temporal.
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS processed;

-- -----------------------------------------------------------------------------
-- Regiao (permite expandir com dados manuais: estados, portos, paises)
-- -----------------------------------------------------------------------------
CREATE TABLE processed.dim_region (
    region_id   SERIAL PRIMARY KEY,
    region_code VARCHAR(32)  NOT NULL UNIQUE,
    region_name VARCHAR(128) NOT NULL,
    country_iso CHAR(2)      NULL,
    notes       TEXT         NULL
);

COMMENT ON TABLE processed.dim_region IS
    'Dimensao geografica. FAO FFPI e global; codigo GLOBAL para serie principal.';

COMMENT ON COLUMN processed.dim_region.region_id IS
    'PK surrogate: identificador interno estavel.';

COMMENT ON COLUMN processed.dim_region.region_code IS
    'Codigo de negocio unico (ex.: GLOBAL, BR_SOUTH, US_CBOT).';

-- -----------------------------------------------------------------------------
-- Commodity / categoria de produto (indices FAO: Cereals, Meat, etc.)
-- -----------------------------------------------------------------------------
CREATE TABLE processed.dim_commodity (
    commodity_id   SERIAL PRIMARY KEY,
    commodity_code VARCHAR(64)  NOT NULL UNIQUE,
    display_name   VARCHAR(128) NOT NULL,
    category       VARCHAR(64)  NULL,
    notes          TEXT         NULL
);

COMMENT ON TABLE processed.dim_commodity IS
    'Dimensao produto/categoria alinhada ao pipeline (nome normalizado em maiusculas).';

COMMENT ON COLUMN processed.dim_commodity.commodity_code IS
    'Codigo unico alinhado ao ETL (ex.: CEREALS, FOOD_PRICE_INDEX).';

-- -----------------------------------------------------------------------------
-- Fato: valor de preco/indice por mes
-- -----------------------------------------------------------------------------
CREATE TABLE processed.fact_price_monthly (
    observation_id    BIGSERIAL PRIMARY KEY,
    commodity_id      INTEGER        NOT NULL
        REFERENCES processed.dim_commodity (commodity_id)
        ON DELETE RESTRICT ON UPDATE CASCADE,
    region_id         INTEGER        NOT NULL
        REFERENCES processed.dim_region (region_id)
        ON DELETE RESTRICT ON UPDATE CASCADE,
    -- Primeiro dia do mes (YYYY-MM-01) derivado de ref_period (ex.: 1990-01)
    price_month       DATE           NOT NULL,
    price_value       NUMERIC(14, 4) NOT NULL,
    currency_code     VARCHAR(16)    NOT NULL DEFAULT 'INDEX_POINTS',
    unit              VARCHAR(32)    NOT NULL DEFAULT 'index_points',
    source_system     VARCHAR(64)    NOT NULL,
    source_url        TEXT           NULL,
    ingested_at       TIMESTAMPTZ    NOT NULL DEFAULT NOW(),
    -- Rastreio ao raw (record_id do arquivo); nao e UNIQUE para permitir
    -- reprocessamento com mesma chave de negocio (grain abaixo).
    source_record_id  VARCHAR(256)   NOT NULL,
    CONSTRAINT chk_price_month_first_day CHECK (EXTRACT(DAY FROM price_month) = 1),
    CONSTRAINT chk_price_value_non_negative CHECK (price_value >= 0)
);

COMMENT ON TABLE processed.fact_price_monthly IS
    'Fato mensal: indice ou preco apos ETL. Uma linha por commodity-regiao-mes.';

COMMENT ON COLUMN processed.fact_price_monthly.source_record_id IS
    'Chave natural vinda do raw (record_id); UNIQUE garante reexecucao idempotente.';

CREATE UNIQUE INDEX uq_fact_price_grain
    ON processed.fact_price_monthly (commodity_id, region_id, price_month, source_system);

-- Consultas por periodo e produto (Streamlit / LAG mensal)
CREATE INDEX idx_fact_price_month_commodity
    ON processed.fact_price_monthly (price_month, commodity_id);

CREATE INDEX idx_fact_price_region_month
    ON processed.fact_price_monthly (region_id, price_month);
