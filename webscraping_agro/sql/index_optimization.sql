-- =============================================================================
-- Questao 7 — Otimizacao e indexacao
-- =============================================================================

-- 1) Garantir estatisticas atualizadas (antes de benchmark)
ANALYZE processed.fact_price_monthly;
ANALYZE processed.dim_commodity;
ANALYZE processed.dim_region;

-- 2) Medir plano/tempo da consulta 6.a (LAG mensal)
EXPLAIN (ANALYZE, BUFFERS, VERBOSE)
WITH monthly_avg AS (
    SELECT
        c.commodity_code,
        DATE_TRUNC('month', f.price_month)::date AS month_ref,
        AVG(f.price_value)::numeric(14, 4) AS avg_price
    FROM processed.fact_price_monthly f
    JOIN processed.dim_commodity c
      ON c.commodity_id = f.commodity_id
    GROUP BY 1, 2
)
SELECT
    commodity_code,
    month_ref,
    avg_price,
    LAG(avg_price) OVER (PARTITION BY commodity_code ORDER BY month_ref) AS prev_month_avg_price
FROM monthly_avg
ORDER BY commodity_code, month_ref;

-- 3) Medir plano/tempo da consulta 6.b (top 5 ultimo ano)
EXPLAIN (ANALYZE, BUFFERS, VERBOSE)
SELECT
    c.commodity_code,
    COUNT(*) AS obs_count_last_12m
FROM processed.fact_price_monthly f
JOIN processed.dim_commodity c
  ON c.commodity_id = f.commodity_id
WHERE f.price_month >= (DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '12 months')
GROUP BY c.commodity_code
ORDER BY obs_count_last_12m DESC
LIMIT 5;

-- 4) Medir plano/tempo da consulta 6.c (anomalias)
EXPLAIN (ANALYZE, BUFFERS, VERBOSE)
WITH stats AS (
    SELECT
        commodity_id,
        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY price_value) AS q1,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY price_value) AS q3
    FROM processed.fact_price_monthly
    GROUP BY commodity_id
)
SELECT
    f.observation_id
FROM processed.fact_price_monthly f
JOIN stats s
  ON s.commodity_id = f.commodity_id
WHERE
    f.price_value < 0
    OR f.price_value < (s.q1 - 1.5 * (s.q3 - s.q1))
    OR f.price_value > (s.q3 + 1.5 * (s.q3 - s.q1));

-- 5) Indices recomendados para os padroes de consulta
-- (alguns ja existem no DDL; IF NOT EXISTS evita erro em reexecucao)
CREATE INDEX IF NOT EXISTS idx_fact_price_commodity_month
    ON processed.fact_price_monthly (commodity_id, price_month);

CREATE INDEX IF NOT EXISTS idx_fact_price_month_only
    ON processed.fact_price_monthly (price_month);

-- Opcional para outliers em series maiores (faixas por commodity + preco)
CREATE INDEX IF NOT EXISTS idx_fact_price_commodity_value
    ON processed.fact_price_monthly (commodity_id, price_value);

-- 6) Recoletar estatisticas apos criar indices
ANALYZE processed.fact_price_monthly;
