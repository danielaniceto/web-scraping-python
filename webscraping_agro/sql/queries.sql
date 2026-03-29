-- =============================================================================
-- Questao 6 — Analises SQL (tendencias e indicadores)
-- Base: processed.fact_price_monthly + dimensoes
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 6.a) Preco medio mensal por commodity + variacao percentual vs mes anterior
--      (funcao LAG)
-- -----------------------------------------------------------------------------
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
    LAG(avg_price) OVER (
        PARTITION BY commodity_code
        ORDER BY month_ref
    ) AS prev_month_avg_price,
    ROUND(
        (
            (avg_price - LAG(avg_price) OVER (PARTITION BY commodity_code ORDER BY month_ref))
            / NULLIF(LAG(avg_price) OVER (PARTITION BY commodity_code ORDER BY month_ref), 0)
        ) * 100,
        2
    ) AS mom_pct_change
FROM monthly_avg
ORDER BY commodity_code, month_ref;


-- -----------------------------------------------------------------------------
-- 6.b) Top 5 produtos mais negociados no ultimo ano
--
-- Observacao de modelagem:
-- A fonte atual (FAO index) nao possui volume negociado. Como proxy tecnica
-- para o item do teste, usamos quantidade de observacoes no ultimo ano.
-- -----------------------------------------------------------------------------
SELECT
    c.commodity_code,
    COUNT(*) AS obs_count_last_12m,
    ROUND(AVG(f.price_value)::numeric, 4) AS avg_price_last_12m
FROM processed.fact_price_monthly f
JOIN processed.dim_commodity c
  ON c.commodity_id = f.commodity_id
WHERE f.price_month >= (DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '12 months')
GROUP BY c.commodity_code
ORDER BY obs_count_last_12m DESC, avg_price_last_12m DESC
LIMIT 5;


-- -----------------------------------------------------------------------------
-- 6.c) Registros anomalos
--      - preco negativo
--      - preco fora de faixa (IQR por commodity)
-- -----------------------------------------------------------------------------
WITH stats AS (
    SELECT
        commodity_id,
        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY price_value) AS q1,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY price_value) AS q3
    FROM processed.fact_price_monthly
    GROUP BY commodity_id
)
SELECT
    f.observation_id,
    c.commodity_code,
    r.region_code,
    f.price_month,
    f.price_value,
    CASE
        WHEN f.price_value < 0 THEN 'NEGATIVE_PRICE'
        WHEN f.price_value < (s.q1 - 1.5 * (s.q3 - s.q1))
          OR f.price_value > (s.q3 + 1.5 * (s.q3 - s.q1)) THEN 'IQR_OUTLIER'
        ELSE 'OK'
    END AS anomaly_type
FROM processed.fact_price_monthly f
JOIN stats s
  ON s.commodity_id = f.commodity_id
JOIN processed.dim_commodity c
  ON c.commodity_id = f.commodity_id
JOIN processed.dim_region r
  ON r.region_id = f.region_id
WHERE
    f.price_value < 0
    OR f.price_value < (s.q1 - 1.5 * (s.q3 - s.q1))
    OR f.price_value > (s.q3 + 1.5 * (s.q3 - s.q1))
ORDER BY c.commodity_code, f.price_month;
