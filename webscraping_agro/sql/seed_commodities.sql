-- =============================================================================
-- Commodities alinhadas ao FAO FFPI (subindices + indice geral)
-- O ETL deve mapear nomes normalizados do raw (ex.: "FOOD PRICE INDEX")
-- para commodity_code (ex.: FOOD_PRICE_INDEX).
-- =============================================================================

INSERT INTO processed.dim_commodity (commodity_code, display_name, category)
VALUES
    ('FOOD_PRICE_INDEX', 'Food Price Index', 'composite_index'),
    ('MEAT', 'Meat', 'livestock'),
    ('DAIRY', 'Dairy', 'livestock'),
    ('CEREALS', 'Cereals', 'crops'),
    ('OILS', 'Oils', 'oils_fats'),
    ('SUGAR', 'Sugar', 'sugar')
ON CONFLICT (commodity_code) DO NOTHING;
