-- =============================================================================
-- Dados de referencia minimos (dim_region)
-- O ETL da FAO deve associar todas as linhas da fonte a region_code = GLOBAL.
-- Regioes adicionais podem ser usadas em cargas manuais (edital: diversidade).
-- =============================================================================

INSERT INTO processed.dim_region (region_code, region_name, country_iso, notes)
VALUES
    ('GLOBAL', 'Mundo (agregado)', NULL, 'Serie FAO Food Price Index e subindices; sem desagregacao geografica na fonte.'),
    ('BR_SOUTH', 'Sul do Brasil', 'BR', 'Exemplo para enriquecimento manual / segunda fonte.'),
    ('BR_CENTER_WEST', 'Centro-Oeste do Brasil', 'BR', 'Exemplo para enriquecimento manual.'),
    ('US_MIDWEST', 'Midwest (EUA)', 'US', 'Exemplo para precos em bolsa / cotacoes locais.')
ON CONFLICT (region_code) DO NOTHING;
