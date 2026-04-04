-- Docs: https://docs.mage.ai/guides/sql-blocks
CREATE TABLE clean.reporte_duplicados AS
SELECT
    COUNT(*) as total_filas,
    COUNT(*) - COUNT(DISTINCT(
        lpep_pickup_datetime::text ||
        vendor_id::text ||
        pu_location_id::text ||
        total_amount::text
    )) as filas_duplicadas,
    ROUND((COUNT(*) - COUNT(DISTINCT(
        lpep_pickup_datetime::text ||
        vendor_id::text ||
        pu_location_id::text ||
        total_amount::text
    ))) * 100.0 / COUNT(*), 2) as pct_duplicados
FROM raw.ny_taxi;