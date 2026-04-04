-- Docs: https://docs.mage.ai/guides/sql-blocks
-- Bloque: análisis de nulos
CREATE TABLE IF NOT EXISTS clean.reporte_nulos AS
SELECT
    COUNT(*) as total_filas,
    ROUND(COUNT(*) FILTER (WHERE vendor_id IS NULL) * 100.0 / COUNT(*), 2) as pct_nulos_vendor_id,
    ROUND(COUNT(*) FILTER (WHERE passenger_count IS NULL) * 100.0 / COUNT(*), 2) as pct_nulos_passenger_count,
    ROUND(COUNT(*) FILTER (WHERE trip_distance IS NULL) * 100.0 / COUNT(*), 2) as pct_nulos_trip_distance,
    ROUND(COUNT(*) FILTER (WHERE fare_amount IS NULL) * 100.0 / COUNT(*), 2) as pct_nulos_fare_amount,
    ROUND(COUNT(*) FILTER (WHERE total_amount IS NULL) * 100.0 / COUNT(*), 2) as pct_nulos_total_amount,
    ROUND(COUNT(*) FILTER (WHERE payment_type IS NULL) * 100.0 / COUNT(*), 2) as pct_nulos_payment_type,
    ROUND(COUNT(*) FILTER (WHERE pu_location_id IS NULL) * 100.0 / COUNT(*), 2) as pct_nulos_pu_location_id,
    ROUND(COUNT(*) FILTER (WHERE do_location_id IS NULL) * 100.0 / COUNT(*), 2) as pct_nulos_do_location_id
FROM raw.ny_taxi;