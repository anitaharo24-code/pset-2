-- Docs: https://docs.mage.ai/guides/sql-blocks
CREATE TABLE clean.reporte_nulos AS
SELECT
    COUNT(*) as total_filas,
    COUNT(*) FILTER (WHERE vendor_id IS NULL) as nulos_vendor,
    COUNT(*) FILTER (WHERE passenger_count IS NULL) as nulos_pasajeros,
    COUNT(*) FILTER (WHERE trip_distance IS NULL) as nulos_distancia,
    COUNT(*) FILTER (WHERE fare_amount IS NULL) as nulos_tarifa,
    COUNT(*) FILTER (WHERE total_amount IS NULL) as nulos_total,
    COUNT(*) FILTER (WHERE payment_type IS NULL) as nulos_payment,
    COUNT(*) FILTER (WHERE pu_location_id IS NULL) as nulos_pickup_loc,
    COUNT(*) FILTER (WHERE do_location_id IS NULL) as nulos_dropoff_loc,
    COUNT(*) FILTER (WHERE lpep_pickup_datetime IS NULL) as nulos_pickup_dt,
    COUNT(*) FILTER (WHERE lpep_dropoff_datetime IS NULL) as nulos_dropoff_dt,
    ROUND(COUNT(*) FILTER (WHERE vendor_id IS NULL) * 100.0 / COUNT(*), 2) as pct_vendor,
    ROUND(COUNT(*) FILTER (WHERE passenger_count IS NULL) * 100.0 / COUNT(*), 2) as pct_pasajeros,
    ROUND(COUNT(*) FILTER (WHERE trip_distance IS NULL) * 100.0 / COUNT(*), 2) as pct_distancia,
    ROUND(COUNT(*) FILTER (WHERE fare_amount IS NULL) * 100.0 / COUNT(*), 2) as pct_tarifa,
    ROUND(COUNT(*) FILTER (WHERE total_amount IS NULL) * 100.0 / COUNT(*), 2) as pct_total,
    ROUND(COUNT(*) FILTER (WHERE payment_type IS NULL) * 100.0 / COUNT(*), 2) as pct_payment
FROM raw.ny_taxi;