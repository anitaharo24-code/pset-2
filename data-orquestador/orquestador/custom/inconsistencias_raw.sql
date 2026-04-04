-- Docs: https://docs.mage.ai/guides/sql-blocks
CREATE TABLE clean.reporte_consistencias AS
SELECT
    COUNT(*) as total_filas,
    COUNT(*) FILTER (WHERE trip_distance <= 0) as distancia_cero_negativa,
    COUNT(*) FILTER (WHERE trip_distance > 500) as distancia_sospechosa,
    COUNT(*) FILTER (WHERE fare_amount <= 0) as tarifa_invalida,
    COUNT(*) FILTER (WHERE passenger_count <= 0) as pasajeros_invalidos,
    COUNT(*) FILTER (WHERE passenger_count > 6) as pasajeros_excesivos,
    COUNT(*) FILTER (WHERE lpep_dropoff_datetime <= lpep_pickup_datetime) as fechas_inconsistentes,
    COUNT(*) FILTER (WHERE lpep_dropoff_datetime - lpep_pickup_datetime > INTERVAL '24 hours') as viaje_mayor_24h,
    ROUND(COUNT(*) FILTER (WHERE trip_distance <= 0) * 100.0 / COUNT(*), 2) as pct_distancia_invalida,
    ROUND(COUNT(*) FILTER (WHERE fare_amount <= 0) * 100.0 / COUNT(*), 2) as pct_tarifa_invalida,
    ROUND(COUNT(*) FILTER (WHERE lpep_dropoff_datetime <= lpep_pickup_datetime) * 100.0 / COUNT(*), 2) as pct_fechas_inconsistentes,
    ROUND(COUNT(*) FILTER (WHERE passenger_count > 6) * 100.0 / COUNT(*), 2) as pct_pasajeros_excesivos
FROM raw.ny_taxi;