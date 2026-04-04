-- Borrar tablas antiguas para recrearlas limpias
DROP TABLE IF EXISTS clean.fact_trips;
DROP TABLE IF EXISTS clean.dim_vendor CASCADE;
DROP TABLE IF EXISTS clean.dim_payment_type CASCADE;
DROP TABLE IF EXISTS clean.dim_pickup_location CASCADE;
DROP TABLE IF EXISTS clean.dim_dropoff_location CASCADE;
DROP TABLE IF EXISTS clean.reporte_nulos;
DROP TABLE IF EXISTS clean.reporte_duplicados;
DROP TABLE IF EXISTS clean.reporte_consistencias;

-- Crear schema clean si no existe
CREATE SCHEMA IF NOT EXISTS clean;-- Docs: https://docs.mage.ai/guides/sql-blocks
