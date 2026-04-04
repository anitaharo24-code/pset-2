-- ── Tabla de hechos ──────────────────────────────────────
CREATE TABLE IF NOT EXISTS clean.fact_trips (
    trip_key                SERIAL PRIMARY KEY,
    vendor_key              INTEGER REFERENCES clean.dim_vendor(vendor_key),
    payment_key             INTEGER REFERENCES clean.dim_payment_type(payment_key),
    pickup_location_key     INTEGER REFERENCES clean.dim_pickup_location(pickup_location_key),
    dropoff_location_key    INTEGER REFERENCES clean.dim_dropoff_location(dropoff_location_key),
    pickup_datetime         TIMESTAMP,
    dropoff_datetime        TIMESTAMP,
    passenger_count         INTEGER,
    trip_distance           NUMERIC,
    fare_amount             NUMERIC,
    tip_amount              NUMERIC,
    tolls_amount            NUMERIC,
    total_amount            NUMERIC,
    raw_year                INTEGER,
    raw_month               INTEGER
);

INSERT INTO clean.fact_trips (
    vendor_key,
    payment_key,
    pickup_location_key,
    dropoff_location_key,
    pickup_datetime,
    dropoff_datetime,
    passenger_count,
    trip_distance,
    fare_amount,
    tip_amount,
    tolls_amount,
    total_amount,
    raw_year,
    raw_month
)
SELECT
    v.vendor_key,
    p.payment_key,
    pl.pickup_location_key,
    dl.dropoff_location_key,
    r.lpep_pickup_datetime,
    r.lpep_dropoff_datetime,
    r.passenger_count,
    r.trip_distance,
    r.fare_amount,
    r.tip_amount,
    r.tolls_amount,
    r.total_amount,
    r.raw_year,
    r.raw_month
FROM raw.ny_taxi r
LEFT JOIN clean.dim_vendor v
    ON r.vendor_id = v.vendor_id
LEFT JOIN clean.dim_payment_type p
    ON r.payment_type = p.payment_type_id
LEFT JOIN clean.dim_pickup_location pl
    ON r.pu_location_id = pl.pu_location_id
LEFT JOIN clean.dim_dropoff_location dl
    ON r.do_location_id = dl.do_location_id
WHERE r.trip_distance > 0
  AND r.fare_amount > 0
  AND r.total_amount > 0
  AND r.lpep_pickup_datetime IS NOT NULL
  AND r.lpep_dropoff_datetime IS NOT NULL
  AND r.lpep_dropoff_datetime > r.lpep_pickup_datetime;-- Docs: https://docs.mage.ai/guides/sql-blocks
