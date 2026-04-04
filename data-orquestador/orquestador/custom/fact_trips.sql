-- Docs: https://docs.mage.ai/guides/sql-blocks
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
    trip_duration_minutes   NUMERIC,
    raw_year                INTEGER,
    raw_month               INTEGER
);