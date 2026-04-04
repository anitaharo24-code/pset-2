-- Docs: https://docs.mage.ai/guides/sql-blocks
CREATE TABLE IF NOT EXISTS clean.dim_vendor (
    vendor_key    SERIAL PRIMARY KEY,
    vendor_id     INTEGER UNIQUE,
    vendor_name   TEXT
);

INSERT INTO clean.dim_vendor (vendor_id, vendor_name)
SELECT DISTINCT
    vendor_id,
    CASE vendor_id
        WHEN 1 THEN 'Creative Mobile Technologies'
        WHEN 2 THEN 'VeriFone Inc'
        ELSE 'Desconocido'
    END
FROM raw.ny_taxi
WHERE vendor_id IS NOT NULL
ON CONFLICT DO NOTHING;

CREATE TABLE IF NOT EXISTS clean.dim_payment_type (
    payment_key     SERIAL PRIMARY KEY,
    payment_type_id INTEGER UNIQUE,
    payment_desc    TEXT
);

INSERT INTO clean.dim_payment_type (payment_type_id, payment_desc)
SELECT DISTINCT
    payment_type,
    CASE payment_type
        WHEN 1 THEN 'Credit card'
        WHEN 2 THEN 'Cash'
        WHEN 3 THEN 'No charge'
        WHEN 4 THEN 'Dispute'
        WHEN 5 THEN 'Unknown'
        WHEN 6 THEN 'Voided trip'
        ELSE 'Otro'
    END
FROM raw.ny_taxi
WHERE payment_type IS NOT NULL
ON CONFLICT DO NOTHING;

CREATE TABLE IF NOT EXISTS clean.dim_pickup_location (
    pickup_location_key  SERIAL PRIMARY KEY,
    pu_location_id       INTEGER UNIQUE
);

INSERT INTO clean.dim_pickup_location (pu_location_id)
SELECT DISTINCT pu_location_id
FROM raw.ny_taxi
WHERE pu_location_id IS NOT NULL
ON CONFLICT DO NOTHING;

CREATE TABLE IF NOT EXISTS clean.dim_dropoff_location (
    dropoff_location_key  SERIAL PRIMARY KEY,
    do_location_id        INTEGER UNIQUE
);

INSERT INTO clean.dim_dropoff_location (do_location_id)
SELECT DISTINCT do_location_id
FROM raw.ny_taxi
WHERE do_location_id IS NOT NULL
ON CONFLICT DO NOTHING;