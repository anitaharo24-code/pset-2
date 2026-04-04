-- Docs: https://docs.mage.ai/guides/sql-blocks
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
    trip_duration_minutes,
    raw_year,
    raw_month
)
SELECT DISTINCT
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
    ROUND(CAST(EXTRACT(EPOCH FROM (
        r.lpep_dropoff_datetime - r.lpep_pickup_datetime
    ))/60 AS NUMERIC), 2),
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
  AND r.trip_distance < 500
  AND r.fare_amount > 0
  AND r.fare_amount < 1000
  AND r.total_amount > 0
  AND r.passenger_count > 0
  AND r.passenger_count <= 6
  AND r.lpep_pickup_datetime IS NOT NULL
  AND r.lpep_dropoff_datetime IS NOT NULL
  AND r.lpep_dropoff_datetime > r.lpep_pickup_datetime
  AND r.lpep_dropoff_datetime - r.lpep_pickup_datetime < INTERVAL '24 hours'
  AND r.vendor_id IS NOT NULL
  AND r.payment_type IS NOT NULL;