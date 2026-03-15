-- vendor
CREATE OR REPLACE TABLE silver.vendor_lookup (
    vendor_id INTEGER,
    vendor_name STRING
);

INSERT INTO silver.vendor_lookup (vendor_id, vendor_name)
VALUES
    (1, 'Creative Mobile Technologies, LLC'),
    (2, 'Curb Mobility, LLC'),
    (6, 'Myle Technologies Inc'),
    (7, 'Helix'),
    (999, 'Unknown');


-- payment type
CREATE OR REPLACE TABLE silver.payment_lookup (
    payment_type INTEGER,
    payment_name STRING
);

INSERT INTO silver.payment_lookup (payment_type, payment_name)
VALUES
    (0, 'Flex Fare trip'),
    (1, 'Credit card'),
    (2, 'Cash'),
    (3, 'No charge'),
    (4, 'Dispute'),
    (5, 'Unknown'),
    (6, 'Voided trip');

CREATE OR REPLACE TABLE silver.ratecode_lookup (
    ratecode_id INTEGER,
    ratecode_name STRING
);

-- ratecode 
INSERT INTO silver.ratecode_lookup (ratecode_id, ratecode_name)
VALUES
    (1, 'Standard rate'),
    (2, 'JFK'),
    (3, 'Newark'),
    (4, 'Nassau or Westchester'),
    (5, 'Negotiated fare'),
    (6, 'Group ride'),
    (99, 'Unknown');
    
-- Silver Yellow Taxi Clean
CREATE OR REPLACE TABLE silver.yellow_taxi_clean AS
SELECT 
    coalesce(vendor_id, 999) vendor_id,
    CAST(TO_TIMESTAMP_NTZ(tpep_pickup_datetime::NUMBER / 1000000) AS DATE) AS pickup_date,
    TO_CHAR(TO_TIMESTAMP_NTZ(tpep_pickup_datetime::NUMBER / 1000000), 'HH24:MI:SS') AS pickup_time,
    CAST(TO_TIMESTAMP_NTZ(tpep_dropoff_datetime::NUMBER / 1000000) AS DATE) AS dropoff_date,
    TO_CHAR(TO_TIMESTAMP_NTZ(tpep_dropoff_datetime::NUMBER / 1000000), 'HH24:MI:SS') AS dropoff_time,
    passenger_count,
    trip_distance,
    ratecode_id,
    total_amount,
    CASE WHEN coalesce(fare_amount, 0) < 0 OR coalesce(total_amount, 0) < 0 THEN TRUE ELSE FALSE END AS fare_correction,
    CASE WHEN coalesce(improvement_surcharge, 0) < 0 THEN TRUE ELSE FALSE END AS improvement_correction,
    coalesce(payment_type, 5) as payment_type,
    coalesce(fare_amount, 0) fare_amount,
    coalesce(extra, 0) extra,
    coalesce(mta_tax, 0) mta_tax,
    coalesce(tip_amount, 0) tip_amount,
    coalesce(tolls_amount, 0) tolls_amount,
    coalesce(congestion_surcharge, 0) congestion_surcharge,
    improvement_surcharge,
    coalesce(airport_fee, 0) airport_fee, 
    PULocationID,
    DOLocationID,
    CASE WHEN (YEAR(pickup_date) <> TO_NUMBER(SUBSTR(source_file, 17, 4))
               or MONTH(pickup_date) <> TO_NUMBER(SUBSTR(source_file, 22, 2)))
        and (YEAR(dropoff_date) <> TO_NUMBER(SUBSTR(source_file, 17, 4))
                 or MONTH(dropoff_date) <> TO_NUMBER(SUBSTR(source_file, 22, 2)))
            THEN TRUE ELSE FALSE END date_mismatch_flag,
    CURRENT_TIMESTAMP() AS valid_from,
    TO_TIMESTAMP_NTZ('9999-12-31 23:59:59') AS valid_to,
    TRUE AS is_current,
    source_file
FROM bronze.yellow_taxi_raw
WHERE coalesce(passenger_count, 0) > 0
  AND coalesce(trip_distance, 0) > 0
  AND tpep_pickup_datetime IS NOT NULL
  AND tpep_dropoff_datetime IS NOT NULL;

-- Taxi Zones
CREATE OR REPLACE TABLE silver.taxi_zones_clean AS
SELECT
    LocationID,
    UPPER(Borough) AS Borough,
    UPPER(Zone) AS Zone,
    UPPER(Service_zone) AS Service_zone,
    CURRENT_TIMESTAMP() AS load_timestamp
FROM bronze.TAXI_ZONES
WHERE LocationID IS NOT NULL;

-- Calendar table
SET min_date = (SELECT MIN(CAST(pickup_date AS DATE)) FROM silver.yellow_taxi_clean);
SET max_date = (SELECT MAX(CAST(pickup_date AS DATE)) FROM silver.yellow_taxi_clean);
SET row_count = (SELECT DATEDIFF(day, $min_date, $max_date) + 1);

CREATE OR REPLACE TABLE silver.calendar AS
SELECT DATEADD(day, seq4(), $min_date) AS calendar_date,
       YEAR(DATEADD(day, seq4(), $min_date)) AS year,
       MONTH(DATEADD(day, seq4(), $min_date)) AS month,
       WEEK(DATEADD(day, seq4(), $min_date)) AS week_of_year,
       DAYOFWEEK(DATEADD(day, seq4(), $min_date)) AS day_of_week,
       CASE WHEN DAYOFWEEK(DATEADD(day, seq4(), $min_date)) IN (1,7) THEN TRUE ELSE FALSE END AS is_weekend
FROM TABLE(GENERATOR(ROWCOUNT => $row_count))
ORDER BY calendar_date;