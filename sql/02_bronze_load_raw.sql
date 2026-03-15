USE DATABASE TAXI_PROJECT;

-- tworzenie tabel --
CREATE OR REPLACE TABLE bronze.taxi_zones (
    LocationID INTEGER,
    Borough STRING,
    Zone STRING,
    service_zone STRING
);
USE DATABASE TAXI_PROJECT;
CREATE OR REPLACE TABLE bronze.yellow_taxi_raw (
    vendor_id INTEGER,
    tpep_pickup_datetime STRING,
    tpep_dropoff_datetime STRING,
    passenger_count INTEGER,
    trip_distance FLOAT,
    ratecode_id INTEGER,
    store_and_fwd_flag STRING,
    pulocationid INTEGER,
    dolocationid INTEGER,
    payment_type INTEGER,
    fare_amount FLOAT,
    extra FLOAT,
    mta_tax FLOAT,
    tip_amount FLOAT,
    tolls_amount FLOAT,
    improvement_surcharge FLOAT,
    total_amount FLOAT,
    congestion_surcharge FLOAT,
    airport_fee FLOAT,
    source_file STRING               -- plik, z którego pochodzi rekord
);


COPY INTO bronze.yellow_taxi_raw (vendor_id, tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count,
    trip_distance, ratecode_id, store_and_fwd_flag, pulocationid, dolocationid, payment_type,
    fare_amount, extra, mta_tax, tip_amount, tolls_amount, improvement_surcharge, total_amount,
    congestion_surcharge, airport_fee, source_file)
FROM (
    SELECT
        $1:VendorID::INTEGER,
        $1:tpep_pickup_datetime::STRING,
        $1:tpep_dropoff_datetime::STRING,
        $1:passenger_count::INTEGER,
        $1:trip_distance::FLOAT,
        $1:RatecodeID::INTEGER,
        $1:store_and_fwd_flag::STRING,
        $1:PULocationID::INTEGER,
        $1:DOLocationID::INTEGER,
        $1:payment_type::INTEGER,
        $1:fare_amount::FLOAT,
        $1:extra::FLOAT,
        $1:mta_tax::FLOAT,
        $1:tip_amount::FLOAT,
        $1:tolls_amount::FLOAT,
        $1:improvement_surcharge::FLOAT,
        $1:total_amount::FLOAT,
        $1:congestion_surcharge::FLOAT,
        $1:airport_fee::FLOAT,
        METADATA$FILENAME AS source_file
    FROM @BRONZE.NYC_TAXI_INTERNAL_STAGE
)
FILE_FORMAT = (TYPE=PARQUET);



COPY INTO bronze.taxi_zones
FROM @bronze.TAXI_ZONE_STAGE
FILE_FORMAT = (TYPE=CSV, FIELD_OPTIONALLY_ENCLOSED_BY='"', SKIP_HEADER=1);




