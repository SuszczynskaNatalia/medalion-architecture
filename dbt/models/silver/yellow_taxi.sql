{{ config(
    materialized='incremental',
    unique_key='trip_id'
) }}

WITH source_data AS (
    SELECT *
    FROM {{ source('bronze', 'yellow_taxi_raw') }}
),

cleaned AS (

    SELECT
        coalesce(vendor_id, 999) AS vendor_id,
        TO_TIMESTAMP_NTZ(tpep_pickup_datetime::NUMBER / 1000000) AS pickup_ts,
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
        coalesce(payment_type, 5) AS payment_type,
        coalesce(fare_amount, 0) AS fare_amount,
        coalesce(extra, 0) AS extra,
        coalesce(mta_tax, 0) AS mta_tax,
        coalesce(tip_amount, 0) AS tip_amount,
        coalesce(tolls_amount, 0) AS tolls_amount,
        coalesce(congestion_surcharge, 0) AS congestion_surcharge,
        coalesce(improvement_surcharge, 0) AS improvement_surcharge,
        coalesce(airport_fee, 0) AS airport_fee,
        PULocationID,
        DOLocationID,
        CASE
            WHEN CAST(TO_TIMESTAMP_NTZ(tpep_pickup_datetime::NUMBER / 1000000) AS DATE)
                 > CAST(TO_TIMESTAMP_NTZ(tpep_dropoff_datetime::NUMBER / 1000000) AS DATE)
            THEN TRUE ELSE FALSE
        END AS date_mismatch_flag,
        CURRENT_TIMESTAMP() AS valid_from,
        TO_TIMESTAMP_NTZ('9999-12-31 23:59:59') AS valid_to,
        TRUE AS is_current,
        source_file,
        -- UNIKALNY KLUCZ
        MD5(
            COALESCE(vendor_id::STRING, '0') ||
            COALESCE(tpep_pickup_datetime::STRING, '') ||
            COALESCE(tpep_dropoff_datetime::STRING, '') ||
            COALESCE(source_file, '')
        ) AS trip_id
    FROM source_data

    WHERE coalesce(passenger_count, 0) > 0
      AND coalesce(trip_distance, 0) > 0
      AND tpep_pickup_datetime IS NOT NULL
      AND tpep_dropoff_datetime IS NOT NULL

),

deduplicated AS (
    SELECT *
    FROM cleaned
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY trip_id
        ORDER BY pickup_ts DESC
    ) = 1
)

SELECT *
FROM deduplicated
{% if is_incremental() %}
-- tylko nowe dane (z buforem 3 dni na late-arriving data)
WHERE pickup_ts > (SELECT DATEADD(day, -3, MAX(pickup_ts)) FROM {{ this }})

{% endif %}
