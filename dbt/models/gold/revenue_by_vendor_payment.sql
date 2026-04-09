{{ config(
    materialized='incremental',
    unique_key=['year', 'month', 'pickup_zone', 'dropoff_zone'],
    incremental_strategy='merge'
) }}

WITH trips AS (
    SELECT * FROM {{ ref('yellow_taxi') }}
    WHERE date_mismatch_flag = FALSE
      AND PULocationID NOT IN (264, 256)
      AND DOLocationID NOT IN (264, 256)

    {% if is_incremental() %}
    AND pickup_ts > (SELECT COALESCE(DATEADD(day, -3, MAX(max_pickup_ts)), '1900-01-01') FROM {{ this }})
    {% endif %}
)

SELECT
    c.year,
    c.month,
    COALESCE(z_start.zone, 'UNKNOWN') AS pickup_zone,
    COALESCE(z_end.zone, 'UNKNOWN')   AS dropoff_zone,
    COUNT(*)                                            AS total_trips,
    SUM(t.total_amount)                                 AS total_revenue,
    SUM(t.tip_amount)                                   AS total_tips,
    AVG(t.trip_distance)                                AS avg_trip_distance,
    SUM(CASE WHEN t.fare_amount < 0 THEN 1 ELSE 0 END)  AS total_corrections,
    MAX(t.pickup_ts)                                    AS max_pickup_ts

FROM trips t
LEFT JOIN {{ ref('dim_taxi_zones') }} z_start
    ON t.PULocationID = z_start.location_id
LEFT JOIN {{ ref('dim_taxi_zones') }} z_end
    ON t.DOLocationID = z_end.location_id
LEFT JOIN {{ ref('dim_calendar') }} c
    ON t.pickup_date = c.calendar_date
GROUP BY
    c.year,
    c.month,
    pickup_zone,
    dropoff_zone