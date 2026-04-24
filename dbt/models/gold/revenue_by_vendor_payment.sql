{{ config(
    materialized='incremental',
    unique_key=['year', 'month', 'vendor_id', 'payment_type'],
    incremental_strategy='merge'
) }}

WITH trips AS (
    SELECT * FROM {{ ref('yellow_taxi') }}
    WHERE date_mismatch_flag = FALSE

    {% if is_incremental() %}
    AND pickup_ts > (
        SELECT COALESCE(DATEADD(day, -3, MAX(max_pickup_ts)), '1900-01-01'::TIMESTAMP_NTZ)
        FROM {{ this }}
    )
    {% endif %}
)

SELECT
    c.year,
    c.month,
    t.vendor_id,
    v.vendor_name,
    t.payment_type,
    p.payment_name,
    COUNT(*)                                                    AS total_trips,
    SUM(t.total_amount)                                         AS total_revenue,
    SUM(t.tip_amount)                                           AS total_tips,
    AVG(t.fare_amount)                                          AS avg_fare,
    SUM(CASE WHEN t.fare_amount < 0 THEN 1 ELSE 0 END)          AS total_corrections,
    MAX(t.pickup_ts)                                            AS max_pickup_ts

FROM trips t
LEFT JOIN {{ ref('dim_vendor') }} v
    ON t.vendor_id = v.vendor_id
LEFT JOIN {{ ref('dim_payment_type') }} p
    ON t.payment_type = p.payment_type
LEFT JOIN {{ ref('dim_calendar') }} c
    ON t.pickup_date = c.calendar_date
GROUP BY
    c.year,
    c.month,
    t.vendor_id,
    v.vendor_name,
    t.payment_type,
    p.payment_name
