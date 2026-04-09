{{ config(materialized='table') }}

WITH bounds AS (
    SELECT
        MIN(pickup_date::DATE) AS min_date,
        MAX(pickup_date::DATE) AS max_date
    FROM {{ ref('yellow_taxi') }}
),

raw_numbers AS (
    SELECT ROW_NUMBER() OVER (ORDER BY NULL) - 1 AS seq
    FROM TABLE(GENERATOR(ROWCOUNT => 15000))
),

dates AS (
    SELECT 
        DATEADD(day, n.seq, b.min_date) AS calendar_date
    FROM raw_numbers n
    CROSS JOIN bounds b
    WHERE DATEADD(day, n.seq, b.min_date) <= b.max_date
)

SELECT
    calendar_date,
    YEAR(calendar_date)      AS year,
    MONTH(calendar_date)     AS month,
    WEEK(calendar_date)      AS week_of_year,
    DAYOFWEEK(calendar_date) AS day_of_week,
    CASE
        WHEN DAYOFWEEK(calendar_date) IN (1, 7) THEN TRUE
        ELSE FALSE
    END AS is_weekend

FROM dates
ORDER BY calendar_date