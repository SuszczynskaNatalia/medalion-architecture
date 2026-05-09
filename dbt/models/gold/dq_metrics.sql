{{ config(materialized='incremental', unique_key=['run_at', 'metric_name'], incremental_strategy='merge') }}

-- Data Quality metrics calculated automatically on every dbt run.
-- Results are stored in GOLD.DQ_METRICS and accumulate over time (one snapshot per run).
-- Query: SELECT * FROM TAXI_PROJECT.GOLD.DQ_METRICS ORDER BY run_at DESC;

WITH now AS (
    -- Single call so all 5 metric rows share the exact same run_at timestamp
    SELECT CURRENT_TIMESTAMP() AS ts
),

agg AS (
    SELECT
        COUNT(*)                                                                AS total_rows,
        COUNT(DISTINCT trip_id)                                                 AS unique_ids,
        COUNT(pickup_ts)                                                        AS non_null_pickup_ts,
        SUM(
            CASE WHEN ABS(total_amount - (
                fare_amount + extra + mta_tax + tip_amount +
                tolls_amount + improvement_surcharge
            )) > 0.50 THEN 1 ELSE 0 END
        )                                                                       AS fare_math_invalid,
        MAX(valid_from)                                                         AS last_load_ts
    FROM {{ ref('yellow_taxi') }}
)

SELECT now.ts                           AS run_at,
       'Uniqueness — trip_id'           AS metric_name,
       ROUND(unique_ids * 100.0 / NULLIF(total_rows, 0), 4)
                                        AS current_value,
       '%'                              AS unit,
       100.0                            AS threshold,
       CASE WHEN unique_ids = total_rows THEN 'PASS' ELSE 'FAIL' END
                                        AS status
FROM agg CROSS JOIN now

UNION ALL

SELECT now.ts,
       'Completeness — pickup_ts',
       ROUND(non_null_pickup_ts * 100.0 / NULLIF(total_rows, 0), 4),
       '%', 99.0,
       CASE WHEN non_null_pickup_ts * 100.0 / NULLIF(total_rows, 0) >= 99.0
            THEN 'PASS' ELSE 'FAIL' END
FROM agg CROSS JOIN now

UNION ALL

SELECT now.ts,
       'Validity — fare math check',
       ROUND((1 - fare_math_invalid * 1.0 / NULLIF(total_rows, 0)) * 100, 4),
       '%', 99.5,
       CASE WHEN (1 - fare_math_invalid * 1.0 / NULLIF(total_rows, 0)) * 100 >= 99.5
            THEN 'PASS' ELSE 'WARN' END
FROM agg CROSS JOIN now

UNION ALL

SELECT now.ts,
       'Row count — total trips',
       total_rows,
       'rows', NULL,
       'INFO'
FROM agg CROSS JOIN now

UNION ALL

SELECT now.ts,
       'Freshness — hours since last load',
       DATEDIFF('hour', last_load_ts, now.ts),
       'hours', 25,
       CASE WHEN DATEDIFF('hour', last_load_ts, now.ts) <= 25
            THEN 'PASS' ELSE 'WARN' END
FROM agg CROSS JOIN now
