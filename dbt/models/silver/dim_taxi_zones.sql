{{ config(
    materialized='table'
) }}

WITH source_data AS (
    SELECT * FROM {{ source('bronze', 'taxi_zones') }}
),

refined AS (
    SELECT
        CAST(LocationID AS INT) AS location_id,
        UPPER(COALESCE(NULLIF(TRIM(Borough), 'N/A'), 'UNKNOWN')) AS borough,
        UPPER(COALESCE(NULLIF(TRIM(Zone), 'N/A'), 'UNKNOWN')) AS zone,
        UPPER(COALESCE(NULLIF(TRIM(Service_zone), 'N/A'), 'UNKNOWN')) AS service_zone,
        CURRENT_TIMESTAMP() AS dbt_updated_at

    FROM source_data

    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY LocationID 
        ORDER BY 
            (CASE WHEN Borough = 'N/A' OR Borough IS NULL THEN 1 ELSE 0 END) ASC,
            LocationID ASC
    ) = 1
)

SELECT * FROM refined