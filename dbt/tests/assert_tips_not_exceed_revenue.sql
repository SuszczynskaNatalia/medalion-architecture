{{ config(severity = 'warn') }}

SELECT
    year,
    month,
    pickup_zone,
    total_tips,
    total_revenue
FROM {{ ref('revenue_by_zone') }}
WHERE total_tips > total_revenue