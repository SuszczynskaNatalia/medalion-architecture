{{ config(severity = 'warn') }}

-- Czy kolumna sumaryczna total_amount w miarę zgadza się ze zsumowaniem składowych?
-- Pozwalamy na błąd zaokrąglenia 0.05 centów. Poważniejsze rozjazdy to błąd bilingowy.

WITH amount_check AS (
    SELECT
        *,
        (fare_amount + extra + mta_tax + tip_amount + tolls_amount + improvement_surcharge) AS calculated_total
    FROM {{ ref('yellow_taxi') }}
)

SELECT * FROM amount_check
WHERE ABS(total_amount - calculated_total) > 0.50