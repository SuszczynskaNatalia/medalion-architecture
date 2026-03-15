-- ========================================================
-- GOLD: Analiza przestrzenno-czasowa
-- ========================================================
CREATE OR REPLACE TABLE gold.yellow_taxi_revenue_by_zone_year_month AS
SELECT
    c.year,
    c.month,
    z_start.Zone AS pickup_zone,
    z_end.Zone AS dropoff_zone,
    COUNT(*) AS total_trips,
    SUM(total_amount) AS total_revenue,
    SUM(tip_amount) AS total_tips,
    AVG(trip_distance) AS avg_trip_distance,
    SUM(CASE WHEN fare_amount < 0 THEN 1 ELSE 0 END) AS total_corrections
FROM silver.yellow_taxi_clean t
LEFT JOIN silver.taxi_zones_clean z_start ON t.PULocationID = z_start.LocationID
LEFT JOIN silver.taxi_zones_clean z_end ON t.DOLocationID = z_end.LocationID
LEFT JOIN silver.calendar c ON t.pickup_date = c.calendar_date
WHERE t.is_current = TRUE
  AND t.date_mismatch_flag = FALSE
GROUP BY pickup_zone, dropoff_zone, c.year, c.month
ORDER BY 1, 2, total_revenue DESC;


-- ====================================================================================================
-- GOLD: Analiza biznesowo-finansowa – który vendor, taryfa i typ płatności generuje przychód i korekty
-- ====================================================================================================
CREATE OR REPLACE TABLE gold.yellow_taxi_revenue_by_vendor_payment AS
SELECT
    v.vendor_name,
    p.payment_name,
    r.ratecode_name,
    COUNT(*) AS total_trips,
    SUM(total_amount) AS total_revenue,
    SUM(tip_amount) AS total_tips,
    AVG(trip_distance) AS avg_trip_distance,
    SUM(CASE WHEN fare_amount < 0 THEN 1 ELSE 0 END) AS total_corrections
FROM silver.yellow_taxi_clean t
LEFT JOIN silver.vendor_lookup v ON t.vendor_id = v.vendor_id
LEFT JOIN silver.payment_lookup p ON t.payment_type = p.payment_type
LEFT JOIN silver.ratecode_lookup r ON t.ratecode_id = r.ratecode_id
WHERE t.is_current = TRUE
  AND t.date_mismatch_flag = FALSE
GROUP BY vendor_name, payment_name, ratecode_name
ORDER BY total_revenue DESC;