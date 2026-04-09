{{ config(materialized='table') }}

SELECT
    vendor_id,
    vendor_name

FROM (
    VALUES
        (1, 'Creative Mobile Technologies, LLC'),
        (2, 'Curb Mobility, LLC'),
        (6, 'Myle Technologies Inc'),
        (7, 'Helix'),
        (999, 'Unknown')
) AS t(vendor_id, vendor_name)