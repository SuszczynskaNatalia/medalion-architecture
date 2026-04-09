{{ config(materialized='table') }}

SELECT
    ratecode_id,
    ratecode_name

FROM (
    VALUES
        (1, 'Standard rate'),
        (2, 'JFK'),
        (3, 'Newark'),
        (4, 'Nassau or Westchester'),
        (5, 'Negotiated fare'),
        (6, 'Group ride'),
        (99, 'Unknown')
) AS t(ratecode_id, ratecode_name)