{{ config(
    materialized='table',
    engine='MergeTree()',
    order_by='(load_date, region)',
    partition_by='toYYYYMM(load_date)'
) }}

SELECT
    load_date,
    region,
    count() AS earthquakes_count,
    avg(magnitude) AS avg_magnitude,
    max(magnitude) AS max_magnitude,
    min(magnitude) AS min_magnitude,
    sum(if(tsunami = 1, 1, 0)) AS tsunami_count,
    avg(depth) AS avg_depth
FROM {{ source('raw', 'enriched_earthquakes') }}
GROUP BY
    load_date,
    region