{{ config(
    materialized='table',
    engine='MergeTree()',
    order_by='(region)'
) }}

SELECT
    region,
    count() AS total_earthquakes,
    avg(magnitude) AS avg_magnitude,
    max(magnitude) AS max_magnitude,
    min(magnitude) AS min_magnitude,
    sum(if(tsunami = 1, 1, 0)) AS tsunami_events,
    avg(depth) AS avg_depth,
    min(load_date) AS first_date,
    max(load_date) AS last_date
FROM {{ source('raw', 'enriched_earthquakes') }}
GROUP BY region