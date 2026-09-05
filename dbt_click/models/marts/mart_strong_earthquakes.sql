{{ config(
    materialized='table',
    engine='MergeTree()',
    order_by='(load_date, magnitude)',
    partition_by='toYYYYMM(load_date)'
) }}

SELECT
    id,
    ts,
    place,
    region,
    magnitude,
    felt,
    tsunami,
    url,
    longitude,
    latitude,
    depth,
    load_date
FROM {{ source('raw', 'enriched_earthquakes') }}
WHERE magnitude >= 5.0