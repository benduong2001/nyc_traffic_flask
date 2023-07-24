{{ config(materialized='table') }}


{% set radius = var('radius') %}


WITH 
street_segment_buffers AS (
SELECT
SSC1."Segment_ID",
ST_BUFFER(SSC1."geom", {{radius}}) AS "geom"
FROM {{ ref('street_segment_cleaned') }} SSC1
)
SELECT SSB0.* FROM street_segment_buffers SSB0