{{ config(materialized='table') }}


WITH

temp_street_segment_buffers AS (
SELECT SSB0.* FROM {{  ref('street_segment_buffers')  }} SSB0
)

, temp_landuse_cleaned AS (
SELECT LC0.* FROM {{  ref('landuse_cleaned')  }}  LC0
)

, street_segment_buffer_landuses_within AS (
SELECT
SSB0."Segment_ID",
LC0."LandUse"
FROM temp_street_segment_buffers SSB0
JOIN temp_landuse_cleaned LC0
ON ST_Contains(LC0."geom", SSB0."geom")
)

, street_segment_buffer_landuses_intersect AS (
SELECT
SSB0."Segment_ID",
LC0."LandUse"
FROM temp_street_segment_buffers SSB0
JOIN temp_landuse_cleaned  LC0
ON ST_Intersects(LC0."geom", SSB0."geom")
)

, street_segment_buffer_landuses AS (
SELECT 
SSBLW0."Segment_ID",
SSBLW0."LandUse"
FROM 
street_segment_buffer_landuses_within SSBLW0
UNION ALL
SELECT
SSBLI0."Segment_ID",
SSBLI0."LandUse"
FROM
street_segment_buffer_landuses_intersect SSBLI0

)
SELECT SSBL0.* FROM street_segment_buffer_landuses SSBL0