{{ config(materialized='table') }}


WITH
street_segment_landuse_traffic_volume AS (
SELECT
SSGE0.*,
TVAT0."Season",
TVAT0."IsWeekend",
TVAT0."HH"
FROM {{ ref('traffic_volume_agg_time')  }} TVAT0
LEFT JOIN {{ ref('street_segment_geoenriched')  }} SSGE0
ON TVT0."SegmentID" = SSGE0."Segment_ID"
)
SELECT SSLTV0.* FROM street_segment_landuse_traffic_volume SSLTV0