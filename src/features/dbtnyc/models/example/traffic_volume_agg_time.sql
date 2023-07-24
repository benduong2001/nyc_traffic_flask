{{ config(materialized='table') }}


WITH
traffic_volume_agg_directions AS (
SELECT
TV0."SegmentID"
, TV0."Yr"
, TV0."M"
, TV0."D"
, TV0."HH"
, TV0."MM"
, SUM(TV0."Vol") AS "Vol"
FROM {{ ref('traffic_volume_cleaned') }} TV0
GROUP BY
TV0."SegmentID"
, TV0."Yr"
, TV0."M"
, TV0."D"
, TV0."HH"
, TV0."MM"
)

-- cte
, traffic_volume_agg_time AS (
SELECT
TV0."SegmentID"
, TV0."Season"
, TV0."IsWeekend"
, TV0."HH"
, AVG(TV0."Vol") AS "Vol"
FROM traffic_volume_agg_directions TV0
GROUP BY
TV0."SegmentID"
, TV0."Season"
, TV0."IsWeekend"
, TV0."HH"
)
SELECT TVAT0.* FROM traffic_volume_agg_time TVAT0