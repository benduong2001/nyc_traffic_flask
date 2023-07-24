{{ config(materialized='table') }}


WITH
traffic_volume_orig AS (
SELECT
TV0.* FROM traffic_volume TV0
)

-- cte
, traffic_volume_cleaned AS (
SELECT
TV0.*
, FLOOR(TV0."M" // 3) AS "Season"
--, EXTRACT(ISODOW FROM MAKE_DATE(TV0."Yr", TV0."M", TV0."D")) AS "DayOfWeek"
, (CASE WHEN (EXTRACT(ISODOW FROM MAKE_DATE(TV0."Yr", TV0."M", TV0."D")) < 6) THEN 0 ELSE 1 END) AS "IsWeekend"
FROM traffic_volume_orig TV0
)
SELECT TVC0.* FROM traffic_volume_cleaned TVC0