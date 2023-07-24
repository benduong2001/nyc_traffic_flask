{{ config(materialized='table') }}


WITH
street_segment_buffer_landuses_pivoted AS (
SELECT
SSBL0.Segment_ID
, SUM(CASE WHEN (SSBL0.LandUse = '01') THEN 1 ELSE 0 END) AS LandUse_01
, SUM(CASE WHEN (SSBL0.LandUse = '02') THEN 1 ELSE 0 END) AS LandUse_02
, SUM(CASE WHEN (SSBL0.LandUse = '03') THEN 1 ELSE 0 END) AS LandUse_03
, SUM(CASE WHEN (SSBL0.LandUse = '04') THEN 1 ELSE 0 END) AS LandUse_04
, SUM(CASE WHEN (SSBL0.LandUse = '05') THEN 1 ELSE 0 END) AS LandUse_05
, SUM(CASE WHEN (SSBL0.LandUse = '06') THEN 1 ELSE 0 END) AS LandUse_06
, SUM(CASE WHEN (SSBL0.LandUse = '07') THEN 1 ELSE 0 END) AS LandUse_07
, SUM(CASE WHEN (SSBL0.LandUse = '08') THEN 1 ELSE 0 END) AS LandUse_08
, SUM(CASE WHEN (SSBL0.LandUse = '09') THEN 1 ELSE 0 END) AS LandUse_09
, SUM(CASE WHEN (SSBL0.LandUse = '10') THEN 1 ELSE 0 END) AS LandUse_10
, SUM(CASE WHEN (SSBL0.LandUse = '11') THEN 1 ELSE 0 END) AS LandUse_11
, SUM(CASE WHEN ((SSBL0.LandUse IS NULL) OR (SSBL0.LandUse NOT IN ('01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11'))) THEN 1 ELSE 0 END) AS LandUse_NULL,
COUNT(*) AS tempNumLandUses
FROM {{  ref('street_segment_buffer_landuses')  }} SSBL0
GROUP BY SSBL0.Segment_ID
)


-- cte
, street_segment_geoenriched AS (
SELECT
SS1.Segment_ID
, SS1.StreetWidt
, SS1.Number_Tra
, SS1.SHAPE_Leng
, SSBLP0.LandUse_01/SSBLP0.tempNumLandUses AS LandUse_01
, SSBLP0.LandUse_02/SSBLP0.tempNumLandUses AS LandUse_02
, SSBLP0.LandUse_03/SSBLP0.tempNumLandUses AS LandUse_03
, SSBLP0.LandUse_04/SSBLP0.tempNumLandUses AS LandUse_04
, SSBLP0.LandUse_05/SSBLP0.tempNumLandUses AS LandUse_05
, SSBLP0.LandUse_06/SSBLP0.tempNumLandUses AS LandUse_06
, SSBLP0.LandUse_07/SSBLP0.tempNumLandUses AS LandUse_07
, SSBLP0.LandUse_08/SSBLP0.tempNumLandUses AS LandUse_08
, SSBLP0.LandUse_09/SSBLP0.tempNumLandUses AS LandUse_09
, SSBLP0.LandUse_10/SSBLP0.tempNumLandUses AS LandUse_10
, SSBLP0.LandUse_11/SSBLP0.tempNumLandUses AS LandUse_11
, SSBLP0.LandUse_NULL/SSBLP0.tempNumLandUses AS LandUse_NULL
FROM {{  ref('street_segment_cleaned')  }} SS1
LEFT JOIN
street_segment_buffer_landuses_pivoted SSBLP0
ON
SS1.Segment_ID = SSBLP0.Segment_ID
)
SELECT SSG0.* FROM street_segment_geoenriched SSG0