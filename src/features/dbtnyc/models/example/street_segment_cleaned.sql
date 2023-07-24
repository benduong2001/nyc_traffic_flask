{{ config(materialized='table') }}


{% set projection = var('projection') %}


{% set null_placeholder = var('null_placeholder') %}

WITH
street_segment_orig AS (
SELECT
SS0.* FROM street_segments SS0
)


-- cte
, street_segment_cleaned_0 AS (
SELECT
SS0."Segment_ID"
, SS0."SHAPE_Leng" AS "SHAPE_Leng"
, SS0."StreetWidt"::FLOAT AS "StreetWidt"
, SS0."Number_Tra"::INTEGER AS "Number_Tra"
, COALESCE(SS0."FeatureTyp", {{ null_placeholder }}) AS "FeatureTyp"
, COALESCE(SS0."RW_TYPE", {{ null_placeholder }}) AS "RW_TYPE"
, COALESCE(SS0."TrafDir", {{ null_placeholder }}) AS "TrafDir"
, COALESCE(SS0."Status", {{ null_placeholder }}) AS "Status"
, ST_Transform(SS0."geometry", {{ projection }}) AS "geom" 
FROM street_segment_orig SS0
)

-- cte
, street_segment_cleaned AS (
SELECT
SSC0."Segment_ID"
, SSC0."SHAPE_Leng"
, SSC0."StreetWidt"
, SSC0."Number_Tra"
, SSC0."FeatureTyp"
, SSC0."RW_TYPE"
, SSC0."TrafDir"
, SSC0."Status"
, SSC0."geom"
FROM street_segment_cleaned_0 SSC0
WHERE
SSC0."Segment_ID" IS NOT NULL
AND SSC0."StreetWidt" >= 8.0
AND SSC0."Number_Tra" >= 1
AND SSC0."RW_TYPE" IN ('1','2','3','4','9','11','13',{{ null_placeholder }})
AND SSC0."FeatureTyp" NOT IN ('F', 'A', 'W', '9', '8','2','5')
AND (SSC0."TrafDir" <> 'P' OR SSC0."TrafDir" <> {{ null_placeholder }})
AND (SSC0."Status" = '2' OR SSC0."Status" = {{ null_placeholder }})
)
SELECT SSC1.* FROM street_segment_cleaned SSC1