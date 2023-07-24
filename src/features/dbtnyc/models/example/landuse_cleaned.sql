{{ config(materialized='table') }}


{% set projection = var('projection') %}


WITH
landuse_orig AS (
SELECT
L0.* FROM landuse L0
)

-- cte
, landuse_cleaned AS (
SELECT
ST_Transform(L0."geometry",{{projection}}) AS "geom"
, L0."LandUse"
FROM landuse_orig L0
)
SELECT LC0.* FROM landuse_cleaned LC0