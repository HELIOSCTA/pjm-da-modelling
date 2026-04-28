{{
  config(
    materialized='ephemeral'
  )
}}

---------------------------
-- RT Hourly Solar Generation by Area (normalized)
-- Grain: 1 row per datetime_beginning_utc x region
-- RFC and OTHER are dropped: RFC = MIDATL + WEST (redundant), OTHER is non-RTO.
---------------------------

SELECT
    datetime_beginning_utc
    ,datetime_beginning_utc + INTERVAL '1 hour' AS datetime_ending_utc
    ,'US/Eastern' AS timezone
    ,datetime_beginning_ept AS datetime_beginning_local
    ,datetime_beginning_ept + INTERVAL '1 hour' AS datetime_ending_local
    ,datetime_beginning_ept::DATE AS date
    ,(EXTRACT(HOUR FROM datetime_beginning_ept) + 1)::INT AS hour_ending

    ,area AS region

    ,solar_generation_mw::NUMERIC AS solar_gen_mw

FROM {{ source('pjm_v1', 'solar_generation_by_area') }}
WHERE area IN ('RTO', 'MIDATL', 'WEST', 'SOUTH')
