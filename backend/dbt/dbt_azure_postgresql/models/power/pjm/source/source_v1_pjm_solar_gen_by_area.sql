{{
  config(
    materialized='ephemeral'
  )
}}

---------------------------
-- RT Hourly Solar Generation by Area (normalized)
-- Grain: 1 row per datetime_beginning_utc x region
-- RFC and OTHER are dropped: RFC = MIDATL + WEST (redundant), OTHER is non-RTO.
--
-- PJM pads forward-dated and unreported hours with 0 instead of leaving rows
-- out (~2-day publication gap observed). Filter to <= the latest hour with
-- any non-zero value across in-scope regions ("publication watermark") so
-- those padding zeros don't reach downstream marts. The 5-min companion
-- feed fills the gap above the watermark.
---------------------------

WITH WATERMARK AS (
    SELECT MAX(datetime_beginning_utc) AS dt
    FROM {{ source('pjm_v1', 'solar_generation_by_area') }}
    WHERE area IN ('RTO', 'MIDATL', 'WEST', 'SOUTH')
      AND solar_generation_mw > 0
)

SELECT
    s.datetime_beginning_utc
    ,s.datetime_beginning_utc + INTERVAL '1 hour' AS datetime_ending_utc
    ,'US/Eastern' AS timezone
    ,s.datetime_beginning_ept AS datetime_beginning_local
    ,s.datetime_beginning_ept + INTERVAL '1 hour' AS datetime_ending_local
    ,s.datetime_beginning_ept::DATE AS date
    ,(EXTRACT(HOUR FROM s.datetime_beginning_ept) + 1)::INT AS hour_ending

    ,s.area AS region

    ,s.solar_generation_mw::NUMERIC AS solar_gen_mw

FROM {{ source('pjm_v1', 'solar_generation_by_area') }} s
CROSS JOIN WATERMARK w
WHERE s.area IN ('RTO', 'MIDATL', 'WEST', 'SOUTH')
  AND s.datetime_beginning_utc <= w.dt
