{{
  config(
    materialized='ephemeral'
  )
}}

---------------------------
-- RT Hourly Wind Generation by Area (normalized)
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
    FROM {{ source('pjm_v1', 'wind_generation_by_area') }}
    WHERE area IN ('RTO', 'MIDATL', 'WEST', 'SOUTH')
      AND wind_generation_mw > 0
)

SELECT
    w.datetime_beginning_utc
    ,w.datetime_beginning_utc + INTERVAL '1 hour' AS datetime_ending_utc
    ,'US/Eastern' AS timezone
    ,w.datetime_beginning_ept AS datetime_beginning_local
    ,w.datetime_beginning_ept + INTERVAL '1 hour' AS datetime_ending_local
    ,w.datetime_beginning_ept::DATE AS date
    ,(EXTRACT(HOUR FROM w.datetime_beginning_ept) + 1)::INT AS hour_ending

    ,w.area AS region

    ,w.wind_generation_mw::NUMERIC AS wind_gen_mw

FROM {{ source('pjm_v1', 'wind_generation_by_area') }} w
CROSS JOIN WATERMARK wm
WHERE w.area IN ('RTO', 'MIDATL', 'WEST', 'SOUTH')
  AND w.datetime_beginning_utc <= wm.dt
