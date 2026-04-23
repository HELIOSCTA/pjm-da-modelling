{{
  config(
    materialized='ephemeral'
  )
}}

---------------------------
-- Meteologica: usa_pjm_midatlantic_pv_power_generation_forecast_hourly
-- Thin passthrough of raw Meteologica source table (schema: meteologica).
-- Grain: 1 row per update_id x forecast_period_start
---------------------------

SELECT
    content_id
    ,update_id
    ,issue_date
    ,forecast_period_start
    ,forecast_period_end
    ,forecast_mw
FROM {{ source('meteologica_pjm_v1', 'usa_pjm_midatlantic_pv_power_generation_forecast_hourly') }}
