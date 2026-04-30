{{
  config(
    materialized='ephemeral'
  )
}}

---------------------------
-- PJM 7-Day Load Forecast — full captured history (from PJM API, by forecast area)
-- Same standardized shape as source_v1_pjm_seven_day_load_forecast, but without
-- the rolling 7-day filter — exposes every scrape preserved in the underlying
-- table (back to ~2025 when capture began).
-- Grain: 1 row per forecast_execution_datetime × forecast_date × hour_ending × region
---------------------------

SELECT
    evaluated_at_datetime_utc AS forecast_execution_datetime_utc
    ,'US/Eastern' AS timezone
    ,evaluated_at_datetime_ept AS forecast_execution_datetime_local
    ,evaluated_at_datetime_ept::DATE AS forecast_execution_date

    ,forecast_datetime_beginning_ept::DATE AS forecast_date
    ,EXTRACT(HOUR FROM forecast_datetime_beginning_ept)::INT + 1 AS hour_ending

    ,CASE forecast_area
        WHEN 'RTO_COMBINED' THEN 'RTO'
        WHEN 'MID_ATLANTIC_REGION' THEN 'MIDATL'
        WHEN 'WESTERN_REGION' THEN 'WEST'
        WHEN 'SOUTHERN_REGION' THEN 'SOUTH'
        ELSE forecast_area
    END AS region
    ,forecast_load_mw

FROM {{ source('pjm_v1', 'seven_day_load_forecast_v1_2025_08_13') }}
WHERE forecast_area IN ('RTO_COMBINED', 'MID_ATLANTIC_REGION', 'WESTERN_REGION', 'SOUTHERN_REGION')
