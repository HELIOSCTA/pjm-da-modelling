{{
  config(
    materialized='ephemeral'
  )
}}

---------------------------
-- PJM Outages Actual Daily (normalized)
-- Actuals only: forecast_execution_date = forecast_date
-- Grain: 1 row per date × region
---------------------------

WITH ACTUALS AS (
    SELECT
        forecast_execution_date AS date
        ,region
        ,total_outages_mw
        ,planned_outages_mw
        ,maintenance_outages_mw
        ,forced_outages_mw

    FROM {{ ref('source_v1_pjm_seven_day_outage_forecast') }}
    WHERE
        forecast_execution_date = forecast_date
)

SELECT * FROM ACTUALS
ORDER BY date DESC, region

