{{
  config(
    materialized='ephemeral'
  )
}}

---------------------------
-- PJM 7-Day Outage Forecast (normalized)
-- Grain: 1 row per forecast_execution_date × forecast_date × region
---------------------------

WITH RAW AS (
    SELECT
        forecast_execution_date::DATE AS forecast_execution_date
        ,forecast_date::DATE AS forecast_date
        ,region
        ,total_outages_mw::NUMERIC AS total_outages_mw
        ,planned_outages_mw::NUMERIC AS planned_outages_mw
        ,maintenance_outages_mw::NUMERIC AS maintenance_outages_mw
        ,forced_outages_mw::NUMERIC AS forced_outages_mw

    FROM {{ source('pjm_v1', 'seven_day_outage_forecast') }}
    WHERE
        EXTRACT(YEAR FROM forecast_execution_date::DATE) >= 2020
),

---------------------------
-- MAP REGION NAMES
---------------------------

MAPPED AS (
    SELECT
        forecast_execution_date
        ,forecast_date

        ,CASE region
            WHEN 'PJM RTO' THEN 'RTO'
            WHEN 'Mid Atlantic - Dominion' THEN 'MIDATL_DOM'
            WHEN 'Western' THEN 'WEST'
            ELSE region
        END AS region

        ,total_outages_mw
        ,planned_outages_mw
        ,maintenance_outages_mw
        ,forced_outages_mw

    FROM RAW
)

SELECT * FROM MAPPED
ORDER BY forecast_execution_date DESC, forecast_date, region
