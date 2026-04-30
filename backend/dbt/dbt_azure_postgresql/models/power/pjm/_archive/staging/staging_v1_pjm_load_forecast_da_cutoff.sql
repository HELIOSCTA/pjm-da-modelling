{{
  config(
    materialized='ephemeral'
  )
}}

---------------------------
-- PJM 7-Day Load Forecast — DA Cutoff (bias-safe for training)
-- Latest forecast revision issued before 10:00 AM EPT per forecast_date × hour_ending × region
-- Grain: 1 row per forecast_date × hour_ending × region
---------------------------

WITH all_forecasts AS (
    SELECT * FROM {{ ref('pjm_load_forecast_hourly') }}
    WHERE 
        forecast_execution_datetime_local::TIME <= '10:00:00'
        AND forecast_execution_datetime_local::DATE = (CURRENT_TIMESTAMP AT TIME ZONE 'US/Eastern')::DATE
),

-- ────── Latest pre-10 AM EPT revision per forecast_date × hour_ending × region ──────

latest AS (
    SELECT
        *
        ,ROW_NUMBER() OVER (
            PARTITION BY forecast_date, hour_ending, region
            ORDER BY forecast_execution_datetime_local DESC
        ) AS rn
    FROM all_forecasts
)

SELECT
    forecast_execution_datetime_utc
    ,timezone
    ,forecast_execution_datetime_local
    ,forecast_rank
    ,forecast_execution_date
    ,forecast_datetime
    ,forecast_date
    ,hour_ending
    ,region
    ,forecast_load_mw
FROM latest
WHERE rn = 1
