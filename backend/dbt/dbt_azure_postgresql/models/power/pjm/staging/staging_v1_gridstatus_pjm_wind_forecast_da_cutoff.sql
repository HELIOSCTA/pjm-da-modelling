{{
  config(
    materialized='ephemeral'
  )
}}

---------------------------
-- PJM 2-Day Wind Forecast — DA Cutoff (bias-safe for training)
-- Latest forecast revision issued before 10:00 AM EPT per forecast_date × hour_ending
-- Grain: 1 row per forecast_date × hour_ending
---------------------------

WITH all_forecasts AS (
    SELECT * FROM {{ ref('pjm_gridstatus_wind_forecast_hourly') }}
    WHERE
        forecast_execution_datetime_local::TIME <= '10:00:00'
        AND forecast_execution_datetime_local::DATE = (CURRENT_TIMESTAMP AT TIME ZONE 'US/Eastern')::DATE
),

-- ────── Latest pre-10 AM EPT revision per forecast_date × hour_ending ──────

latest AS (
    SELECT
        *
        ,ROW_NUMBER() OVER (
            PARTITION BY forecast_date, hour_ending
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
    ,wind_forecast
FROM latest
WHERE rn = 1
