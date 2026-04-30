{{
  config(
    materialized='ephemeral'
  )
}}

---------------------------
-- Meteologica PJM Wind Forecast -- DA Cutoff (bias-safe for training)
-- Latest forecast revision issued in the 48 hours ending at 10:00 AM EPT today,
-- picked per forecast_date x hour_ending x region.
-- Meteologica wind publishes once daily at ~09:47 EPT. The 48h window captures
-- today's issue when it has landed and falls back to yesterday's when it has not.
-- Still bias-safe: cap at today 10 AM EPT ensures no post-cutoff info leaks in.
-- Grain: 1 row per forecast_date x hour_ending x region
---------------------------

WITH all_forecasts AS (
    SELECT * FROM {{ ref('meteologica_pjm_wind_forecast_hourly') }}
    WHERE
        forecast_execution_datetime_local <= (
            ((CURRENT_TIMESTAMP AT TIME ZONE 'US/Eastern')::DATE) + TIME '10:00:00'
        )
        AND forecast_execution_datetime_local > (
            ((CURRENT_TIMESTAMP AT TIME ZONE 'US/Eastern')::DATE) + TIME '10:00:00' - INTERVAL '48 hours'
        )
        AND forecast_date >= (CURRENT_TIMESTAMP AT TIME ZONE 'US/Eastern')::DATE
),

-- Latest pre-10 AM EPT revision per forecast_date x hour_ending x region

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
    ,wind_forecast
FROM latest
WHERE rn = 1
