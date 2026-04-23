{{
  config(
    materialized='ephemeral'
  )
}}

---------------------------
-- Meteologica PJM Load Forecast -- DA Cutoff (bias-safe for training)
-- Latest forecast revision issued in the 48 hours ending at 10:00 AM EPT today,
-- picked per forecast_date x hour_ending x region.
-- Window (vs plain "issued today EPT pre-10 AM") is needed because Meteologica
-- forecasts only cover FUTURE hours from their issue time: the earliest
-- today-EPT issue is ~00:22 which already sits inside HE 1, so a today-only
-- filter never covers HE 1 of today. A multi-day lookback picks yesterday's
-- late-evening revision for HE 1 while still preferring today's latest for HE 2+.
-- 48h (vs 24h) keeps behavior consistent with solar/wind, which publish once
-- daily and need the wider window to fall back to yesterday's issue.
-- Grain: 1 row per forecast_date x hour_ending x region
---------------------------

WITH all_forecasts AS (
    SELECT * FROM {{ ref('meteologica_pjm_load_forecast_hourly') }}
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
    ,forecast_load_mw
FROM latest
WHERE rn = 1
