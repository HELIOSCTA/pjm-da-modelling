{{
  config(
    materialized='ephemeral'
  )
}}

---------------------------
-- PJM 2-Day Wind Forecast (normalized)
-- 10-min data aggregated to hourly
-- Grain: 1 row per forecast_execution_datetime × forecast_date × hour_ending
---------------------------

WITH TEN_MIN AS (
    SELECT
        DATE_TRUNC('hour', publish_time_utc) AS forecast_execution_datetime_utc
        ,'US/Eastern' AS timezone
        ,DATE_TRUNC('hour', publish_time_local) AS forecast_execution_datetime_local
        ,publish_time_local::DATE AS forecast_execution_date

        ,interval_start_local::DATE AS forecast_date
        ,EXTRACT(HOUR FROM interval_start_local)::INT + 1 AS hour_ending

        ,wind_forecast::NUMERIC AS wind_forecast

    FROM {{ source('gridstatus_v1', 'pjm_wind_forecast_hourly') }}
    WHERE
        publish_time_local::DATE >= (CURRENT_TIMESTAMP AT TIME ZONE 'MST')::DATE - 7
),

---------------------------
-- AGGREGATE 10-MIN TO HOURLY
---------------------------

HOURLY AS (
    SELECT
        forecast_execution_datetime_utc
        ,timezone
        ,forecast_execution_datetime_local
        ,forecast_execution_date

        ,forecast_date
        ,hour_ending

        ,AVG(wind_forecast) AS wind_forecast

    FROM TEN_MIN
    GROUP BY forecast_execution_datetime_utc, timezone, forecast_execution_datetime_local, forecast_execution_date, forecast_date, hour_ending
)

SELECT * FROM HOURLY
ORDER BY forecast_execution_datetime_local DESC, forecast_date, hour_ending
