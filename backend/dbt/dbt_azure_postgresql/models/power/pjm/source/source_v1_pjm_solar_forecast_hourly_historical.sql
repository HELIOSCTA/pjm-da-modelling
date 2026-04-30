{{
  config(
    materialized='ephemeral'
  )
}}

---------------------------
-- PJM 2-Day Solar Forecast — full captured history (GridStatus-sourced)
-- Same standardized shape as source_v1_gridstatus_pjm_solar_forecast_hourly
-- (sub-hourly publishes truncated to the hour and averaged to a single hourly
-- row per execution-hour × delivery-hour), but without the rolling 7-day
-- filter — exposes every scrape preserved in the underlying table.
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

        ,solar_forecast::NUMERIC AS solar_forecast
        ,solar_forecast_btm::NUMERIC AS solar_forecast_btm

    FROM {{ source('gridstatus_v1', 'pjm_solar_forecast_hourly') }}
),

---------------------------
-- AGGREGATE SUB-HOURLY PUBLISHES TO HOURLY
---------------------------

HOURLY AS (
    SELECT
        forecast_execution_datetime_utc
        ,timezone
        ,forecast_execution_datetime_local
        ,forecast_execution_date

        ,forecast_date
        ,hour_ending

        ,AVG(solar_forecast) AS solar_forecast
        ,AVG(solar_forecast_btm) AS solar_forecast_btm

    FROM TEN_MIN
    GROUP BY forecast_execution_datetime_utc, timezone, forecast_execution_datetime_local, forecast_execution_date, forecast_date, hour_ending
)

SELECT * FROM HOURLY
