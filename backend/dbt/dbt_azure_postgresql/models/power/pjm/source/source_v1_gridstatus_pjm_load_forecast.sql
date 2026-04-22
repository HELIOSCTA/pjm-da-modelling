{{
  config(
    materialized='ephemeral'
  )
}}

---------------------------
-- PJM 7-Day Load Forecast (normalized)
-- Grain: 1 row per forecast_execution_datetime × forecast_date × hour × region
---------------------------

WITH RAW AS (
    SELECT
        publish_time_utc AS forecast_execution_datetime_utc
        ,'US/Eastern' AS timezone
        ,publish_time_local AS forecast_execution_datetime_local
        ,publish_time_local::DATE AS forecast_execution_date

        ,interval_start_local::DATE AS forecast_date
        ,EXTRACT(HOUR FROM interval_start_local)::INT + 1 AS hour_ending

        ,rto_combined
        ,mid_atlantic_region
        ,western_region
        ,southern_region

    FROM {{ source('gridstatus_v1', 'pjm_load_forecast') }}
    WHERE
        publish_time_local::DATE >= ((CURRENT_TIMESTAMP AT TIME ZONE 'MST')::DATE - 7)
),

--------------------------------
-- Unpivot to long format
--------------------------------

UNPIVOTED AS (
    SELECT forecast_execution_datetime_utc, timezone, forecast_execution_datetime_local, forecast_execution_date, forecast_date, hour_ending, 'RTO' AS region, rto_combined AS forecast_load_mw FROM RAW
    UNION ALL
    SELECT forecast_execution_datetime_utc, timezone, forecast_execution_datetime_local, forecast_execution_date, forecast_date, hour_ending, 'MIDATL' AS region, mid_atlantic_region AS forecast_load_mw FROM RAW
    UNION ALL
    SELECT forecast_execution_datetime_utc, timezone, forecast_execution_datetime_local, forecast_execution_date, forecast_date, hour_ending, 'WEST' AS region, western_region AS forecast_load_mw FROM RAW
    UNION ALL
    SELECT forecast_execution_datetime_utc, timezone, forecast_execution_datetime_local, forecast_execution_date, forecast_date, hour_ending, 'SOUTH' AS region, southern_region AS forecast_load_mw FROM RAW
)

SELECT * FROM UNPIVOTED
ORDER BY forecast_execution_datetime_local DESC, forecast_date, hour_ending, region
