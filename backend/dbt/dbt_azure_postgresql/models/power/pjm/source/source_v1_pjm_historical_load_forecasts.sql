{{
  config(
    materialized='ephemeral'
  )
}}

---------------------------
-- PJM Historical Load Forecast (load_frcstd_hist, from PJM API)
-- All preserved vintages back to 2020-01-01 delivery date.
-- Region standardization to match the live seven-day feed (RTO/MIDATL/WEST/SOUTH):
--   SOUTH = DOM
--   WEST  = RTO - (MIDATL + DOM)
-- Grain: 1 row per forecast_execution_datetime × forecast_date × hour_ending × region
---------------------------

WITH RAW AS (
    SELECT
        evaluated_at_utc AS forecast_execution_datetime_utc
        ,evaluated_at_ept AS forecast_execution_datetime_local
        ,evaluated_at_ept::DATE AS forecast_execution_date
        ,forecast_hour_beginning_ept::DATE AS forecast_date
        ,EXTRACT(HOUR FROM forecast_hour_beginning_ept)::INT + 1 AS hour_ending
        ,forecast_area
        ,forecast_load_mw
    FROM {{ source('pjm_v1', 'historical_load_forecasts') }}
    WHERE
        forecast_hour_beginning_ept >= '2020-01-01'
        AND forecast_area IN ('RTO', 'MIDATL', 'DOM')
),

PIVOTED AS (
    SELECT
        forecast_execution_datetime_utc
        ,forecast_execution_datetime_local
        ,forecast_execution_date
        ,forecast_date
        ,hour_ending
        ,MAX(CASE WHEN forecast_area = 'RTO'    THEN forecast_load_mw END) AS rto_mw
        ,MAX(CASE WHEN forecast_area = 'MIDATL' THEN forecast_load_mw END) AS midatl_mw
        ,MAX(CASE WHEN forecast_area = 'DOM'    THEN forecast_load_mw END) AS dom_mw
    FROM RAW
    GROUP BY 1, 2, 3, 4, 5
),

UNPIVOTED AS (
    SELECT forecast_execution_datetime_utc, 'US/Eastern' AS timezone, forecast_execution_datetime_local, forecast_execution_date, forecast_date, hour_ending, 'RTO'    AS region, rto_mw                         AS forecast_load_mw FROM PIVOTED
    UNION ALL
    SELECT forecast_execution_datetime_utc, 'US/Eastern' AS timezone, forecast_execution_datetime_local, forecast_execution_date, forecast_date, hour_ending, 'MIDATL' AS region, midatl_mw                      AS forecast_load_mw FROM PIVOTED
    UNION ALL
    SELECT forecast_execution_datetime_utc, 'US/Eastern' AS timezone, forecast_execution_datetime_local, forecast_execution_date, forecast_date, hour_ending, 'WEST'   AS region, rto_mw - midatl_mw - dom_mw    AS forecast_load_mw FROM PIVOTED
    UNION ALL
    SELECT forecast_execution_datetime_utc, 'US/Eastern' AS timezone, forecast_execution_datetime_local, forecast_execution_date, forecast_date, hour_ending, 'SOUTH'  AS region, dom_mw                         AS forecast_load_mw FROM PIVOTED
)

SELECT * FROM UNPIVOTED
