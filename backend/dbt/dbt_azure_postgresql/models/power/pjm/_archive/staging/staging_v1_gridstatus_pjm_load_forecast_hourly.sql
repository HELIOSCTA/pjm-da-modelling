{{
  config(
    materialized='ephemeral'
  )
}}

---------------------------
-- PJM 7-Day Load Forecast (normalized)
-- All revisions, ranked by issue time (most recent first)
-- Grain: 1 row per forecast_execution_datetime × forecast_date × hour_ending × region
---------------------------

WITH FORECAST AS (
    SELECT
        forecast_execution_datetime_utc
        ,timezone
        ,forecast_execution_datetime_local
        ,forecast_execution_date
        ,forecast_date
        ,hour_ending
        ,region
        ,forecast_load_mw
    FROM {{ ref('source_v1_gridstatus_pjm_load_forecast') }}
),

--------------------------------
-- Rank forecasts per forecast_date by issue time (most recent first)
--------------------------------

FORECAST_RANK AS (
    SELECT
        forecast_execution_datetime_local
        ,forecast_date

        ,DENSE_RANK() OVER (
            PARTITION BY forecast_date
            ORDER BY forecast_execution_datetime_local DESC
        ) AS forecast_rank

    FROM (
        SELECT DISTINCT forecast_execution_datetime_local, forecast_date
        FROM FORECAST
    ) sub
),

--------------------------------
--------------------------------

FINAL AS (
    SELECT
        f.forecast_execution_datetime_utc
        ,f.timezone
        ,f.forecast_execution_datetime_local
        ,r.forecast_rank
        ,f.forecast_execution_date

        ,(f.forecast_date + INTERVAL '1 hour' * (f.hour_ending - 1)) AS forecast_datetime
        ,f.forecast_date
        ,f.hour_ending

        ,f.region
        ,f.forecast_load_mw

    FROM FORECAST f
    JOIN FORECAST_RANK r
        ON f.forecast_execution_datetime_local = r.forecast_execution_datetime_local
        AND f.forecast_date = r.forecast_date
)

SELECT * FROM FINAL
ORDER BY forecast_date DESC, forecast_execution_datetime_local DESC, hour_ending, region


