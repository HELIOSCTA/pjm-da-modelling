{{
  config(
    materialized='ephemeral'
  )
}}

---------------------------
-- PJM 7-Day Outage Forecast (normalized)
-- Ranked by issue time (earliest first)
-- Grain: 1 row per forecast_execution_date × forecast_date × region
---------------------------

WITH FORECAST AS (
    SELECT
        forecast_execution_date
        ,forecast_date
        ,region
        ,total_outages_mw
        ,planned_outages_mw
        ,maintenance_outages_mw
        ,forced_outages_mw

    FROM {{ ref('source_v1_pjm_seven_day_outage_forecast') }}
    WHERE
        forecast_execution_date >= (CURRENT_TIMESTAMP AT TIME ZONE 'MST')::DATE - 7
),

---------------------------
-- RANK FORECASTS BY ISSUE TIME (EARLIEST FIRST)
---------------------------

FORECAST_RANK AS (
    SELECT
        forecast_execution_date
        ,forecast_date

        ,DENSE_RANK() OVER (
            PARTITION BY forecast_date
            ORDER BY forecast_execution_date ASC
        ) AS forecast_rank

    FROM (
        SELECT DISTINCT forecast_execution_date, forecast_date
        FROM FORECAST
    ) sub
),

---------------------------
-- FINAL
---------------------------

FINAL AS (
    SELECT
        r.forecast_rank

        ,f.forecast_execution_date
        ,f.forecast_date
        ,(f.forecast_date - f.forecast_execution_date) + 1 AS forecast_day_number

        ,f.region

        ,f.total_outages_mw
        ,f.planned_outages_mw
        ,f.maintenance_outages_mw
        ,f.forced_outages_mw

    FROM FORECAST f
    JOIN FORECAST_RANK r
        ON f.forecast_execution_date = r.forecast_execution_date
        AND f.forecast_date = r.forecast_date
)

SELECT * FROM FINAL
ORDER BY forecast_date DESC, forecast_execution_date DESC, region



