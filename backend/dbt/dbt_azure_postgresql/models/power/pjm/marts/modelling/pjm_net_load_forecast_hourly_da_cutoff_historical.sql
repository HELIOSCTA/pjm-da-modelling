{{
  config(
    materialized='table'
  )
}}

---------------------------
-- PJM Net Load Forecast — DA Cutoff, full captured history (bias-safe for backtests)
-- net_load = load - solar - wind (utility-scale; no BTM)
-- All three components share the same effective cutoff per delivery date D:
--   LEAST(D 10:00 AM EPT, today 10:00 AM EPT).
-- For D <= today, cutoff = D 10:00 AM EPT (per-day historical view).
-- For D > today, cutoff = today 10:00 AM EPT — matches the live mart exactly.
-- Load historical is filtered to as_of = LEAST(forecast_date, today) so its
-- cutoff lines up with solar/wind historical's per-(D, HE) cap.
-- Grain: 1 row per forecast_date × hour_ending (RTO only — solar/wind are RTO-wide).
-- INNER JOIN: missing solar or wind forecast drops the row rather than imputing zero.
---------------------------

WITH load AS (
    SELECT *
    FROM {{ ref('pjm_load_forecast_hourly_da_cutoff_historical') }}
    WHERE region = 'RTO'
      AND forecast_execution_date = LEAST(
          forecast_date,
          (CURRENT_TIMESTAMP AT TIME ZONE 'US/Eastern')::DATE
      )
),

solar AS (
    SELECT * FROM {{ ref('pjm_solar_forecast_hourly_da_cutoff_historical') }}
),

wind AS (
    SELECT * FROM {{ ref('pjm_wind_forecast_hourly_da_cutoff_historical') }}
)

SELECT
    load.forecast_datetime
    ,load.forecast_date
    ,load.hour_ending
    ,load.region
    ,load.forecast_load_mw
    ,solar.solar_forecast
    ,wind.wind_forecast
    ,(load.forecast_load_mw - solar.solar_forecast - wind.wind_forecast) AS net_load_forecast_mw
    ,load.forecast_execution_datetime_local  AS load_forecast_execution_datetime_local
    ,solar.forecast_execution_datetime_local AS solar_forecast_execution_datetime_local
    ,wind.forecast_execution_datetime_local  AS wind_forecast_execution_datetime_local
FROM load
INNER JOIN solar
    ON load.forecast_date = solar.forecast_date
    AND load.hour_ending = solar.hour_ending
INNER JOIN wind
    ON load.forecast_date = wind.forecast_date
    AND load.hour_ending = wind.hour_ending
