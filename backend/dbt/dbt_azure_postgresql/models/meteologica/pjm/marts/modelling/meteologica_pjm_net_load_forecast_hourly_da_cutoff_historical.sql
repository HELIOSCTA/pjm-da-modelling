{{
  config(
    materialized='table'
  )
}}

---------------------------
-- Meteologica PJM Regional Net-Load Forecast — DA Cutoff, full captured history (bias-safe vintage for backtests)
-- net_load = load - solar - wind (utility-scale; no BTM)
-- Combines the three component historical marts at the same as_of_date snapshot.
-- as_of_date is the simulated "today" bucket; each series can carry its own
-- forecast_execution_date (the chosen issue's actual date), which may differ
-- across series for HE 1 because the 48h lookback can pull D-1's late issue.
-- Grain: 1 row per as_of_date x forecast_date x hour_ending x region,
--         region in {RTO, MIDATL, SOUTH, WEST}.
-- INNER JOIN on (as_of_date, forecast_date, hour_ending, region): a missing
-- series drops the row (do not impute zero).
---------------------------

WITH load AS (
    SELECT * FROM {{ ref('meteologica_pjm_load_forecast_hourly_da_cutoff_historical') }}
),

solar AS (
    SELECT * FROM {{ ref('meteologica_pjm_solar_forecast_hourly_da_cutoff_historical') }}
),

wind AS (
    SELECT * FROM {{ ref('meteologica_pjm_wind_forecast_hourly_da_cutoff_historical') }}
)

SELECT
    load.as_of_date
    ,load.forecast_datetime
    ,load.forecast_date
    ,load.hour_ending
    ,load.region
    ,'meteologica' AS source
    ,load.forecast_load_mw
    ,solar.solar_forecast
    ,wind.wind_forecast
    ,(load.forecast_load_mw - solar.solar_forecast - wind.wind_forecast) AS net_load_forecast_mw
    ,load.forecast_execution_datetime_local  AS load_forecast_execution_datetime_local
    ,solar.forecast_execution_datetime_local AS solar_forecast_execution_datetime_local
    ,wind.forecast_execution_datetime_local  AS wind_forecast_execution_datetime_local
FROM load
INNER JOIN solar
    ON load.as_of_date    = solar.as_of_date
    AND load.forecast_date = solar.forecast_date
    AND load.hour_ending   = solar.hour_ending
    AND load.region        = solar.region
INNER JOIN wind
    ON load.as_of_date    = wind.as_of_date
    AND load.forecast_date = wind.forecast_date
    AND load.hour_ending   = wind.hour_ending
    AND load.region        = wind.region
