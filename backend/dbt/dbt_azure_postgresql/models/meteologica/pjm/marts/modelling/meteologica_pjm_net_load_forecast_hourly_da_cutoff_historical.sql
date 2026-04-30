{{
  config(
    materialized='incremental',
    unique_key=['as_of_date', 'forecast_date', 'hour_ending', 'region'],
    incremental_strategy='delete+insert',
    on_schema_change='append_new_columns',
    indexes=[
      {'columns': ['as_of_date'], 'type': 'btree'},
      {'columns': ['forecast_date'], 'type': 'btree'},
      {'columns': ['as_of_date', 'forecast_date', 'hour_ending', 'region'], 'type': 'btree'}
    ]
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
--
-- Incremental: regular runs recompute as_of_dates in a 3-day rolling window
-- (max as_of_date already loaded, minus 2 days). Use --full-refresh to
-- rebuild from scratch.
---------------------------

{% set as_of_filter %}
    {% if is_incremental() %}
    AND as_of_date >= (SELECT MAX(as_of_date) - INTERVAL '2 days' FROM {{ this }})::DATE
    {% endif %}
{% endset %}

WITH load AS (
    SELECT * FROM {{ ref('meteologica_pjm_load_forecast_hourly_da_cutoff_historical') }}
    WHERE 1 = 1
    {{ as_of_filter }}
),

solar AS (
    SELECT * FROM {{ ref('meteologica_pjm_solar_forecast_hourly_da_cutoff_historical') }}
    WHERE 1 = 1
    {{ as_of_filter }}
),

wind AS (
    SELECT * FROM {{ ref('meteologica_pjm_wind_forecast_hourly_da_cutoff_historical') }}
    WHERE 1 = 1
    {{ as_of_filter }}
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
