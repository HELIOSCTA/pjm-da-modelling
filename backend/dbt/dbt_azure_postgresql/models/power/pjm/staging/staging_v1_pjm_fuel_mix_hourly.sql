{{
  config(
    materialized='ephemeral'
  )
}}

---------------------------
-- PJM Fuel Mix Hourly
-- Grain: 1 row per date Ã— hour
---------------------------

WITH HOURLY AS (
    SELECT
        datetime_beginning_utc
        ,datetime_ending_utc
        ,timezone
        ,datetime_beginning_local
        ,datetime_ending_local
        ,date
        ,hour_ending

        ,coal
        ,gas
        ,hydro
        ,multiple_fuels
        ,nuclear
        ,oil
        ,solar
        ,storage
        ,wind
        ,other
        ,other_renewables

        ,total
        ,thermal
        ,renewables
        ,gas_pct_thermal
        ,coal_pct_thermal

    FROM {{ ref('source_v1_gridstatus_pjm_fuel_mix_hourly') }}
)

SELECT * FROM HOURLY
ORDER BY datetime_ending_local DESC
