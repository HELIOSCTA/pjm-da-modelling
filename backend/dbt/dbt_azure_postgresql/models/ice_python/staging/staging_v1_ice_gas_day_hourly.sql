{{
  config(
    materialized='ephemeral'
  )
}}

---------------------------
-- HOURLY GAS-DAY STAGING (10 PJM-relevant hubs)
-- Source pivots ICE prices by trade_date and forward-fills gaps at
-- trade-date grain before joining the spine. This staging is a
-- pass-through that pins the column contract for the published mart.
---------------------------

SELECT
    datetime_beginning_utc,
    datetime_ending_utc,
    timezone,
    datetime_beginning_local,
    datetime_ending_local,
    gas_day,
    hour_ending,
    trade_date,
    tetco_m3_cash,
    columbia_tco_cash,
    transco_z6_ny_cash,
    dominion_south_cash,
    nng_ventura_cash,
    tetco_m2_cash,
    transco_z5_north_cash,
    tenn_z4_marcellus_cash,
    transco_leidy_cash,
    chicago_cg_cash
FROM {{ ref('source_v1_ice_gas_day_hourly') }}
