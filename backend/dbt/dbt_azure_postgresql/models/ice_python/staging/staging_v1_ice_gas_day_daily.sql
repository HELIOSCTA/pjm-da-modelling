{{
  config(
    materialized='ephemeral'
  )
}}

---------------------------
-- DAILY GAS-DAY STAGING (10 PJM-relevant hubs)
-- Aggregates the hourly grain into one row per gas_day spanning the
-- full 9:00-9:00 Central interval. Prices are constant across the 24
-- hourly rows of a gas_day, so MAX collapses without ambiguity.
---------------------------

SELECT
    MIN(datetime_beginning_utc) AS datetime_beginning_utc,
    MAX(datetime_ending_utc) AS datetime_ending_utc,
    MAX(timezone) AS timezone,
    MIN(datetime_beginning_local) AS datetime_beginning_local,
    MAX(datetime_ending_local) AS datetime_ending_local,
    gas_day,
    trade_date,
    MAX(tetco_m3_cash) AS tetco_m3_cash,
    MAX(columbia_tco_cash) AS columbia_tco_cash,
    MAX(transco_z6_ny_cash) AS transco_z6_ny_cash,
    MAX(dominion_south_cash) AS dominion_south_cash,
    MAX(nng_ventura_cash) AS nng_ventura_cash,
    MAX(tetco_m2_cash) AS tetco_m2_cash,
    MAX(transco_z5_north_cash) AS transco_z5_north_cash,
    MAX(tenn_z4_marcellus_cash) AS tenn_z4_marcellus_cash,
    MAX(transco_leidy_cash) AS transco_leidy_cash,
    MAX(chicago_cg_cash) AS chicago_cg_cash
FROM {{ ref('staging_v1_ice_gas_day_hourly') }}
GROUP BY gas_day, trade_date
