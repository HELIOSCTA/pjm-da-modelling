{{
  config(
    materialized='view'
  )
}}

---------------------------
-- PJM Cleared Operating Reserves (system-wide, hourly)
-- Grain: 1 row per (date, hour_ending) at locale='PJM_RTO'.
-- Source: PJM Data Miner 2 reserve_market_results feed.
-- Backward-only -- consumers compute a rolling (DOW, HE) profile from
-- these rows when they need a forward-date proxy.
---------------------------

SELECT * FROM {{ ref('staging_v1_pjm_reserve_market_results_hourly') }}
ORDER BY datetime_ending_local DESC
