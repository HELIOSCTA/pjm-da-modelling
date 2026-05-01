{{
  config(
    materialized='view'
  )
}}

---------------------------
-- DA Hourly Demand Bids by region
-- Grain: 1 row per (date, hour_ending, region) where region is
--   RTO / MIDATL / WEST (native) or SOUTH (derived = RTO - MIDATL - WEST).
-- Source: PJM Data Miner 2 hrl_dmd_bids feed.
---------------------------

SELECT * FROM {{ ref('staging_v1_pjm_load_da_hourly') }}
