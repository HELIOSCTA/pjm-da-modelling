{{
  config(
    materialized='view'
  )
}}

---------------------------
-- ICE GAS-DAY DAILY MART
-- 10 PJM-relevant hubs. One row per gas_day with trade_date pointing at
-- the actual session that priced this delivery day.
---------------------------

WITH FINAL AS (
    SELECT
        timezone
        ,datetime_beginning_local
        ,datetime_ending_local
        ,gas_day
        ,trade_date
        ,tetco_m3_cash
        ,columbia_tco_cash
        ,transco_z6_ny_cash
        ,dominion_south_cash
        ,nng_ventura_cash
        ,tetco_m2_cash
        ,transco_z5_north_cash
        ,tenn_z4_marcellus_cash
        ,transco_leidy_cash
        ,chicago_cg_cash
    FROM {{ ref('staging_v1_ice_gas_day_daily') }}
)

SELECT * FROM FINAL
ORDER BY gas_day DESC
