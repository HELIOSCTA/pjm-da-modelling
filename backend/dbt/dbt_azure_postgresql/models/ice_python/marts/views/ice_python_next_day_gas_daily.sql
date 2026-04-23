{{
  config(
    materialized='view'
  )
}}

---------------------------
-- ICE NEXT-DAY GAS DAILY MART
---------------------------

WITH FINAL AS (
    SELECT * FROM {{ ref('staging_v1_ice_next_day_gas_daily') }}
)

SELECT * FROM FINAL
ORDER BY gas_day DESC
