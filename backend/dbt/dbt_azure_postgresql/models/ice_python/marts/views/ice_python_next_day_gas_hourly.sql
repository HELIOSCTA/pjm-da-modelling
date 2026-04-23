{{
  config(
    materialized='view'
  )
}}

---------------------------
-- ICE NEXT-DAY GAS HOURLY MART
---------------------------

WITH FINAL AS (
    SELECT * FROM {{ ref('staging_v1_ice_next_day_gas_hourly') }}
)

SELECT * FROM FINAL
ORDER BY datetime DESC
