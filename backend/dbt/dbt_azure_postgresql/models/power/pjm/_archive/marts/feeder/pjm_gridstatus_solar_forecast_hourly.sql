{{
  config(
    materialized='view'
  )
}}

SELECT * FROM {{ ref('staging_v1_gridstatus_pjm_solar_forecast_hourly') }}
