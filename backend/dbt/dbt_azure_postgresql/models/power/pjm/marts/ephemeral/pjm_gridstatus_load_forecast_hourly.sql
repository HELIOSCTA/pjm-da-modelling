{{
  config(
    materialized='ephemeral'
  )
}}

SELECT * FROM {{ ref('staging_v1_gridstatus_pjm_load_forecast_hourly') }}
