{{
  config(
    materialized='table',
  )
}}

SELECT * FROM {{ ref('staging_v1_gridstatus_pjm_wind_forecast_da_cutoff') }}
