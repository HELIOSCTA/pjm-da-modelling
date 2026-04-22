{{
  config(
    materialized='table',
  )
}}

SELECT * FROM {{ ref('staging_v1_gridstatus_pjm_solar_forecast_da_cutoff') }}
