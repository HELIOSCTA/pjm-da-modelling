{{
  config(
    materialized='table',
  )
}}

SELECT * FROM {{ ref('staging_v1_pjm_solar_forecast_da_cutoff_historical') }}
