{{
  config(
    materialized='table',
  )
}}

SELECT * FROM {{ ref('staging_v1_pjm_wind_forecast_da_cutoff_historical') }}
