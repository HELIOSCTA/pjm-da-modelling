{{
  config(
    materialized='table'
  )
}}

SELECT * FROM {{ ref('staging_v1_meteologica_pjm_wind_forecast_da_cutoff') }}
