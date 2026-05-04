{{
  config(
    materialized='view'
  )
}}

SELECT * FROM {{ ref('staging_v1_pjm_outages_forecast_history') }}
