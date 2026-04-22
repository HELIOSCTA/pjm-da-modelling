{{
  config(
    materialized='view'
  )
}}

SELECT * FROM {{ ref('utils_v1_pjm_dates_daily') }}
