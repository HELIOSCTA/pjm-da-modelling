{{
  config(
    materialized='view'
  )
}}

SELECT * FROM {{ ref('staging_v1_pjm_day_gen_capacity_daily') }}
