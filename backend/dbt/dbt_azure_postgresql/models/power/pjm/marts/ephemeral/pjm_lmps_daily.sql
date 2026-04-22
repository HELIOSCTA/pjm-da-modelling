{{
  config(
    materialized='ephemeral'
  )
}}

SELECT * FROM {{ ref('staging_v1_pjm_lmps_daily') }}
