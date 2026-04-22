{{
  config(
    materialized='ephemeral'
  )
}}

SELECT * FROM {{ ref('staging_v1_pjm_load_rt_instantaneous_hourly') }}
