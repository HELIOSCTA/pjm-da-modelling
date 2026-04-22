{{
  config(
    materialized='ephemeral'
  )
}}

---------------------------
-- RT Prelim Hourly Load
-- Grain: 1 row per date Ã— hour Ã— region
---------------------------

SELECT
    datetime_beginning_utc
    ,datetime_ending_utc
    ,timezone
    ,datetime_beginning_local
    ,datetime_ending_local
    ,date
    ,hour_ending
    ,load_area AS region
    ,load_mw AS rt_load_mw
FROM {{ ref('source_v1_pjm_hrl_load_prelim') }}
WHERE
    load_area IN ('RTO', 'WEST', 'MIDATL', 'SOUTH')
