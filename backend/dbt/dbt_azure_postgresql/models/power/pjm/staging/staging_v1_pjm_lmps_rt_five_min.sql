{{
  config(
    materialized='ephemeral'
  )
}}

---------------------------
-- PJM Unverified 5-Minute Real-Time LMPs (normalized)
-- Grain: 1 row per 5-min interval × hub
---------------------------

SELECT
    datetime_beginning_utc
    ,timezone
    ,datetime_beginning_local
    ,date
    ,hour_ending
    ,hub
    ,five_min_rt_lmp AS rt_lmp
FROM {{ ref('source_v1_pjm_unverified_five_min_lmps') }}
ORDER BY datetime_beginning_local DESC, hub
