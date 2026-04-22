{{
  config(
    materialized='ephemeral'
  )
}}

---------------------------
-- Unverified 5-Minute RT LMPs (normalized)
-- Grain: 1 row per 5-min interval x hub
-- Filtered to type = 'HUB' at ingestion
---------------------------

WITH RAW AS (
    SELECT
        datetime_beginning_utc
        ,'US/Eastern' AS timezone
        ,datetime_beginning_ept AS datetime_beginning_local
        ,datetime_beginning_ept::DATE AS date
        ,(EXTRACT(HOUR FROM datetime_beginning_ept) + 1)::INT AS hour_ending
        ,name AS hub
        ,type
        ,five_min_rtlmp::NUMERIC AS five_min_rt_lmp
        ,hourly_lmp::NUMERIC AS hourly_lmp
    FROM {{ source('pjm_v1', 'unverified_five_min_lmps') }}
)

SELECT * FROM RAW
ORDER BY datetime_beginning_local DESC, hub
