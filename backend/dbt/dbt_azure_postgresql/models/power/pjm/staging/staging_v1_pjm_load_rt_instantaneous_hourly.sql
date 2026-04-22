{{
  config(
    materialized='ephemeral'
  )
}}

---------------------------
-- RT Instantaneous Hourly Load (5-min averaged to hourly)
-- Grain: 1 row per date Ã— hour Ã— region
---------------------------

SELECT
    DATE_TRUNC('hour', datetime_beginning_utc) AS datetime_beginning_utc
    ,DATE_TRUNC('hour', datetime_beginning_utc) + INTERVAL '1 hour' AS datetime_ending_utc
    ,timezone
    ,DATE_TRUNC('hour', datetime_beginning_local) AS datetime_beginning_local
    ,DATE_TRUNC('hour', datetime_beginning_local) + INTERVAL '1 hour' AS datetime_ending_local
    ,date
    ,hour_ending
    ,load_area AS region
    ,AVG(load_mw) AS rt_load_mw
FROM {{ ref('source_v1_pjm_five_min_load') }}
WHERE
    load_area IN ('RTO', 'WEST', 'MIDATL', 'SOUTH')
GROUP BY 1, 2, 3, 4, 5, date, hour_ending, load_area
