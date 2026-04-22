{{
  config(
    materialized='ephemeral'
  )
}}

---------------------------
-- PJM Tie Flows Hourly (normalized)
-- 5-min data aggregated to hourly
-- Grain: 1 row per date Ã— hour Ã— tie_flow_name
---------------------------

WITH HOURLY AS (
    SELECT
        DATE_TRUNC('hour', datetime_beginning_utc) AS datetime_beginning_utc
        ,DATE_TRUNC('hour', datetime_beginning_utc) + INTERVAL '1 hour' AS datetime_ending_utc
        ,timezone
        ,DATE_TRUNC('hour', datetime_beginning_local) AS datetime_beginning_local
        ,DATE_TRUNC('hour', datetime_beginning_local) + INTERVAL '1 hour' AS datetime_ending_local
        ,date
        ,hour_ending

        ,tie_flow_name
        ,AVG(actual_mw) AS actual_mw
        ,AVG(scheduled_mw) AS scheduled_mw

    FROM {{ ref('source_v1_pjm_five_min_tie_flows') }}
    GROUP BY 1, 2, 3, 4, 5, 6, 7, tie_flow_name
)

SELECT * FROM HOURLY
ORDER BY datetime_ending_local DESC, tie_flow_name
