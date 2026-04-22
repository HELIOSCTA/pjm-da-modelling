{{
  config(
    materialized='ephemeral'
  )
}}

---------------------------
-- PJM 5-Min Tie Flows (normalized)
-- Grain: 1 row per 5-min timestamp × tie_flow_name
---------------------------

WITH FIVE_MIN AS (
    SELECT
        datetime_beginning_utc
        ,'US/Eastern' AS timezone
        ,datetime_beginning_ept AS datetime_beginning_local
        ,datetime_beginning_ept::DATE AS date
        ,(EXTRACT(HOUR FROM datetime_beginning_ept) + 1)::INT AS hour_ending

        ,tie_flow_name
        ,actual_mw::NUMERIC AS actual_mw
        ,scheduled_mw::NUMERIC AS scheduled_mw

    FROM {{ source('pjm_v1', 'five_min_tie_flows') }}
    WHERE
        EXTRACT(YEAR FROM datetime_beginning_ept) >= 2020
)

SELECT * FROM FIVE_MIN
ORDER BY datetime_beginning_local DESC, tie_flow_name
