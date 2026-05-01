{{
  config(
    materialized='ephemeral'
  )
}}

---------------------------
-- PJM Real-Time Binding-Constraint Shadow Prices (normalized)
-- Grain: 1 row per (datetime_beginning_utc, monitored_facility, contingency_facility)
-- Source: PJM Data Miner 2 rt_marginal_value feed
---------------------------

WITH RAW AS (
    SELECT
        datetime_beginning_utc::TIMESTAMP                       AS datetime_beginning_utc
        ,datetime_ending_utc::TIMESTAMP                         AS datetime_ending_utc
        ,'US/Eastern'                                           AS timezone
        ,datetime_beginning_ept::TIMESTAMP                      AS datetime_beginning_local
        ,datetime_ending_ept::TIMESTAMP                         AS datetime_ending_local
        ,DATE(datetime_beginning_ept)                           AS date
        ,(EXTRACT(HOUR FROM datetime_beginning_ept) + 1)::INT   AS hour_ending
        ,monitored_facility
        ,contingency_facility
        ,transmission_constraint_penalty_factor::NUMERIC        AS transmission_constraint_penalty_factor
        ,limit_control_percentage::NUMERIC                      AS limit_control_percentage
        ,shadow_price::NUMERIC                                  AS shadow_price

    FROM {{ source('pjm_v1', 'rt_marginal_value') }}
)

SELECT * FROM RAW
ORDER BY date DESC, hour_ending DESC, monitored_facility, contingency_facility
