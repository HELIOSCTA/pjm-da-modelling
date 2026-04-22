{{
  config(
    materialized='ephemeral'
  )
}}

---------------------------
-- RT Unverified LMPS (normalized)
-- Grain: 1 row per date × hour × hub
-- https://dataminer2.pjm.com/feed/rt_unverified_hrl_lmps/definition
---------------------------

WITH RT_UNVERIFIED_LMPS AS (
    SELECT
        datetime_beginning_utc
        ,datetime_beginning_utc + INTERVAL '1 hour' AS datetime_ending_utc
        ,'US/Eastern' AS timezone
        ,datetime_beginning_ept AS datetime_beginning_local
        ,datetime_beginning_ept + INTERVAL '1 hour' AS datetime_ending_local
        ,DATE(datetime_beginning_ept) AS date
        ,(EXTRACT(HOUR FROM datetime_beginning_ept) + 1)::INT AS hour_ending

        ,pnode_name AS hub

        ,total_lmp_rt AS rt_lmp_total
        ,(total_lmp_rt - congestion_price_rt - marginal_loss_price_rt) AS rt_lmp_system_energy_price
        ,congestion_price_rt AS rt_lmp_congestion_price
        ,marginal_loss_price_rt AS rt_lmp_marginal_loss_price

    FROM {{source('pjm_v1', 'rt_unverified_hourly_lmps')}}
    WHERE
        DATE(datetime_beginning_ept) >= ((CURRENT_TIMESTAMP AT TIME ZONE 'MST')::DATE - 7)
)

SELECT * FROM RT_UNVERIFIED_LMPS
ORDER BY date DESC, hour_ending DESC, hub
