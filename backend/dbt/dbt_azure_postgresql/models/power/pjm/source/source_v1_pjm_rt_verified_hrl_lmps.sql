{{
  config(
    materialized='ephemeral'
  )
}}

---------------------------
-- RT Verified LMPS (normalized)
-- Grain: 1 row per date × hour × hub
---------------------------

WITH RT_VERIFIED_LMPS AS (
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
        ,system_energy_price_rt AS rt_lmp_system_energy_price
        ,congestion_price_rt AS rt_lmp_congestion_price
        ,marginal_loss_price_rt AS rt_lmp_marginal_loss_price

    FROM {{source('pjm_v1', 'rt_settlements_verified_hourly_lmps')}}
    WHERE
        datetime_beginning_ept >= '2014-01-01 00:00:00'
)

SELECT * FROM RT_VERIFIED_LMPS
ORDER BY date DESC, hour_ending DESC, hub
