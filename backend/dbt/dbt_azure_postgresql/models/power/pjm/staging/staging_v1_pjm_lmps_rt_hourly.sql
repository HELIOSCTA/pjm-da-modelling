{{
  config(
    materialized='ephemeral'
  )
}}

---------------------------
-- RT Hourly LMPs (normalized)
-- Combines verified + unverified, preferring verified
-- Grain: 1 row per date × hour × hub
---------------------------

WITH RT_VERIFIED AS (
    SELECT
        datetime_beginning_utc
        ,datetime_ending_utc
        ,timezone
        ,datetime_beginning_local
        ,datetime_ending_local
        ,date
        ,hour_ending
        ,hub
        ,'verified' AS source_table
        ,rt_lmp_total
        ,rt_lmp_system_energy_price
        ,rt_lmp_congestion_price
        ,rt_lmp_marginal_loss_price
    FROM {{ ref('source_v1_pjm_rt_verified_hrl_lmps') }}
),

RT_UNVERIFIED AS (
    SELECT
        datetime_beginning_utc
        ,datetime_ending_utc
        ,timezone
        ,datetime_beginning_local
        ,datetime_ending_local
        ,date
        ,hour_ending
        ,hub
        ,'unverified' AS source_table
        ,rt_lmp_total
        ,rt_lmp_system_energy_price
        ,rt_lmp_congestion_price
        ,rt_lmp_marginal_loss_price
    FROM {{ ref('source_v1_pjm_rt_unverified_hrl_lmps') }}
),

RT_COMBINED AS (
    SELECT * FROM RT_VERIFIED
    UNION ALL
    SELECT * FROM RT_UNVERIFIED
),

RT_RANKED AS (
    SELECT
        *
        ,ROW_NUMBER() OVER (
            PARTITION BY
                date,
                hour_ending,
                hub
            ORDER BY
                CASE WHEN source_table = 'verified' THEN 0 ELSE 1 END
        ) AS priority_rank
    FROM RT_COMBINED
),

FINAL AS (
    SELECT
        datetime_beginning_utc
        ,datetime_ending_utc
        ,timezone
        ,datetime_beginning_local
        ,datetime_ending_local
        ,date
        ,hour_ending
        ,hub
        ,source_table AS rt_source
        ,rt_lmp_total
        ,rt_lmp_system_energy_price
        ,rt_lmp_congestion_price
        ,rt_lmp_marginal_loss_price
    FROM RT_RANKED
    WHERE priority_rank = 1
)

SELECT * FROM FINAL
ORDER BY date DESC, hour_ending DESC, hub
