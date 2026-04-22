{{
  config(
    materialized='incremental',
    unique_key=['datetime_beginning_utc', 'hub', 'market'],
    incremental_strategy='delete+insert',
    on_schema_change='append_new_columns',
    indexes=[
      {'columns': ['datetime_beginning_utc', 'hub', 'market'], 'type': 'btree'},
      {'columns': ['datetime_beginning_utc'], 'type': 'btree'}
    ]
  )
}}

---------------------------
-- Hourly LMPs (normalized)
-- Grain: 1 row per date × hour × hub × market
-- Mirrors the logic in staging_v1_pjm_lmps_hourly; inlined here so the
-- is_incremental() lookback filter pushes into the DA/RT source scans.
---------------------------

{% set lookback_filter %}
    {% if is_incremental() %}
    WHERE datetime_beginning_utc >= (SELECT MAX(datetime_beginning_utc) - INTERVAL '10 days' FROM {{ this }})
    {% endif %}
{% endset %}

WITH DA AS (
    SELECT
        datetime_beginning_utc
        ,datetime_ending_utc
        ,timezone
        ,datetime_beginning_local
        ,datetime_ending_local
        ,date
        ,hour_ending
        ,hub
        ,da_lmp_total
        ,da_lmp_system_energy_price
        ,da_lmp_congestion_price
        ,da_lmp_marginal_loss_price
    FROM {{ ref('source_v1_pjm_da_hrl_lmps') }}
    {{ lookback_filter }}
),

-- Inlined from staging_v1_pjm_lmps_rt_hourly so the lookback filter prunes
-- BEFORE the priority_rank window function (verified over unverified).
RT_VERIFIED AS (
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
    {{ lookback_filter }}
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
    {{ lookback_filter }}
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
            PARTITION BY date, hour_ending, hub
            ORDER BY CASE WHEN source_table = 'verified' THEN 0 ELSE 1 END
        ) AS priority_rank
    FROM RT_COMBINED
),

RT AS (
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
),

--------------------------------
-- Pivot to DA / RT / DART rows
--------------------------------

DA_ROWS AS (
    SELECT
        datetime_beginning_utc
        ,datetime_ending_utc
        ,timezone
        ,datetime_beginning_local
        ,datetime_ending_local
        ,date
        ,hour_ending
        ,hub
        ,'da' AS market
        ,NULL::TEXT AS rt_source
        ,da_lmp_total AS lmp_total
        ,da_lmp_system_energy_price AS lmp_system_energy_price
        ,da_lmp_congestion_price AS lmp_congestion_price
        ,da_lmp_marginal_loss_price AS lmp_marginal_loss_price
    FROM DA
),

RT_ROWS AS (
    SELECT
        datetime_beginning_utc
        ,datetime_ending_utc
        ,timezone
        ,datetime_beginning_local
        ,datetime_ending_local
        ,date
        ,hour_ending
        ,hub
        ,'rt' AS market
        ,rt_source
        ,rt_lmp_total AS lmp_total
        ,rt_lmp_system_energy_price AS lmp_system_energy_price
        ,rt_lmp_congestion_price AS lmp_congestion_price
        ,rt_lmp_marginal_loss_price AS lmp_marginal_loss_price
    FROM RT
),

DART_ROWS AS (
    SELECT
        da.datetime_beginning_utc
        ,da.datetime_ending_utc
        ,da.timezone
        ,da.datetime_beginning_local
        ,da.datetime_ending_local
        ,da.date
        ,da.hour_ending
        ,da.hub
        ,'dart' AS market
        ,rt.rt_source
        ,(da.da_lmp_total - rt.rt_lmp_total) AS lmp_total
        ,(da.da_lmp_system_energy_price - rt.rt_lmp_system_energy_price) AS lmp_system_energy_price
        ,(da.da_lmp_congestion_price - rt.rt_lmp_congestion_price) AS lmp_congestion_price
        ,(da.da_lmp_marginal_loss_price - rt.rt_lmp_marginal_loss_price) AS lmp_marginal_loss_price
    FROM DA
    INNER JOIN RT ON da.date = rt.date AND da.hour_ending = rt.hour_ending AND da.hub = rt.hub
),

LMPS AS (
    SELECT * FROM DA_ROWS
    UNION ALL
    SELECT * FROM RT_ROWS
    UNION ALL
    SELECT * FROM DART_ROWS
)

SELECT * FROM LMPS
