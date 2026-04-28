{{
  config(
    materialized='ephemeral'
  )
}}

---------------------------
-- RT 5-min Solar Generation, RTO total (normalized)
-- Grain: 1 row per 5-min interval x region (RTO only)
-- The upstream feed is system-wide and has no `area` column, so we project
-- a constant 'RTO' region. Only fills the RTO row in the hourly mart;
-- sub-region rows remain HOURLY-only.
---------------------------

SELECT
    datetime_beginning_utc
    ,'US/Eastern' AS timezone
    ,datetime_beginning_ept AS datetime_beginning_local
    ,datetime_beginning_ept::DATE AS date
    ,(EXTRACT(HOUR FROM datetime_beginning_ept) + 1)::INT AS hour_ending

    ,'RTO'::TEXT AS region

    ,solar_generation_mw::NUMERIC AS solar_gen_mw

FROM {{ source('pjm_v1', 'five_min_solar_generation_v1_2026_apr_28') }}
