{{
  config(
    materialized='ephemeral'
  )
}}

---------------------------
-- RT 15-second Wind Generation, RTO total (normalized)
-- Grain: 1 row per 15-second sample x region (RTO only)
-- The upstream feed is system-wide and has no `area` column, so we project
-- a constant 'RTO' region. The hourly mart's INSTANTANEOUS CTE averages
-- ~240 samples down to one hourly value per region. Only fills the RTO row
-- in the hourly mart; sub-region rows remain HOURLY-only.
---------------------------

SELECT
    datetime_beginning_utc
    ,'US/Eastern' AS timezone
    ,datetime_beginning_ept AS datetime_beginning_local
    ,datetime_beginning_ept::DATE AS date
    ,(EXTRACT(HOUR FROM datetime_beginning_ept) + 1)::INT AS hour_ending

    ,'RTO'::TEXT AS region

    ,wind_generation_mw::NUMERIC AS wind_gen_mw

FROM {{ source('pjm_v1', 'instantaneous_wind_gen_v1_2026_apr_28') }}
