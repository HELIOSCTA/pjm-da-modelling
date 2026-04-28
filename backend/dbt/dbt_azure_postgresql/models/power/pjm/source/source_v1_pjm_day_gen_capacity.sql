{{
  config(
    materialized='ephemeral'
  )
}}

---------------------------
-- PJM Day Generation Capacity (normalized)
-- Grain: 1 row per hour (system-wide)
--
-- Backward-only feed: today and forward-dated hours are not published.
-- eco_max excludes outages; total_committed is the RPM-cleared installed
-- capacity (structural, effectively flat day-to-day).
---------------------------

WITH RAW AS (
    SELECT
         bid_datetime_beginning_ept::TIMESTAMP AS bid_datetime_beginning_ept
        ,eco_max::NUMERIC                      AS eco_max_mw
        ,emerg_max::NUMERIC                    AS emerg_max_mw
        ,total_committed::NUMERIC              AS total_committed_mw

    FROM {{ source('pjm_v1', 'day_gen_capacity') }}
    WHERE
        EXTRACT(YEAR FROM bid_datetime_beginning_ept::DATE) >= 2014
)

SELECT * FROM RAW
ORDER BY bid_datetime_beginning_ept DESC
