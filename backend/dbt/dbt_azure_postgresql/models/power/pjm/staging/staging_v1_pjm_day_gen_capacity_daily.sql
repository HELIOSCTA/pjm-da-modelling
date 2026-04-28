{{
  config(
    materialized='ephemeral'
  )
}}

---------------------------
-- PJM Day Generation Capacity Daily (aggregated)
-- Grain: 1 row per delivery date (system-wide)
--
-- Aggregations:
--   eco_max_daily_avg / min / max  — economic max offered (excludes outages)
--   emerg_max_daily_avg            — emergency + economic max
--   total_committed_mw             — RPM-cleared installed capacity (flat
--                                    intra-day; AVG used as a robust point
--                                    estimate even though variation is ~0)
---------------------------

WITH HOURLY AS (
    SELECT
         bid_datetime_beginning_ept::DATE AS date
        ,eco_max_mw
        ,emerg_max_mw
        ,total_committed_mw

    FROM {{ ref('source_v1_pjm_day_gen_capacity') }}
),

DAILY AS (
    SELECT
         date
        ,AVG(eco_max_mw)         AS eco_max_daily_avg_mw
        ,MIN(eco_max_mw)         AS eco_max_daily_min_mw
        ,MAX(eco_max_mw)         AS eco_max_daily_max_mw
        ,AVG(emerg_max_mw)       AS emerg_max_daily_avg_mw
        ,AVG(total_committed_mw) AS total_committed_mw

    FROM HOURLY
    GROUP BY date
)

SELECT * FROM DAILY
ORDER BY date DESC
