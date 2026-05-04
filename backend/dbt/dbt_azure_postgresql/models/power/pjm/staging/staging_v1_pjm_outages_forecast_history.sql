{{
  config(
    materialized='ephemeral'
  )
}}

---------------------------
-- PJM Outages Forecast — multi-vintage long-form history
-- Source: pjm.seven_day_outage_forecast. PJM publishes the 7-day outage
-- forecast once per morning; each publication carries 8 rows per region —
-- forecast_date ∈ [exec_date, exec_date + 7]. There are no intra-day
-- vintages.
-- Grain: 1 row per (forecast_date × as_of_date × region).
-- lead_days = forecast_date - as_of_date (0..7 expected):
--   lead_days = 0 → publication-day view (freshest snapshot for the day,
--                   sometimes called "actual" but is still PJM's morning
--                   snapshot, not settled outage MW).
--   lead_days = 1 → published the morning before target_date — what was
--                   knowable at DA decision time for target_date.
--   lead_days = k → published k mornings before target.
-- Carries both as_of_date and forecast_execution_date for consistency
-- with the load / solar / wind forecast historical marts. For outages
-- the two are always equal — there is only one publish per morning,
-- so as_of_date (the model-facing snapshot bucket) and
-- forecast_execution_date (the actual publish date) collapse onto the
-- same value. Drops the legacy 1-indexed forecast_day_number from
-- staging_v1_pjm_outages_forecast_daily — the 0-indexed lead_days
-- here is the canonical lead.
-- Goes back to source's 2020 floor; no rolling window cutoff.
---------------------------

WITH src AS (
    SELECT
        forecast_execution_date
        ,forecast_execution_date AS as_of_date
        ,forecast_date
        ,(forecast_date - forecast_execution_date)::INT AS lead_days
        ,region
        ,total_outages_mw
        ,planned_outages_mw
        ,maintenance_outages_mw
        ,forced_outages_mw

    FROM {{ ref('source_v1_pjm_seven_day_outage_forecast') }}
),

---------------------------
-- Defensive dedup. Source has natural key (exec, target, region); upstream
-- re-publishes have not been observed but the ROW_NUMBER guards against
-- silent duplication if it ever happens. Tie-break is arbitrary-but-
-- deterministic (highest total_outages_mw, NULLS LAST).
---------------------------

DEDUPED AS (
    SELECT
        as_of_date
        ,forecast_execution_date
        ,forecast_date
        ,lead_days
        ,region
        ,total_outages_mw
        ,planned_outages_mw
        ,maintenance_outages_mw
        ,forced_outages_mw

        ,ROW_NUMBER() OVER (
            PARTITION BY as_of_date, forecast_date, region
            ORDER BY total_outages_mw DESC NULLS LAST
        ) AS rn

    FROM src
)

SELECT
    as_of_date
    ,forecast_execution_date
    ,forecast_date
    ,lead_days
    ,region
    ,total_outages_mw
    ,planned_outages_mw
    ,maintenance_outages_mw
    ,forced_outages_mw

FROM DEDUPED
WHERE rn = 1
ORDER BY forecast_date DESC, as_of_date DESC, region
