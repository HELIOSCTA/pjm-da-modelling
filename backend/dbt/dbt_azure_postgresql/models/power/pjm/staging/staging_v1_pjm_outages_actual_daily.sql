{{
  config(
    materialized='ephemeral'
  )
}}

---------------------------
-- PJM Outages "Actual" Daily — same-day publication slice
-- Thin filter on staging_v1_pjm_outages_forecast_history restricted to
-- lead_days = 0, i.e. the row PJM publishes on the operating day itself.
-- This is the freshest snapshot for the day, sometimes called the
-- "actual" — but it is still PJM's morning view, not settled outage MW.
-- Settlement-grade actuals are not provided by this feed.
-- Grain: 1 row per date × region.
---------------------------

SELECT
    forecast_date AS date
    ,region
    ,total_outages_mw
    ,planned_outages_mw
    ,maintenance_outages_mw
    ,forced_outages_mw

FROM {{ ref('staging_v1_pjm_outages_forecast_history') }}
WHERE lead_days = 0
ORDER BY date DESC, region
