{{
  config(
    materialized='ephemeral'
  )
}}

---------------------------
-- PJM 7-Day Outage Forecast — live rolling 7-day window
-- Thin filter on staging_v1_pjm_outages_forecast_history (the canonical
-- multi-vintage source of truth) restricted to the most recent 7 days
-- of publishes.
-- Grain: 1 row per forecast_execution_date × forecast_date × region.
-- Output preserves the legacy column shape (forecast_rank,
-- forecast_day_number) so existing callers — _build_outages_query in
-- domains.py, the Streamlit Fundies_Outages page, and the HTML report
-- outages fragment — keep working unchanged.
-- forecast_day_number is the legacy 1-indexed lead (1 = same-day,
-- 2 = day-before, ..., 8 = week-ahead). Prefer lead_days from the
-- history view in new code; keep this column on the daily view for
-- backward compatibility only.
---------------------------

WITH WINDOW_ROWS AS (
    SELECT
        forecast_execution_date
        ,forecast_date
        ,lead_days
        ,region
        ,total_outages_mw
        ,planned_outages_mw
        ,maintenance_outages_mw
        ,forced_outages_mw

    FROM {{ ref('staging_v1_pjm_outages_forecast_history') }}
    WHERE
        as_of_date >= (CURRENT_TIMESTAMP AT TIME ZONE 'MST')::DATE - 7
),

---------------------------
-- RANK FORECASTS BY ISSUE TIME (EARLIEST FIRST)
---------------------------

FORECAST_RANK AS (
    SELECT
        forecast_execution_date
        ,forecast_date

        ,DENSE_RANK() OVER (
            PARTITION BY forecast_date
            ORDER BY forecast_execution_date ASC
        ) AS forecast_rank

    FROM (
        SELECT DISTINCT forecast_execution_date, forecast_date
        FROM WINDOW_ROWS
    ) sub
),

---------------------------
-- FINAL
---------------------------

FINAL AS (
    SELECT
        r.forecast_rank

        ,f.forecast_execution_date
        ,f.forecast_date
        ,f.lead_days + 1 AS forecast_day_number  -- legacy 1-indexed; prefer lead_days

        ,f.region

        ,f.total_outages_mw
        ,f.planned_outages_mw
        ,f.maintenance_outages_mw
        ,f.forced_outages_mw

    FROM WINDOW_ROWS f
    JOIN FORECAST_RANK r
        ON f.forecast_execution_date = r.forecast_execution_date
        AND f.forecast_date = r.forecast_date
)

SELECT * FROM FINAL
ORDER BY forecast_date DESC, forecast_execution_date DESC, region
