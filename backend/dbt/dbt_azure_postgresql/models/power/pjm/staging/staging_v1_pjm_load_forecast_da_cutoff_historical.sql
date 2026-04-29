{{
  config(
    materialized='ephemeral'
  )
}}

---------------------------
-- PJM Historical Load Forecast — DA Cutoff (bias-safe vintage for backtests)
-- For each delivery date D, picks the latest revision issued at or before
-- D-1 10:00 AM EPT (i.e. before the PJM DA market close on the day before
-- delivery). Mirrors the live mart's "before 10:00 AM" rule. The historical
-- feed only retains ~5-6 vintages per delivery hour, so the chosen vintage
-- is often from D-2 or earlier when no D-1 pre-10:00 AM vintage exists.
-- Grain: 1 row per forecast_date × hour_ending × region, starting 2020-01-01.
---------------------------

WITH all_forecasts AS (
    SELECT
        forecast_execution_datetime_utc
        ,timezone
        ,forecast_execution_datetime_local
        ,forecast_execution_date
        ,forecast_date
        ,hour_ending
        ,region
        ,forecast_load_mw
    FROM {{ ref('source_v1_pjm_historical_load_forecasts') }}
    WHERE
        forecast_date >= '2020-01-01'
        AND forecast_execution_datetime_local
            <= (forecast_date::TIMESTAMP - INTERVAL '14 hours')
),

-- ────── Rank execution vintages per forecast_date (most recent first) ──────

forecast_rank AS (
    SELECT
        forecast_execution_datetime_local
        ,forecast_date

        ,DENSE_RANK() OVER (
            PARTITION BY forecast_date
            ORDER BY forecast_execution_datetime_local DESC
        ) AS forecast_rank

    FROM (
        SELECT DISTINCT forecast_execution_datetime_local, forecast_date
        FROM all_forecasts
    ) sub
),

-- ────── Latest pre-cutoff revision per forecast_date × hour_ending × region ──────

latest AS (
    SELECT
        f.forecast_execution_datetime_utc
        ,f.timezone
        ,f.forecast_execution_datetime_local
        ,r.forecast_rank
        ,f.forecast_execution_date

        ,(f.forecast_date + INTERVAL '1 hour' * (f.hour_ending - 1)) AS forecast_datetime
        ,f.forecast_date
        ,f.hour_ending

        ,f.region
        ,f.forecast_load_mw

        ,ROW_NUMBER() OVER (
            PARTITION BY f.forecast_date, f.hour_ending, f.region
            ORDER BY f.forecast_execution_datetime_local DESC
        ) AS rn

    FROM all_forecasts f
    JOIN forecast_rank r
        ON f.forecast_execution_datetime_local = r.forecast_execution_datetime_local
        AND f.forecast_date = r.forecast_date
)

SELECT
    forecast_execution_datetime_utc
    ,timezone
    ,forecast_execution_datetime_local
    ,forecast_rank
    ,forecast_execution_date
    ,forecast_datetime
    ,forecast_date
    ,hour_ending
    ,region
    ,forecast_load_mw
FROM latest
WHERE rn = 1
