{{
  config(
    materialized='ephemeral'
  )
}}

---------------------------
-- PJM 7-Day Load Forecast — DA Cutoff, full captured history (bias-safe vintage for backtests)
-- For each forecast_execution_date, takes that day's last pre-10:00 AM EPT
-- issue and emits its full 7-day horizon. Mirrors the live DA-cutoff mart's
-- "before 10:00 AM" rule, applied across every issue date preserved in the
-- seven_day_load_forecast capture.
-- Grain: 1 row per forecast_execution_date × forecast_date × hour_ending × region.
---------------------------

WITH source_forecasts AS (
    SELECT
        forecast_execution_datetime_utc
        ,timezone
        ,forecast_execution_datetime_local
        ,forecast_execution_date
        ,forecast_date
        ,hour_ending
        ,region
        ,forecast_load_mw
    FROM {{ ref('source_v1_pjm_seven_day_load_forecast_historical') }}
),

-- ────── Rank ALL execution vintages per forecast_date (most recent first) ──────

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
        FROM source_forecasts
    ) sub
),

-- ────── Pre-10:00 AM EPT issues only ──────

pre_cutoff AS (
    SELECT *
    FROM source_forecasts
    WHERE forecast_execution_datetime_local::TIME <= '10:00:00'
),

-- ────── Latest pre-cutoff issue per execution date ──────

latest_issue_per_date AS (
    SELECT
        forecast_execution_date
        ,MAX(forecast_execution_datetime_local) AS latest_issue
    FROM pre_cutoff
    GROUP BY forecast_execution_date
),

-- ────── Full 7-day horizon for each chosen issue ──────

final AS (
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

    FROM pre_cutoff f
    JOIN latest_issue_per_date l
        ON f.forecast_execution_date = l.forecast_execution_date
        AND f.forecast_execution_datetime_local = l.latest_issue
    LEFT JOIN forecast_rank r
        ON f.forecast_execution_datetime_local = r.forecast_execution_datetime_local
        AND f.forecast_date = r.forecast_date
)

SELECT * FROM final
ORDER BY forecast_execution_date DESC, forecast_date, hour_ending, region
