{{
  config(
    materialized='ephemeral'
  )
}}

---------------------------
-- PJM 2-Day Solar Forecast — DA Cutoff, full captured history (bias-safe vintage for backtests)
-- For each as_of_date D, takes the latest publish in the window
-- (D 10:00 EPT - 48h, D 10:00 EPT] per (forecast_date x hour_ending)
-- and emits its full delivery horizon. Mirrors the live solar DA-cutoff mart's
-- per-(D, HE) latest-pre-10:00 EPT rule, applied across every issue date
-- preserved in the gridstatus capture.
-- as_of_date is the simulated "today morning" snapshot bucket;
-- forecast_execution_date is the chosen publish's actual date (matches live
-- mart semantics — they DIFFER for early hours of D when the chosen publish
-- is one issued the prior evening).
-- Grain: 1 row per as_of_date x forecast_date x hour_ending.
---------------------------

WITH source_forecasts AS (
    SELECT
        forecast_execution_datetime_utc
        ,timezone
        ,forecast_execution_datetime_local
        ,forecast_execution_date
        ,forecast_date
        ,hour_ending
        ,solar_forecast
        ,solar_forecast_btm
    FROM {{ ref('source_v1_pjm_solar_forecast_hourly_historical') }}
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

-- ────── Candidate as_of_dates D to produce a vintage for ──────

as_of_dates AS (
    SELECT DISTINCT forecast_execution_date AS as_of_date
    FROM source_forecasts
),

-- ────── Publishes eligible for each as_of_date D ──────
-- D's 48h pre-10:00 EPT window pairs each publish with up to two consecutive
-- as_of_dates. forecast_date >= D matches the live mart, which only emits
-- delivery hours from D forward.

eligible AS (
    SELECT
        d.as_of_date
        ,f.forecast_execution_datetime_utc
        ,f.timezone
        ,f.forecast_execution_datetime_local
        ,f.forecast_execution_date
        ,f.forecast_date
        ,f.hour_ending
        ,f.solar_forecast
        ,f.solar_forecast_btm
    FROM source_forecasts f
    JOIN as_of_dates d
        ON f.forecast_execution_datetime_local <= (d.as_of_date + TIME '10:00:00')
       AND f.forecast_execution_datetime_local >  (d.as_of_date + TIME '10:00:00' - INTERVAL '48 hours')
    WHERE f.forecast_date >= d.as_of_date
),

-- ────── Latest eligible publish per (as_of_date, forecast_date, hour_ending) ──────

ranked AS (
    SELECT
        e.*
        ,ROW_NUMBER() OVER (
            PARTITION BY e.as_of_date, e.forecast_date, e.hour_ending
            ORDER BY e.forecast_execution_datetime_local DESC
        ) AS rn
    FROM eligible e
),

final AS (
    SELECT
        r.as_of_date
        ,r.forecast_execution_datetime_utc
        ,r.timezone
        ,r.forecast_execution_datetime_local
        ,fr.forecast_rank
        ,r.forecast_execution_date

        ,(r.forecast_date + INTERVAL '1 hour' * (r.hour_ending - 1)) AS forecast_datetime
        ,r.forecast_date
        ,r.hour_ending

        ,r.solar_forecast
        ,r.solar_forecast_btm

    FROM ranked r
    LEFT JOIN forecast_rank fr
        ON r.forecast_execution_datetime_local = fr.forecast_execution_datetime_local
       AND r.forecast_date = fr.forecast_date
    WHERE r.rn = 1
)

SELECT * FROM final
ORDER BY as_of_date DESC, forecast_date, hour_ending
