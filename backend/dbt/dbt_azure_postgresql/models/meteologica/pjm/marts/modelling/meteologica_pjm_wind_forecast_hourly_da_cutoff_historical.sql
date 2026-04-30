{{
  config(
    materialized='incremental',
    unique_key=['as_of_date', 'forecast_date', 'hour_ending', 'region'],
    incremental_strategy='delete+insert',
    on_schema_change='append_new_columns',
    indexes=[
      {'columns': ['as_of_date'], 'type': 'btree'},
      {'columns': ['forecast_date'], 'type': 'btree'},
      {'columns': ['as_of_date', 'forecast_date', 'hour_ending', 'region'], 'type': 'btree'}
    ]
  )
}}

---------------------------
-- Meteologica PJM Wind Forecast — DA Cutoff, full captured history (bias-safe vintage for backtests)
-- For each as_of_date D, takes the latest issue in the window
-- (D 10:00 EPT - 48h, D 10:00 EPT] per (forecast_date x hour_ending x region)
-- and emits its full horizon. as_of_date is the simulated "today" bucket;
-- forecast_execution_date is the chosen issue's actual date and may differ
-- from as_of_date for HE 1 because the 48h lookback can pull D-1's late
-- evening issue.
-- Meteologica wind publishes once daily at ~09:47 EPT. The 48h window
-- captures D's issue when it has landed and falls back to D-1's when it
-- has not, while still preferring D's latest issue when both are eligible.
-- Grain: 1 row per as_of_date x forecast_date x hour_ending x region.
--
-- Incremental: regular runs recompute as_of_dates in a 3-day rolling window
-- (max as_of_date already loaded, minus 2 days). Older buckets are immutable
-- once their 48h window closes. Use --full-refresh to rebuild from scratch.
---------------------------

{% set as_of_filter %}
    {% if is_incremental() %}
    AND forecast_execution_date >= (SELECT MAX(as_of_date) - INTERVAL '2 days' FROM {{ this }})::DATE
    {% endif %}
{% endset %}

WITH source_forecasts AS (
    SELECT
        forecast_execution_datetime_utc
        ,timezone
        ,forecast_execution_datetime_local
        ,forecast_execution_date
        ,forecast_date
        ,hour_ending
        ,region
        ,wind_forecast
    FROM {{ ref('staging_v1_meteologica_pjm_wind_forecast_hourly') }}
),

-- ────── Rank ALL execution vintages per (forecast_date, region) (most recent first) ──────

forecast_rank AS (
    SELECT
        forecast_execution_datetime_local
        ,forecast_date
        ,region

        ,DENSE_RANK() OVER (
            PARTITION BY forecast_date, region
            ORDER BY forecast_execution_datetime_local DESC
        ) AS forecast_rank

    FROM (
        SELECT DISTINCT forecast_execution_datetime_local, forecast_date, region
        FROM source_forecasts
    ) sub
),

-- ────── Candidate as_of_dates D to produce a vintage for ──────
-- Filtered to a 3-day rolling window on incremental runs.

as_of_dates AS (
    SELECT DISTINCT forecast_execution_date AS as_of_date
    FROM source_forecasts
    WHERE 1 = 1
    {{ as_of_filter }}
),

-- ────── Issues eligible for each as_of_date D ──────
-- D's 48h pre-10:00 EPT window pairs each issue with up to two consecutive D values.
-- forecast_date >= D matches the live mart, which only emits future-from-D hours.

eligible AS (
    SELECT
        d.as_of_date
        ,f.forecast_execution_datetime_utc
        ,f.timezone
        ,f.forecast_execution_datetime_local
        ,f.forecast_execution_date
        ,f.forecast_date
        ,f.hour_ending
        ,f.region
        ,f.wind_forecast
    FROM source_forecasts f
    JOIN as_of_dates d
        ON f.forecast_execution_datetime_local <= (d.as_of_date + TIME '10:00:00')
       AND f.forecast_execution_datetime_local >  (d.as_of_date + TIME '10:00:00' - INTERVAL '48 hours')
    WHERE f.forecast_date >= d.as_of_date
),

-- ────── Latest eligible issue per (as_of_date, forecast_date, hour_ending, region) ──────

ranked AS (
    SELECT
        e.*
        ,ROW_NUMBER() OVER (
            PARTITION BY e.as_of_date, e.forecast_date, e.hour_ending, e.region
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

        ,r.region
        ,r.wind_forecast

    FROM ranked r
    LEFT JOIN forecast_rank fr
        ON r.forecast_execution_datetime_local = fr.forecast_execution_datetime_local
       AND r.forecast_date = fr.forecast_date
       AND r.region = fr.region
    WHERE r.rn = 1
)

SELECT * FROM final
