{{
  config(
    materialized='ephemeral'
  )
}}

---------------------------
-- PJM 2-Day Solar Forecast — DA Cutoff, full captured history
-- For each (forecast_date, hour_ending), picks the latest publish at or
-- before LEAST(forecast_date 10:00 AM EPT, today 10:00 AM EPT). The cap on
-- "today 10:00 AM EPT" makes future-delivery rows match the live mart
-- exactly (both use today's cutoff, since you can't see beyond now).
-- Historical past-delivery rows use D 10:00 AM EPT as their cutoff —
-- "what was knowable on D's morning of delivery".
-- Output schema mirrors the live mart exactly so a join on
-- (forecast_execution_date, forecast_date, hour_ending) lines up.
-- Grain: 1 row per forecast_date × hour_ending.
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

-- ────── Latest publish at or before D 10:00 AM EPT per (forecast_date, hour_ending) ──────

ranked AS (
    SELECT
        s.*
        ,ROW_NUMBER() OVER (
            PARTITION BY s.forecast_date, s.hour_ending
            ORDER BY s.forecast_execution_datetime_local DESC
        ) AS rn
    FROM source_forecasts s
    WHERE s.forecast_execution_datetime_local
        <= LEAST(
            s.forecast_date::TIMESTAMP + TIME '10:00:00',
            (CURRENT_TIMESTAMP AT TIME ZONE 'US/Eastern')::DATE + TIME '10:00:00'
        )
),

final AS (
    SELECT
        r.forecast_execution_datetime_utc
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
ORDER BY forecast_date DESC, hour_ending
