{{
  config(
    materialized='view'
  )
}}

---------------------------
-- ICE GAS-DAY PRICES FOR PJM DA HOURLY MODELS
-- Grain: 1 row per PJM forecast_date x PJM hour_ending.
--
-- PJM DA model features are keyed to the electric operating day in
-- US/Eastern. Physical gas days are 9:00-9:00 America/Chicago, so an electric
-- calendar day can overlap two gas_days. Each PJM hour's UTC instant maps to
-- exactly one gas_day: convert the instant to America/Chicago wall-clock and
-- apply the 09:00 CT rollover. Equi-joining on gas_day avoids the expensive
-- range join used previously.
---------------------------

WITH PJM_HOURS AS (
    SELECT
        datetime_beginning_utc,
        timezone AS pjm_timezone,
        datetime_beginning_local AS forecast_datetime,
        datetime_beginning_local AS pjm_datetime_beginning_local,
        datetime_ending_local AS pjm_datetime_ending_local,
        date AS forecast_date,
        hour_ending,
        (datetime_beginning_utc AT TIME ZONE 'UTC' AT TIME ZONE 'America/Chicago') AS pjm_central_local
    FROM {{ ref('pjm_dates_hourly') }}
    WHERE date >= DATE '2020-01-01'
),

PJM_HOURS_WITH_GAS_DAY AS (
    SELECT
        datetime_beginning_utc,
        pjm_timezone,
        forecast_datetime,
        pjm_datetime_beginning_local,
        pjm_datetime_ending_local,
        forecast_date,
        hour_ending,
        CASE
            WHEN pjm_central_local::TIME >= TIME '09:00:00'
                THEN pjm_central_local::DATE
            ELSE (pjm_central_local::DATE - INTERVAL '1 day')::DATE
        END AS gas_day
    FROM PJM_HOURS
),

GAS_DAILY AS (
    SELECT
        timezone AS gas_timezone,
        datetime_beginning_local AS gas_datetime_beginning_local,
        datetime_ending_local AS gas_datetime_ending_local,
        gas_day,
        trade_date,
        tetco_m3_cash,
        columbia_tco_cash,
        transco_z6_ny_cash,
        dominion_south_cash,
        nng_ventura_cash,
        tetco_m2_cash,
        transco_z5_north_cash,
        tenn_z4_marcellus_cash,
        transco_leidy_cash,
        chicago_cg_cash
    FROM {{ ref('staging_v1_ice_gas_day_daily') }}
),

FINAL AS (
    SELECT
        p.forecast_datetime as datetime,
        p.forecast_date as date,
        p.hour_ending,
        p.pjm_timezone,
        p.pjm_datetime_beginning_local,
        p.pjm_datetime_ending_local,
        g.gas_timezone,
        g.gas_datetime_beginning_local,
        g.gas_datetime_ending_local,
        g.gas_day,
        g.trade_date,
        g.tetco_m3_cash,
        g.columbia_tco_cash,
        g.transco_z6_ny_cash,
        g.dominion_south_cash,
        g.nng_ventura_cash,
        g.tetco_m2_cash,
        g.transco_z5_north_cash,
        g.tenn_z4_marcellus_cash,
        g.transco_leidy_cash,
        g.chicago_cg_cash
    FROM PJM_HOURS_WITH_GAS_DAY p
    INNER JOIN GAS_DAILY g
        ON p.gas_day = g.gas_day
)

SELECT * FROM FINAL
ORDER BY datetime DESC
