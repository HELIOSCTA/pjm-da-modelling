{{
  config(
    materialized='ephemeral'
  )
}}

---------------------------
-- GAS-DAY SPINE
---------------------------
-- One row per (gas_day, hour_ending). Each gas_day's trade_date points at
-- the actual ICE session that priced it.
--
-- Per NAESB WGQ 1.3.1, a gas day is a single 24-hour delivery period.
-- Per the ICE 2026 calendar, every gas_day is priced by exactly one session:
--   * Standard weekday delivery -> prior business day's session
--   * Weekend strip (Sat/Sun/Mon) -> the prior Friday's session
--   * Holiday extensions -> the most recent trading day before the holiday
-- Per NAESB, physical gas days run 9:00 a.m. to 9:00 a.m. Central Clock Time.
-- Timestamp fields below use the DST-aware America/Chicago timezone.
--
-- Implementation: each trading day T owns the strip of gas_days that fall
-- between T and the NEXT trading day. We expand each session's strip via
-- LATERAL generate_series, then cross-join 24 HEs. Every gas_day in the
-- output has exactly one trade_date attribution.

WITH NON_TRADING_DAYS AS (
    SELECT "date"::DATE AS non_trading_date
    FROM {{ ref('ice_us_physical_gas_non_trading_days') }}
),

DATE_SPINE AS (
    SELECT generate_series(
        DATE '2019-12-01',
        (CURRENT_DATE + INTERVAL '2 years')::DATE,
        INTERVAL '1 day'
    )::DATE AS calendar_date
),

TRADING_DAYS AS (
    SELECT calendar_date AS trade_date
    FROM DATE_SPINE
    WHERE EXTRACT(DOW FROM calendar_date) BETWEEN 1 AND 5
      AND calendar_date NOT IN (SELECT non_trading_date FROM NON_TRADING_DAYS)
),

SESSIONS AS (
    SELECT
        trade_date,
        LEAD(trade_date) OVER (ORDER BY trade_date) AS next_trading_day
    FROM TRADING_DAYS
),

-- Each session owns the strip [trade_date + 1 ... next_trading_day] inclusive.
GAS_DAY_TRADE_DATE AS (
    SELECT
        s.trade_date,
        gas_day::DATE AS gas_day
    FROM SESSIONS s
    CROSS JOIN LATERAL generate_series(
        (s.trade_date + INTERVAL '1 day')::DATE,
        s.next_trading_day::DATE,
        INTERVAL '1 day'
    ) AS gas_day
    WHERE s.next_trading_day IS NOT NULL
),

HOURS AS (
    SELECT generate_series(1, 24) AS hour_ending
),

HOURLY_SPINE AS (
    SELECT
        (
            (
                g.gas_day + TIME '09:00:00'
                + ((h.hour_ending - 1) * INTERVAL '1 hour')
            ) AT TIME ZONE 'America/Chicago' AT TIME ZONE 'UTC'
        ) AS datetime_beginning_utc,
        (
            (
                g.gas_day + TIME '09:00:00'
                + (h.hour_ending * INTERVAL '1 hour')
            ) AT TIME ZONE 'America/Chicago' AT TIME ZONE 'UTC'
        ) AS datetime_ending_utc,
        'America/Chicago' AS timezone,
        (
            g.gas_day + TIME '09:00:00'
            + ((h.hour_ending - 1) * INTERVAL '1 hour')
        ) AS datetime_beginning_local,
        (
            g.gas_day + TIME '09:00:00'
            + (h.hour_ending * INTERVAL '1 hour')
        ) AS datetime_ending_local,
        g.gas_day,
        h.hour_ending::INTEGER AS hour_ending,
        g.trade_date
    FROM GAS_DAY_TRADE_DATE g
    CROSS JOIN HOURS h
)

SELECT * FROM HOURLY_SPINE
WHERE gas_day >= DATE '2020-01-01'
