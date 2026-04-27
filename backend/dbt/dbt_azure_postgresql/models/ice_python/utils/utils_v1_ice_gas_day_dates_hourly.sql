{{
  config(
    materialized='ephemeral'
  )
}}

---------------------------
-- HOURLY GAS DAY DATE SPINE
---------------------------
-- Mirrors ICE's U.S. Next Day Gas Trading Calendar:
--   https://www.ice.com/publicdocs/support/phys_gas_calendar.pdf
--
-- Rules derived from that calendar:
--
-- 1. Each trading day T runs a single, self-contained session. All 24
--    hour-ending snapshots on T belong to T's session — there are NO
--    "continuation hours" leaking into the next trading day.
--
-- 2. A session's delivery coverage ("strip") runs from the day after T to
--    (and including) the next trading day:
--       strip = [T+1 ... next_trading_day(T)]
--    Examples:
--       Mon standard           -> [Tue]                         (1 day)
--       Fri standard           -> [Sat, Sun, Mon]               (3 days)
--       Thu before Good Friday -> [Fri, Sat, Sun, Mon]          (4 days)
--       Wed before Thanksgiving-> [Thu, Fri, Sat, Sun, Mon]     (5 days)
--
-- 3. Month-end split session. When a month ends on Sat or Sun, that
--    month's trailing weekend days are assigned to the LAST TRADING
--    THURSDAY of the month (so they settle inside the month's book,
--    synchronized with the start of bidweek). The last trading Friday
--    then starts its strip at the first of the next month.
--       Jan 2026 (ends Sat 31):
--         Thu Jan 29 -> [Fri 30, Sat 31]            (2 days, not 1)
--         Fri Jan 30 -> [Sun Feb 1, Mon Feb 2]      (2 days, not 3)
--       May 2026 (ends Sun 31):
--         Thu May 28 -> [Fri 29, Sat 30, Sun 31]    (3 days, not 1)
--         Fri May 29 -> [Mon Jun 1]                 (1 day,  not 3)
--
-- The spine explodes each session across its strip. Every calendar day in
-- coverage receives exactly 24 rows (gas_day, hour_ending), all attributed
-- to the single trade_date that priced that gas.

WITH NON_TRADING_DAYS AS (
    SELECT "date"::DATE AS non_trading_date
    FROM {{ ref('ice_us_physical_gas_non_trading_days') }}
),

-- DATE_SPINE extends back before 2020-01-01 so the first in-coverage gas_days
-- can be attributed to late-2019 trading sessions (final filter clips output
-- to gas_day >= 2020-01-01 below).
DATE_SPINE AS (
    SELECT generate_series(
        DATE '2019-12-01',
        (CURRENT_DATE + INTERVAL '2 years')::DATE,
        INTERVAL '1 day'
    )::DATE AS date
),

TRADING_DAYS AS (
    SELECT date AS trade_date
    FROM DATE_SPINE
    WHERE EXTRACT(DOW FROM date) BETWEEN 1 AND 5
      AND date NOT IN (SELECT non_trading_date FROM NON_TRADING_DAYS)
),

SESSIONS AS (
    SELECT
        trade_date AS session_trade_date,
        LEAD(trade_date) OVER (ORDER BY trade_date) AS next_trading_day
    FROM TRADING_DAYS
),

-- Month-end metadata: last calendar day of each trade_date's month.
MONTH_END_INFO AS (
    SELECT
        s.session_trade_date,
        s.next_trading_day,
        (date_trunc('month', s.session_trade_date) + INTERVAL '1 month - 1 day')::DATE AS month_end
    FROM SESSIONS s
),

-- Apply month-end split rule only for the canonical Thu-Fri-weekend pattern
-- where month_end falls on Sat/Sun of that specific weekend. This avoids
-- false positives in Thanksgiving-disrupted months where the last trading
-- Thu/Fri is not adjacent to the month-end weekend.
--
-- Thu T rule: requires next_trading_day = T+1 (Fri is a normal trading day,
-- so Thu's natural strip would only cover Fri). Then extend Thu's strip
-- through month_end if month_end is T+2 (Sat) or T+3 (Sun).
--
-- Fri T rule: applies whenever month_end is T+1 (Sat) or T+2 (Sun). Fri's
-- strip starts at month_end+1 (first day of next month). The Fri rule
-- deliberately does NOT depend on next_trading_day — when the post-weekend
-- Mon is a holiday (Labor Day / observed New Year), next_trading_day is
-- T+4 rather than T+3, but the month-end weekend still needs to land in
-- Thu's strip, not Fri's.
SESSION_STRIPS AS (
    SELECT
        session_trade_date,
        next_trading_day,
        CASE
            WHEN EXTRACT(DOW FROM session_trade_date) = 5
                 AND month_end IN (
                     (session_trade_date + INTERVAL '1 day')::DATE,
                     (session_trade_date + INTERVAL '2 days')::DATE
                 )
            THEN (month_end + INTERVAL '1 day')::DATE
            ELSE (session_trade_date + INTERVAL '1 day')::DATE
        END AS strip_start,
        CASE
            WHEN EXTRACT(DOW FROM session_trade_date) = 4
                 AND next_trading_day = (session_trade_date + INTERVAL '1 day')::DATE
                 AND month_end IN (
                     (session_trade_date + INTERVAL '2 days')::DATE,
                     (session_trade_date + INTERVAL '3 days')::DATE
                 )
            THEN month_end
            ELSE next_trading_day
        END AS strip_end
    FROM MONTH_END_INFO
),

DELIVERY_STRIPS AS (
    SELECT
        s.session_trade_date,
        gas_day::DATE AS gas_day
    FROM SESSION_STRIPS s
    CROSS JOIN LATERAL generate_series(
        s.strip_start,
        s.strip_end,
        INTERVAL '1 day'
    ) AS gas_day
    WHERE s.next_trading_day IS NOT NULL
),

HOURS AS (
    SELECT generate_series(1, 24) AS hour_ending
),

HOURLY_SPINE AS (
    SELECT
        (d.gas_day::TIMESTAMP + (h.hour_ending || ' hours')::INTERVAL) AS datetime,
        d.gas_day::DATE AS date,
        h.hour_ending::INTEGER AS hour_ending,
        d.gas_day::DATE AS gas_day,
        d.session_trade_date::DATE AS trade_date
    FROM DELIVERY_STRIPS d
    CROSS JOIN HOURS h
)

SELECT * FROM HOURLY_SPINE
WHERE gas_day >= DATE '2020-01-01'
ORDER BY datetime DESC
