{{
  config(
    materialized='ephemeral'
  )
}}

---------------------------
-- HOURLY GAS DAY DATE SPINE
---------------------------
-- Session/strip model:
--
--   A trading session opens at HE10 on every trading day T and covers delivery
--   for every calendar day from T+1 through next_trading_day(T) inclusive.
--   HE1-9 of the next trading day is the continuation of the prior session
--   (residual closing hours).
--
--   Strip examples:
--     Mon (standard) -> [Tue]                       (1 day)
--     Fri (standard) -> [Sat, Sun, Mon]             (3 days)
--     Thu before Good Friday -> [Fri, Sat, Sun, Mon] (4 days)
--     Wed before Thanksgiving -> [Thu, Fri, Sat, Sun, Mon] (5 days)
--
--   The spine explodes each session across every delivery day in its strip,
--   so every calendar day in coverage receives exactly 24 rows keyed by
--   (gas_day, hour_ending). Duplicated session rows on strip days are a
--   feature — downstream groupby("gas_day") / groupby("date") Just Works.
--
--   Row trade_date attribution within a session:
--     HE10-24 -> session_trade_date (the day the session opened)
--     HE1-9   -> next_trading_day   (the day the session closes out)

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

-- For each trading day, find the next trading day (defines the session's close).
SESSIONS AS (
    SELECT
        trade_date AS session_trade_date,
        LEAD(trade_date) OVER (ORDER BY trade_date) AS next_trading_day
    FROM TRADING_DAYS
),

-- Explode each session across its delivery strip.
-- Strip = [session_trade_date + 1 ... next_trading_day] inclusive.
DELIVERY_STRIPS AS (
    SELECT
        s.session_trade_date,
        s.next_trading_day,
        gas_day::DATE AS gas_day
    FROM SESSIONS s
    CROSS JOIN LATERAL generate_series(
        (s.session_trade_date + INTERVAL '1 day')::DATE,
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
        (d.gas_day::TIMESTAMP + (h.hour_ending || ' hours')::INTERVAL) AS datetime,
        d.gas_day::DATE AS date,
        h.hour_ending::INTEGER AS hour_ending,
        d.gas_day::DATE AS gas_day,
        CASE
            WHEN h.hour_ending >= 10 THEN d.session_trade_date::DATE
            ELSE d.next_trading_day::DATE
        END AS trade_date
    FROM DELIVERY_STRIPS d
    CROSS JOIN HOURS h
)

SELECT * FROM HOURLY_SPINE
WHERE gas_day >= DATE '2020-01-01'
ORDER BY datetime DESC
