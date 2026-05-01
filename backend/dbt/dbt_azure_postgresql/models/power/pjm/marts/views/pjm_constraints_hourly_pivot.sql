{{
  config(
    materialized='view'
  )
}}

---------------------------
-- Screenshot-shape pivot of PJM binding-constraint shadow prices across DA / RT / DART.
-- One row per (date, market, monitored_facility, contingency_facility) with daily totals,
-- on-peak / off-peak splits, and a column per hour ending 1..24
-- (HE convention: HE1 = 00:00-01:00 EPT). Filter on `market` to match the
-- screenshot's RT/DA/DART radio toggle.
---------------------------

WITH HOURLY AS (
    SELECT
        date
        ,hour_ending
        ,period
        ,market
        ,monitored_facility
        ,contingency_facility
        ,congestion_event
        ,shadow_price
    FROM {{ ref('staging_v1_pjm_constraints_hourly') }}
)

SELECT
    date
    ,market
    ,monitored_facility                                                     AS constraint_name
    ,contingency_facility                                                   AS contingency
    ,MAX(congestion_event)                                                  AS reported_name
    ,SUM(shadow_price)                                                      AS total_price
    ,COUNT(*) FILTER (WHERE shadow_price IS NOT NULL)                       AS total_hours
    ,SUM(shadow_price) FILTER (WHERE period = 'OnPeak')                     AS onpeak_price
    ,COUNT(*) FILTER (WHERE shadow_price IS NOT NULL AND period = 'OnPeak') AS onpeak_hours
    ,SUM(shadow_price) FILTER (WHERE period = 'OffPeak')                    AS offpeak_price
    ,COUNT(*) FILTER (WHERE shadow_price IS NOT NULL AND period = 'OffPeak') AS offpeak_hours
    ,SUM(shadow_price) FILTER (WHERE hour_ending =  1)                      AS he01
    ,SUM(shadow_price) FILTER (WHERE hour_ending =  2)                      AS he02
    ,SUM(shadow_price) FILTER (WHERE hour_ending =  3)                      AS he03
    ,SUM(shadow_price) FILTER (WHERE hour_ending =  4)                      AS he04
    ,SUM(shadow_price) FILTER (WHERE hour_ending =  5)                      AS he05
    ,SUM(shadow_price) FILTER (WHERE hour_ending =  6)                      AS he06
    ,SUM(shadow_price) FILTER (WHERE hour_ending =  7)                      AS he07
    ,SUM(shadow_price) FILTER (WHERE hour_ending =  8)                      AS he08
    ,SUM(shadow_price) FILTER (WHERE hour_ending =  9)                      AS he09
    ,SUM(shadow_price) FILTER (WHERE hour_ending = 10)                      AS he10
    ,SUM(shadow_price) FILTER (WHERE hour_ending = 11)                      AS he11
    ,SUM(shadow_price) FILTER (WHERE hour_ending = 12)                      AS he12
    ,SUM(shadow_price) FILTER (WHERE hour_ending = 13)                      AS he13
    ,SUM(shadow_price) FILTER (WHERE hour_ending = 14)                      AS he14
    ,SUM(shadow_price) FILTER (WHERE hour_ending = 15)                      AS he15
    ,SUM(shadow_price) FILTER (WHERE hour_ending = 16)                      AS he16
    ,SUM(shadow_price) FILTER (WHERE hour_ending = 17)                      AS he17
    ,SUM(shadow_price) FILTER (WHERE hour_ending = 18)                      AS he18
    ,SUM(shadow_price) FILTER (WHERE hour_ending = 19)                      AS he19
    ,SUM(shadow_price) FILTER (WHERE hour_ending = 20)                      AS he20
    ,SUM(shadow_price) FILTER (WHERE hour_ending = 21)                      AS he21
    ,SUM(shadow_price) FILTER (WHERE hour_ending = 22)                      AS he22
    ,SUM(shadow_price) FILTER (WHERE hour_ending = 23)                      AS he23
    ,SUM(shadow_price) FILTER (WHERE hour_ending = 24)                      AS he24
FROM HOURLY
GROUP BY date, market, monitored_facility, contingency_facility
ORDER BY date DESC, market, total_price DESC NULLS LAST
