{{
  config(
    materialized='ephemeral'
  )
}}

---------------------------
-- PJM Binding Constraints — hourly long form across DA / RT / DART
-- Grain: 1 row per (date, hour_ending, market, monitored_facility, contingency_facility)
--   - market is one of 'DA', 'RT', 'DART'
--   - DART = DA shadow_price - RT shadow_price
--   - DART is emitted ONLY when the same (hour, monitored, contingency) bound
--     in BOTH DA and RT — the trader's "DART spread" definition. Constraints
--     that bound in only one market produce just that market's row.
-- congestion_event label sourced from da_transconstraints (only available
--   on DA-side rows; LEFT JOIN means RT-only constraints have congestion_event = NULL).
---------------------------

WITH DA_TC AS (
    SELECT
        datetime_beginning_utc
        ,monitored_facility
        ,contingency_facility
        ,congestion_event
        -- de-duplicate same-hour duplicates: prefer longest-duration label
        ,ROW_NUMBER() OVER (
            PARTITION BY datetime_beginning_utc, monitored_facility, contingency_facility
            ORDER BY duration_hours DESC, congestion_event
        ) AS rn
    FROM {{ ref('source_v1_pjm_da_transmission_constraints') }}
),

DA AS (
    SELECT
        mv.datetime_beginning_utc
        ,mv.monitored_facility
        ,mv.contingency_facility
        ,tc.congestion_event
        ,mv.shadow_price                                    AS da_shadow_price
    FROM {{ ref('source_v1_pjm_da_marginal_value') }} mv
    LEFT JOIN DA_TC tc
        ON tc.datetime_beginning_utc = mv.datetime_beginning_utc
       AND tc.monitored_facility    = mv.monitored_facility
       AND tc.contingency_facility  = mv.contingency_facility
       AND tc.rn = 1
),

RT AS (
    -- PJM publishes RT marginal_value at 5-minute grain. Aggregate to
    -- hourly here so the join key aligns with DA (which is top-of-hour
    -- only) and with pjm_dates_hourly. AVG is the conventional rollup
    -- for shadow prices spanning an hour. Without this, subhour RT rows
    -- fall through the LEFT JOIN to pjm_dates_hourly with NULL date and
    -- inflate DART hour counts by ~12x.
    SELECT
        DATE_TRUNC('hour', datetime_beginning_utc)::TIMESTAMP AS datetime_beginning_utc
        ,monitored_facility
        ,contingency_facility
        ,AVG(shadow_price)                                  AS rt_shadow_price
    FROM {{ ref('source_v1_pjm_rt_marginal_value') }}
    GROUP BY 1, 2, 3
),

JOINED AS (
    SELECT
        COALESCE(da.datetime_beginning_utc, rt.datetime_beginning_utc) AS datetime_beginning_utc
        ,COALESCE(da.monitored_facility,    rt.monitored_facility)    AS monitored_facility
        ,COALESCE(da.contingency_facility,  rt.contingency_facility)  AS contingency_facility
        ,da.congestion_event
        ,da.da_shadow_price
        ,rt.rt_shadow_price
        -- DART defined only where both sides bound; NULL otherwise so the
        -- LONG CTE's `WHERE dart_shadow_price IS NOT NULL` filter excludes
        -- single-market hours from the DART rowset.
        ,CASE
            WHEN da.da_shadow_price IS NOT NULL AND rt.rt_shadow_price IS NOT NULL
            THEN da.da_shadow_price - rt.rt_shadow_price
        END AS dart_shadow_price
    FROM DA da
    FULL OUTER JOIN RT rt
        ON rt.datetime_beginning_utc = da.datetime_beginning_utc
       AND rt.monitored_facility    = da.monitored_facility
       AND rt.contingency_facility  = da.contingency_facility
),

WITH_TIME AS (
    SELECT
        j.datetime_beginning_utc
        ,d.date
        ,d.hour_ending
        ,d.period
        ,j.monitored_facility
        ,j.contingency_facility
        ,j.congestion_event
        ,j.da_shadow_price
        ,j.rt_shadow_price
        ,j.dart_shadow_price
    FROM JOINED j
    LEFT JOIN {{ ref('pjm_dates_hourly') }} d
        ON d.datetime_beginning_utc = j.datetime_beginning_utc
),

LONG AS (
    SELECT
        datetime_beginning_utc, date, hour_ending, period,
        monitored_facility, contingency_facility, congestion_event,
        'DA'::VARCHAR        AS market,
        da_shadow_price      AS shadow_price
    FROM WITH_TIME WHERE da_shadow_price IS NOT NULL

    UNION ALL

    SELECT
        datetime_beginning_utc, date, hour_ending, period,
        monitored_facility, contingency_facility, congestion_event,
        'RT'::VARCHAR        AS market,
        rt_shadow_price      AS shadow_price
    FROM WITH_TIME WHERE rt_shadow_price IS NOT NULL

    UNION ALL

    SELECT
        datetime_beginning_utc, date, hour_ending, period,
        monitored_facility, contingency_facility, congestion_event,
        'DART'::VARCHAR      AS market,
        dart_shadow_price    AS shadow_price
    FROM WITH_TIME WHERE dart_shadow_price IS NOT NULL
)

SELECT * FROM LONG
ORDER BY date DESC, market, hour_ending DESC, monitored_facility, contingency_facility
