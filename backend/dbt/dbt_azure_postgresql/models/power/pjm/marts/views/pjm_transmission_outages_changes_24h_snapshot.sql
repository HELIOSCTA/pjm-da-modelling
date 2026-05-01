{{
  config(
    materialized='view'
  )
}}

---------------------------
-- Transmission Outages — last 24h delta (snapshot variant)
-- Sources the SCD2 history maintained by snapshots/pjm_transmission_outages_snapshot.
-- Classifies each ticket touched in the window as NEW / REVISED / CLEARED with
-- prev_* diff columns for revisions.
-- Grain: 1 row per (ticket_id, change_type)
--
-- Note: Returns empty rows on day 1 — the snapshot has to run for >=24h before
-- there is any history to diff against.
--
-- Inlined here (not in a staging model) because the staging-name length plus
-- dbt's `__dbt__cte__` prefix would exceed Postgres's 63-char identifier limit.
---------------------------

WITH SNAP AS (
    SELECT *
    FROM {{ ref('pjm_transmission_outages_snapshot') }}
),

CURRENT_ROWS AS (
    SELECT *
    FROM SNAP
    WHERE dbt_valid_to IS NULL
),

LAST_CLOSED AS (
    -- Most recent superseded version per ticket (immediate prior, or the close
    -- row inserted when invalidate_hard_deletes fires).
    SELECT DISTINCT ON (ticket_id) *
    FROM SNAP
    WHERE dbt_valid_to IS NOT NULL
    ORDER BY ticket_id, dbt_valid_to DESC
),

NEW_TICKETS AS (
    SELECT
        c.ticket_id
        ,'NEW'::TEXT                         AS change_type
        ,c.dbt_valid_from                    AS captured_at
        ,c.zone
        ,c.facility_name
        ,c.equipment_type
        ,c.station
        ,c.voltage_kv
        ,c.start_datetime
        ,c.end_datetime
        ,c.status
        ,c.outage_state
        ,c.risk
        ,c.cause
        ,c.last_revised
        ,c.equipment_count
        ,NULL::TEXT                          AS prev_outage_state
        ,NULL::TIMESTAMP                     AS prev_start_datetime
        ,NULL::TIMESTAMP                     AS prev_end_datetime
        ,NULL::TEXT                          AS prev_risk

    FROM CURRENT_ROWS c
    LEFT JOIN LAST_CLOSED p USING (ticket_id)
    WHERE
        c.dbt_valid_from >= NOW() - INTERVAL '24 hours'
        AND p.ticket_id IS NULL
),

REVISED_TICKETS AS (
    SELECT
        c.ticket_id
        ,'REVISED'::TEXT                     AS change_type
        ,c.dbt_valid_from                    AS captured_at
        ,c.zone
        ,c.facility_name
        ,c.equipment_type
        ,c.station
        ,c.voltage_kv
        ,c.start_datetime
        ,c.end_datetime
        ,c.status
        ,c.outage_state
        ,c.risk
        ,c.cause
        ,c.last_revised
        ,c.equipment_count
        ,p.outage_state                      AS prev_outage_state
        ,p.start_datetime                    AS prev_start_datetime
        ,p.end_datetime                      AS prev_end_datetime
        ,p.risk                              AS prev_risk

    FROM CURRENT_ROWS c
    INNER JOIN LAST_CLOSED p USING (ticket_id)
    WHERE
        c.dbt_valid_from >= NOW() - INTERVAL '24 hours'
),

CLEARED_TICKETS AS (
    SELECT
        p.ticket_id
        ,'CLEARED'::TEXT                     AS change_type
        ,p.dbt_valid_to                      AS captured_at
        ,p.zone
        ,p.facility_name
        ,p.equipment_type
        ,p.station
        ,p.voltage_kv
        ,p.start_datetime
        ,p.end_datetime
        ,p.status
        ,p.outage_state
        ,p.risk
        ,p.cause
        ,p.last_revised
        ,p.equipment_count
        ,NULL::TEXT                          AS prev_outage_state
        ,NULL::TIMESTAMP                     AS prev_start_datetime
        ,NULL::TIMESTAMP                     AS prev_end_datetime
        ,NULL::TEXT                          AS prev_risk

    FROM LAST_CLOSED p
    LEFT JOIN CURRENT_ROWS c USING (ticket_id)
    WHERE
        c.ticket_id IS NULL
        AND p.dbt_valid_to >= NOW() - INTERVAL '24 hours'
),

ALL_CHANGES AS (
    SELECT * FROM NEW_TICKETS
    UNION ALL
    SELECT * FROM REVISED_TICKETS
    UNION ALL
    SELECT * FROM CLEARED_TICKETS
)

SELECT * FROM ALL_CHANGES
ORDER BY
    CASE change_type
        WHEN 'NEW' THEN 1
        WHEN 'REVISED' THEN 2
        WHEN 'CLEARED' THEN 3
    END
    ,voltage_kv DESC
    ,captured_at DESC
