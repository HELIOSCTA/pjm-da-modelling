{{
  config(
    materialized='ephemeral'
  )
}}

---------------------------
-- PJM Transmission Outages (normalized)
-- Grain: 1 row per ticket_id (snapshot-overwrite via daily upsert)
-- Source: eDART linesout.txt parsed by backend.scrapes.power.pjm.transmission_outages
---------------------------

WITH RAW AS (
    SELECT
        ticket_id::BIGINT                    AS ticket_id
        ,item_number::INT                    AS item_number
        ,zone
        ,facility_name
        ,equipment_type
        ,station
        ,voltage_kv::NUMERIC                 AS voltage_kv
        ,start_datetime::TIMESTAMP           AS start_datetime
        ,end_datetime::TIMESTAMP             AS end_datetime
        ,status                                                       -- 'O' open, 'C' closed
        ,outage_state                                                 -- Active, Approved, Received, Complete, Cancelle, Denied, Revised
        ,last_revised::TIMESTAMP             AS last_revised
        ,NULLIF(rtep, '')                    AS rtep
        ,NULLIF(availability, '')            AS availability
        ,NULLIF(risk, '')                    AS risk
        ,NULLIF(approval_status, '')         AS approval_status
        ,NULLIF(on_time, '')                 AS on_time
        ,equipment_count::INT                AS equipment_count
        ,section
        ,cause
        ,scrape_date::DATE                   AS scrape_date
        ,scrape_timestamp::TIMESTAMP         AS scrape_timestamp
        ,created_at::TIMESTAMPTZ             AS created_at
        ,updated_at::TIMESTAMPTZ             AS updated_at

    FROM {{ source('pjm_v1', 'transmission_outages') }}
)

SELECT * FROM RAW
