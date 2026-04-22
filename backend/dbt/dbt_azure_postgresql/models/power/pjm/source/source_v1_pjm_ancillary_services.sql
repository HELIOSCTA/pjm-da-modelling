{{
  config(
    materialized='ephemeral'
  )
}}

---------------------------
-- Ancillary Services Prices (normalized)
-- Grain: 1 row per date x hour x ancillary_service
---------------------------

WITH RAW AS (
    SELECT
        datetime_beginning_utc
        ,datetime_beginning_utc + INTERVAL '1 hour' AS datetime_ending_utc
        ,'US/Eastern' AS timezone
        ,datetime_beginning_ept AS datetime_beginning_local
        ,datetime_beginning_ept + INTERVAL '1 hour' AS datetime_ending_local
        ,datetime_beginning_ept::DATE AS date
        ,(EXTRACT(HOUR FROM datetime_beginning_ept) + 1)::INT AS hour_ending
        ,ancillary_service
        ,unit
        ,value::NUMERIC AS value
        ,row_is_current::BOOLEAN AS row_is_current
        ,version_nbr
    FROM {{ source('pjm_v1', 'ancillary_services') }}
),

--------------------------------
-- Dedup: prefer current row, latest version
--------------------------------

RANKED AS (
    SELECT
        *
        ,ROW_NUMBER() OVER (
            PARTITION BY date, hour_ending, ancillary_service
            ORDER BY
                CASE WHEN row_is_current = TRUE THEN 0 ELSE 1 END
                ,version_nbr DESC
        ) AS rn
    FROM RAW
),

DEDUPED AS (
    SELECT
        datetime_beginning_utc
        ,datetime_ending_utc
        ,timezone
        ,datetime_beginning_local
        ,datetime_ending_local
        ,date
        ,hour_ending
        ,ancillary_service
        ,unit
        ,value
    FROM RANKED
    WHERE rn = 1
)

SELECT * FROM DEDUPED
ORDER BY datetime_ending_local DESC, ancillary_service
