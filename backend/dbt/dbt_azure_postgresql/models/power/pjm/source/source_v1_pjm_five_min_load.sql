{{
  config(
    materialized='ephemeral'
  )
}}

---------------------------
-- RT 5-min Instantaneous Load (normalized)
-- Grain: 1 row per 5-min interval × zone
---------------------------

WITH MKT_REGION_LOOKUP AS (
    SELECT area, mkt_region, load_area
    FROM (VALUES
        ('PJM MID ATLANTIC REGION', 'MIDATL', 'MIDATL')
        ,('AE', 'MIDATL', 'AE')
        ,('BC', 'MIDATL', 'BC')
        ,('DPL', 'MIDATL', 'DPL')
        ,('JC', 'MIDATL', 'JC')
        ,('ME', 'MIDATL', 'ME')
        ,('PE', 'MIDATL', 'PE')
        ,('PEP', 'MIDATL', 'PEP')
        ,('PL', 'MIDATL', 'PL')
        ,('PN', 'MIDATL', 'PN')
        ,('PS', 'MIDATL', 'PS')
        ,('RECO', 'MIDATL', 'RECO')
        ,('UG', 'MIDATL', 'UG')

        ,('PJM WESTERN REGION', 'WEST', 'WEST')
        ,('AEP', 'WEST', 'AEP')
        ,('COMED', 'WEST', 'COMED')
        ,('DAYTON', 'WEST', 'DAYTON')
        ,('DEOK', 'WEST', 'DEOK')
        ,('DUQ', 'WEST', 'DUQ')
        ,('EKPC', 'WEST', 'EKPC')
        ,('ATSI', 'WEST', 'ATSI')
        ,('APS', 'WEST', 'APS')

        ,('PJM SOUTHERN REGION', 'SOUTH', 'SOUTH')
        ,('DOM', 'SOUTH', 'DOM')

        ,('PJM RTO', 'RTO', 'RTO')
    ) AS lookup_data(area, mkt_region, load_area)
),

FINAL AS (
    SELECT
        load.datetime_beginning_utc
        ,'US/Eastern' AS timezone
        ,load.datetime_beginning_ept AS datetime_beginning_local
        ,load.datetime_beginning_ept::DATE AS date
        ,(EXTRACT(HOUR FROM load.datetime_beginning_ept) + 1)::INT AS hour_ending

        ,load.area AS zone
        ,lookup.mkt_region
        ,lookup.load_area

        ,instantaneous_load AS load_mw

    FROM {{ source('pjm_v1', 'five_min_instantaneous_load_v1_2025_oct_15') }} load
    LEFT JOIN MKT_REGION_LOOKUP lookup ON load.area = lookup.area
    WHERE
        load.datetime_beginning_ept::DATE >= current_date - 14
)

SELECT * FROM FINAL
ORDER BY datetime_beginning_local DESC, mkt_region, load_area
