{{
  config(
    materialized='ephemeral'
  )
}}

---------------------------
-- PJM Load Region Mapping
-- Maps every region value across all PJM load models to a canonical parent region
-- Join any model's region column to get parent_region and region_type
---------------------------

SELECT region, parent_region, region_type
FROM (VALUES

    -- Aggregate regions (short names — used by RT/DA loads, gridstatus forecast)
     ('RTO',                    'RTO',    'aggregate')
    ,('MIDATL',                 'MIDATL', 'aggregate')
    ,('WEST',                   'WEST',   'aggregate')
    ,('SOUTH',                  'SOUTH',  'aggregate')

    -- Aggregate regions (PJM API names — used by pjm forecast)
    ,('RTO_COMBINED',           'RTO',    'aggregate')
    ,('MID_ATLANTIC_REGION',    'MIDATL', 'aggregate')
    ,('WESTERN_REGION',         'WEST',   'aggregate')
    ,('SOUTHERN_REGION',        'SOUTH',  'aggregate')

    -- Mid-Atlantic zones
    ,('AE/MIDATL',              'MIDATL', 'zone')
    ,('BG&E/MIDATL',            'MIDATL', 'zone')
    ,('DP&L/MIDATL',            'MIDATL', 'zone')
    ,('JCP&L/MIDATL',           'MIDATL', 'zone')
    ,('METED/MIDATL',           'MIDATL', 'zone')
    ,('PECO/MIDATL',            'MIDATL', 'zone')
    ,('PENELEC/MIDATL',         'MIDATL', 'zone')
    ,('PEPCO/MIDATL',           'MIDATL', 'zone')
    ,('PPL/MIDATL',             'MIDATL', 'zone')
    ,('PSE&G/MIDATL',           'MIDATL', 'zone')
    ,('RECO/MIDATL',            'MIDATL', 'zone')
    ,('UGI/MIDATL',             'MIDATL', 'zone')

    -- Western zones
    ,('AEP',                    'WEST',   'zone')
    ,('AP',                     'WEST',   'zone')
    ,('ATSI',                   'WEST',   'zone')
    ,('COMED',                  'WEST',   'zone')
    ,('DAYTON',                 'WEST',   'zone')
    ,('DEOK',                   'WEST',   'zone')
    ,('DUQUESNE',               'WEST',   'zone')

    -- Southern zones
    ,('DOMINION',               'SOUTH',  'zone')
    ,('EKPC',                   'SOUTH',  'zone')

) AS t(region, parent_region, region_type)

