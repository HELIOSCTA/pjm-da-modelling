{% snapshot pjm_transmission_outages_snapshot %}

{{
    config(
        target_schema='pjm_da_modelling_cleaned',
        unique_key='ticket_id',
        strategy='check',
        check_cols=[
            'outage_state',
            'status',
            'start_datetime',
            'end_datetime',
            'risk',
            'cause',
            'approval_status',
            'on_time',
            'equipment_count'
        ],
        invalidate_hard_deletes=true
    )
}}

{# `last_revised` was deliberately removed from check_cols on 2026-05-04.
   It's a PJM eDART metadata timestamp that bumps on every source-side
   revision regardless of whether anything substantive changed. Diagnosed
   from the changes_24h_snapshot mart: 15/19 of "no tracked field changed"
   revisions were pure last_revised churn. The column is still SELECT-ed
   below so downstream marts can surface it for display. #}

SELECT
    ticket_id
    ,item_number
    ,zone
    ,facility_name
    ,equipment_type
    ,station
    ,voltage_kv
    ,start_datetime
    ,end_datetime
    ,status
    ,outage_state
    ,last_revised
    ,rtep
    ,availability
    ,risk
    ,approval_status
    ,on_time
    ,equipment_count
    ,section
    ,cause

FROM {{ ref('source_v1_pjm_transmission_outages') }}

{% endsnapshot %}
