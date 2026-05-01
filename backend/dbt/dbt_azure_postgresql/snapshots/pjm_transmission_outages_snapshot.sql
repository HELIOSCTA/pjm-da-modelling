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
            'last_revised',
            'approval_status',
            'on_time',
            'equipment_count'
        ],
        invalidate_hard_deletes=true
    )
}}

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
